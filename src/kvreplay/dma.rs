//! `[workload.kv_replay]` over `protocol = "dma"`.
//!
//! The lookup is the RESP path's EXISTS pipeline ([`super::lookup`]), sent on
//! the DMA connection's RESP control channel: the module's keys answer EXISTS
//! like any other key. Each GET of the hit prefix is a [`DmaClient::get`] into
//! the connection's registered buffer, and each SET a
//! [`DmaClient::set_registered`] from a window of the registered value pool
//! the size of the object.
//!
//! `DmaClient` has one registered buffer per connection and waits for each
//! transfer's reply, so a connection runs one GET or SET at a time
//! (`protocol = "dma"` requires `connection.pipeline_depth = 1`), and
//! `connection.connections` bounds the trace requests in flight as on the RESP
//! path.
//!
//! Outcomes and per-request metrics are those of the RESP path. Per transfer,
//! `bytes_rx` and `bytes_tx` count the payload moved over the fabric, as the
//! `protocol = "dma"` workload does, not RESP framing.

use super::{
    Failure, KvConn, KvTransport, Outcome, Scratch, drive_on, finish, lookup, prepare, select_sets,
};
use crate::client::{RequestResult, RequestType};
use crate::dma::{DmaClient, DmaClientError, source_window};
use crate::metrics;
use crate::worker::{DisconnectReason, InFlight, record_counters, recv_with_timeout, value_offset};
use kvtrace::Request;
use rand_xoshiro::Xoshiro256PlusPlus;
use ringline::ConnCtx;
use std::time::Instant;

/// A DMA connection running trace requests.
struct DmaKv<'a> {
    client: &'a mut DmaClient,
    /// Whether a SET carries a CRC-32c for the server to verify
    /// ([`crate::config::Dma::checksums_writes`]).
    write_checksums: bool,
}

/// Replay trace requests on one connected DMA client until the run stops or
/// the connection fails. The DMA counterpart of [`super::drive`].
pub(crate) async fn drive_dma(
    client: &mut DmaClient,
    write_checksums: bool,
    conn: ConnCtx,
    ctx: &KvConn<'_>,
    rng: &mut Xoshiro256PlusPlus,
) -> Result<(), DisconnectReason> {
    let mut transport = DmaKv {
        client,
        write_checksums,
    };
    drive_on(&mut transport, conn, ctx, rng).await
}

impl KvTransport for DmaKv<'_> {
    fn is_alive(&self) -> bool {
        self.client.is_alive()
    }

    async fn run_request(
        &mut self,
        conn: ConnCtx,
        ctx: &KvConn<'_>,
        rng: &mut Xoshiro256PlusPlus,
        inflight: &mut InFlight,
        s: &mut Scratch,
        req: &Request,
    ) -> Result<Outcome, Failure> {
        let kv = ctx.kv;
        let input = u64::from(req.input_tokens);
        let mut o = Outcome {
            new_tokens: input,
            ..Default::default()
        };
        if req.chunks.is_empty() {
            return Ok(o);
        }
        let ranks = prepare(kv, s, req);
        // 1. Lookup, on the control channel. Nothing else is in flight on it.
        lookup(self.client.control(), conn, inflight, s, ranks, &mut o).await?;

        // 2. Load: one transfer per object of the hit prefix, in order.
        let gets = o.lookup_hit * ranks;
        if gets > 0 {
            let t0 = Instant::now();
            for i in 0..gets {
                let started = Instant::now();
                metrics::REQUESTS_SENT.increment();
                inflight.push(started);
                let reply = recv_with_timeout(self.client.get(s.key(i)), inflight, conn)
                    .await
                    .map_err(Failure::Disconnect)?;
                inflight.pop();
                let latency_ns = started.elapsed().as_nanos() as u64;
                let get = match reply {
                    Ok(Some(receipt)) => {
                        note_transfer(
                            RequestType::Get,
                            latency_ns,
                            receipt.bytes_written,
                            Some(true),
                            None,
                        );
                        GetResult::Hit
                    }
                    Ok(None) => {
                        note_transfer(RequestType::Get, latency_ns, 0, Some(false), None);
                        GetResult::Nil
                    }
                    Err(error) => {
                        failed(&error, conn)?;
                        note_transfer(RequestType::Get, latency_ns, 0, None, Some(&error));
                        GetResult::ErrorReply
                    }
                };
                apply_get(&mut o, s.objects[i].chunk_index as usize, get);
            }
            o.gets = gets;
            o.load_ns = t0.elapsed().as_nanos() as u64;
        }

        // 3. Store: one transfer per selected object, from the registered pool.
        select_sets(kv, s, &o, ranks);
        let sets = s.sets.len();
        if sets > 0 {
            let pool_len = ctx.value_pool.len();
            let t0 = Instant::now();
            for &i in &s.sets {
                let len = s.objects[i].size as usize;
                // No staging copy: the server reads the value from the pool
                // window it lives in, as the `protocol = "dma"` workload's SETs do.
                let at = value_offset(rng, len, pool_len);
                let Some(window) = source_window(at, len) else {
                    // `open_fabric` spawns no connection task when the pool
                    // does not register, so this connection cannot exist.
                    tracing::error!("kv replay: the value pool is not registered on this worker");
                    conn.close();
                    return Err(Failure::Disconnect(DisconnectReason::SendError));
                };
                let checksum = self
                    .write_checksums
                    .then(|| crate::dma::checksum(&ctx.value_pool[at..at + len]));
                let started = Instant::now();
                metrics::REQUESTS_SENT.increment();
                inflight.push(started);
                let reply = recv_with_timeout(
                    self.client.set_registered(s.key(i), &window, checksum),
                    inflight,
                    conn,
                )
                .await
                .map_err(Failure::Disconnect)?;
                inflight.pop();
                let latency_ns = started.elapsed().as_nanos() as u64;
                match reply {
                    Ok(receipt) => note_transfer(
                        RequestType::Set,
                        latency_ns,
                        receipt.bytes_written,
                        None,
                        None,
                    ),
                    Err(error) => {
                        failed(&error, conn)?;
                        note_transfer(RequestType::Set, latency_ns, 0, None, Some(&error));
                    }
                }
            }
            o.sets = sets;
            o.store_ns = t0.elapsed().as_nanos() as u64;
        }

        finish(kv, input, &mut o);
        Ok(o)
    }
}

/// What a GET of the load returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GetResult {
    Hit,
    /// No receipt although EXISTS said 1: evicted between the two.
    Nil,
    /// The server answered with an error.
    ErrorReply,
}

/// Apply one GET of chunk `chunk` to the outcome, as the RESP path does: a nil
/// counts in `kv_get_nil` and ends the cached prefix at its chunk; an error
/// reply ends it the same way and is counted in `request_errors` by
/// [`note_transfer`].
fn apply_get(o: &mut Outcome, chunk: usize, get: GetResult) {
    match get {
        GetResult::Hit => {}
        GetResult::Nil => {
            o.get_nil += 1;
            metrics::KV_GET_NIL.increment();
            o.hit = o.hit.min(chunk);
        }
        GetResult::ErrorReply => o.hit = o.hit.min(chunk),
    }
}

/// How a failed transfer leaves the connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Disposition {
    /// The server answered with an error; the control channel is still in step.
    ErrorReply,
    /// The connection closed.
    Closed,
    /// Anything else: the control channel, the fabric, or a receipt that
    /// failed its checks. The connection is dropped, as the
    /// `protocol = "dma"` workload drops it.
    Broken,
}

fn disposition(error: &DmaClientError) -> Disposition {
    match error {
        DmaClientError::Resp(ringline_redis::Error::Redis(_)) => Disposition::ErrorReply,
        DmaClientError::Resp(ringline_redis::Error::ConnectionClosed) => Disposition::Closed,
        _ => Disposition::Broken,
    }
}

/// `Ok` for an error reply, which the request carries on past; otherwise the
/// failure that ends the request and the connection.
fn failed(error: &DmaClientError, conn: ConnCtx) -> Result<(), Failure> {
    match disposition(error) {
        Disposition::ErrorReply => {
            tracing::debug!("kv replay: dma transfer error reply: {error}");
            Ok(())
        }
        Disposition::Closed => Err(Failure::Disconnect(DisconnectReason::Eof)),
        Disposition::Broken => {
            tracing::debug!("kv replay: dma transfer failed: {error}");
            conn.close();
            Err(Failure::Disconnect(DisconnectReason::RecvError))
        }
    }
}

/// Count one answered transfer: latency, payload bytes, and the counters the
/// RESP path's `on_result` callback and `record_counters` update. `error` is
/// the error reply, if the server answered with one; `hit` is a GET's
/// hit or miss, `None` for a SET or an error reply.
fn note_transfer(
    request_type: RequestType,
    latency_ns: u64,
    bytes: usize,
    hit: Option<bool>,
    error: Option<&DmaClientError>,
) {
    match request_type {
        RequestType::Get => {
            metrics::BYTES_RX.add(bytes as u64);
            let _ = metrics::GET_LATENCY.increment(latency_ns);
        }
        RequestType::Set => {
            metrics::BYTES_TX.add(bytes as u64);
            let _ = metrics::SET_LATENCY.increment(latency_ns);
        }
        _ => {}
    }
    let _ = metrics::RESPONSE_LATENCY.increment(latency_ns);
    // A KV replay has no rate limiter, so there is no schedule slip to add.
    let _ = metrics::PERCEIVED_LATENCY.increment(latency_ns);
    record_counters(&RequestResult {
        id: 0,
        success: error.is_none(),
        is_error_response: error.is_some(),
        latency_ns,
        // The payload never crosses the socket, so there is no first byte to
        // timestamp.
        ttfb_ns: None,
        request_type,
        hit,
        key_id: None,
        backfill: false,
        prefill: false,
        append: false,
        redirect: None,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dma::DmaError;

    #[test]
    fn a_nil_get_ends_the_prefix_and_counts_as_nil() {
        let mut o = Outcome {
            lookup_hit: 4,
            hit: 4,
            ..Default::default()
        };
        apply_get(&mut o, 0, GetResult::Hit);
        assert_eq!((o.hit, o.get_nil), (4, 0));
        apply_get(&mut o, 2, GetResult::Nil);
        assert_eq!((o.hit, o.get_nil), (2, 1));
        // A later nil does not extend the prefix again.
        apply_get(&mut o, 3, GetResult::Nil);
        assert_eq!((o.hit, o.get_nil), (2, 2));
        // An error reply cuts the prefix without counting a nil.
        apply_get(&mut o, 1, GetResult::ErrorReply);
        assert_eq!((o.hit, o.get_nil), (1, 2));
        assert_eq!(o.lookup_hit, 4);
    }

    #[test]
    fn only_an_error_reply_keeps_the_connection() {
        let redis = DmaClientError::Resp(ringline_redis::Error::Redis("ERR x".into()));
        assert_eq!(disposition(&redis), Disposition::ErrorReply);
        let closed = DmaClientError::Resp(ringline_redis::Error::ConnectionClosed);
        assert_eq!(disposition(&closed), Disposition::Closed);
        // A checksum mismatch or an oversized receipt: the bytes in the buffer
        // cannot be trusted, and the DMA workload drops the connection too.
        let integrity = DmaClientError::Dma(DmaError::Integrity("bad".into()));
        assert_eq!(disposition(&integrity), Disposition::Broken);
        let protocol = DmaClientError::Dma(DmaError::Protocol("bad".into()));
        assert_eq!(disposition(&protocol), Disposition::Broken);
    }
}
