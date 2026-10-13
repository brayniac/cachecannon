//! KV-cache prefix replay (`[workload.kv_replay]`).
//!
//! Replays a kvtrace request trace against the server as an LLM serving
//! engine's KV-cache prefix store would use it, and reports how many prompt
//! tokens were served from the cache. The request-to-object mapping (object
//! ids and sizes for a model geometry) is kvtrace's `ObjectMapper`.
//!
//! One trace request on one connection, modelling LMCache v0.5.5 (05a013b)
//! with a Redis-protocol remote backend under vLLM v0.31.0 (db9527a):
//!
//! 1. Lookup: one EXISTS per object of every full chunk on every storing
//!    rank, all sent in one pipeline. The hit prefix is the chunks before the
//!    first chunk with a missing object on any rank, which is the minimum of
//!    the per-rank prefixes.
//! 2. Load: GET every object in the hit prefix, at most
//!    `connection.pipeline_depth` in flight. A GET that returns nil (the
//!    object was evicted after its EXISTS) is counted in `kv_get_nil`, and the
//!    cached prefix ends at its chunk; a GET answered with an error ends it
//!    the same way and is counted in `request_errors`.
//! 3. Store: SET the objects the write rule selects
//!    ([`KvWriteRule`]), at most `pipeline_depth` in flight. The store starts
//!    at the lookup's prefix, not the loaded one: LMCache records the lookup
//!    result as already saved (`num_saved_tokens = lmcache_cached_tokens`,
//!    `lmcache/integration/vllm/vllm_v1_adapter.py:193,247`) and skips that
//!    many leading tokens when it stores (`:326`).
//!
//! Token accounting is in [`cached_tokens`].
//!
//! Timing: the trace's timestamps divided by `speedup` (open loop), or as
//! fast as connections free up (`speedup = 0`, closed loop). Each worker runs
//! one dispatcher task that hands trace requests to its idle connections and
//! sleeps until a request's scheduled time, so the schedule costs one timer
//! per worker rather than one per connection (see `CLAUDE.md`, "Price any
//! per-request timer").

use crate::config::{KvKeyFormat, KvWriteRule};
use crate::metrics;
use crate::output::{KvReplayResults, LatencyStats};
use crate::worker::{
    DisconnectReason, IDLE_SLEEP_MAX, InFlight, Phase, SharedState, ZC_VALUE_THRESHOLD,
    make_value_guard, map_respmeta, note_discarded, note_failed_fire, record_counters,
    recv_with_timeout, value_slice,
};
use kvtrace::{Geometry, KvObject, ModelSizes, ObjectMapper, Request, RequestReader};
use metriken::histogram::Histogram;
use rand_xoshiro::Xoshiro256PlusPlus;
use ringline::ConnCtx;
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::fs::File;
use std::future::Future;
use std::io::{BufWriter, Write};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

/// Header of the `requests_out` CSV. One row per trace request that
/// completed; see `docs/reference.md` "KV-cache replay" for the columns.
pub const REQUESTS_OUT_HEADER: &str = "req_id,timestamp_ns,lag_ns,input_tokens,chunks,\
lookup_hit_chunks,hit_chunks,cached_tokens,new_tokens,gets,get_nil,sets,lookup_ns,load_ns,\
store_ns,recorded";

/// A loaded trace and the state of its replay, shared by every worker.
pub struct KvReplay {
    /// The trace, sorted by timestamp.
    requests: Vec<Request>,
    /// Timestamp of the first request; trace time 0.
    first_ts: i64,
    chunk_tokens: u64,
    mapper: ObjectMapper,
    key_prefix: Vec<u8>,
    key_format: KvKeyFormat,
    write_rule: KvWriteRule,
    /// 0 for closed loop.
    speedup: f64,
    /// Objects of the request with the most, for sizing the send pool.
    max_objects: usize,
    /// When trace time 0 is, set by the runner as warmup starts.
    start: OnceLock<Instant>,
    /// Index of the next request to hand out.
    next: AtomicUsize,
    /// Requests finished, replayed or failed.
    finished: AtomicUsize,
    log: Option<Mutex<BufWriter<File>>>,
}

impl KvReplay {
    /// Read the model geometry and the trace, and open `requests_out`.
    pub fn load(cfg: &crate::config::KvReplay) -> Result<Self, String> {
        let geometry = geometry(cfg)?;
        let reader = RequestReader::open(&cfg.trace)
            .map_err(|e| format!("kv_replay trace {}: {e}", cfg.trace.display()))?;
        let chunk_tokens = u64::from(reader.chunk_tokens());
        let mut requests = reader
            .collect::<Result<Vec<Request>, _>>()
            .map_err(|e| format!("kv_replay trace {}: {e}", cfg.trace.display()))?;
        // With nothing to replay the run would end at once and report
        // nothing recorded, which reads as a timing problem.
        if requests.is_empty() {
            return Err(format!(
                "kv_replay trace {}: no requests",
                cfg.trace.display()
            ));
        }
        // Requests arrive in timestamp order; a stable sort keeps the file
        // order among equal timestamps.
        requests.sort_by_key(|r| r.timestamp);
        let mapper = ObjectMapper::new(&geometry, chunk_tokens)
            .map_err(|e| format!("kv_replay geometry: {e}"))?;

        let object_size = mapper.max_object_size();
        if object_size == 0 || object_size > crate::runner::VALUE_POOL_SIZE as u64 {
            return Err(format!(
                "kv_replay: an object is {object_size} bytes ({} tokens per chunk); it must be \
                 between 1 and the value pool size ({} bytes)",
                chunk_tokens,
                crate::runner::VALUE_POOL_SIZE
            ));
        }
        let ranks = mapper.sharding().ranks as usize;
        let max_objects = requests
            .iter()
            .map(|r| r.chunks.len())
            .max()
            .unwrap_or(0)
            .saturating_mul(ranks);

        let log = match cfg.requests_out {
            Some(ref path) => {
                let mut w = BufWriter::new(
                    File::create(path)
                        .map_err(|e| format!("kv_replay requests_out {}: {e}", path.display()))?,
                );
                writeln!(w, "{REQUESTS_OUT_HEADER}")
                    .map_err(|e| format!("kv_replay requests_out {}: {e}", path.display()))?;
                Some(Mutex::new(w))
            }
            None => None,
        };

        tracing::info!(
            requests = requests.len(),
            chunk_tokens,
            ranks,
            object_bytes = object_size,
            timing = if cfg.speedup == 0.0 {
                "closed-loop".to_string()
            } else {
                format!("open-loop at {}x", cfg.speedup)
            },
            write_rule = cfg.write_rule.as_str(),
            "kv replay trace loaded"
        );

        Ok(Self {
            first_ts: requests.first().map_or(0, |r| r.timestamp),
            requests,
            chunk_tokens,
            mapper,
            key_prefix: cfg.key_prefix.as_bytes().to_vec(),
            key_format: cfg.key_format,
            write_rule: cfg.write_rule,
            speedup: cfg.speedup,
            max_objects,
            start: OnceLock::new(),
            next: AtomicUsize::new(0),
            finished: AtomicUsize::new(0),
            log,
        })
    }

    /// Trace requests in the replay.
    pub fn len(&self) -> usize {
        self.requests.len()
    }

    pub fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }

    pub fn closed_loop(&self) -> bool {
        self.speedup == 0.0
    }

    /// The largest object, in bytes: the SET value length the send pool is
    /// sized for.
    pub fn max_object_size(&self) -> usize {
        self.mapper.max_object_size() as usize
    }

    /// EXISTS one request can send at once.
    pub fn max_objects(&self) -> usize {
        self.max_objects
    }

    /// Length of the longest key.
    pub fn max_key_len(&self) -> usize {
        let digits = match self.key_format {
            KvKeyFormat::Hex => 16,
            KvKeyFormat::Decimal => 20,
        };
        self.key_prefix.len() + digits
    }

    /// Start the replay clock. Called once, as warmup starts; later calls do
    /// nothing.
    pub fn start_at(&self, at: Instant) {
        let _ = self.start.set(at);
    }

    /// True once every trace request has finished.
    pub fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Acquire) >= self.requests.len()
    }

    /// Flush the `requests_out` file.
    pub fn flush_log(&self) {
        if let Some(ref log) = self.log
            && let Err(e) = log.lock().unwrap().flush()
        {
            tracing::error!("kv_replay requests_out: {e}");
        }
    }

    /// Claim the next trace request, or `None` when all have been handed out.
    fn claim(&self) -> Option<usize> {
        let i = self.next.fetch_add(1, Ordering::Relaxed);
        (i < self.requests.len()).then_some(i)
    }

    /// When request `index` is due, in open loop.
    fn scheduled_at(&self, start: Instant, index: usize) -> Option<Instant> {
        if self.closed_loop() {
            return None;
        }
        let trace_ns = self.requests[index].timestamp.saturating_sub(self.first_ts) as f64;
        Some(start + Duration::from_nanos((trace_ns / self.speedup) as u64))
    }

    fn mark_finished(&self) {
        self.finished.fetch_add(1, Ordering::Release);
    }

    /// Append `key_prefix` and `id` to `buf`.
    fn push_key(&self, buf: &mut Vec<u8>, id: u64) {
        buf.extend_from_slice(&self.key_prefix);
        match self.key_format {
            KvKeyFormat::Hex => write!(buf, "{id:016x}"),
            KvKeyFormat::Decimal => write!(buf, "{id}"),
        }
        .expect("writing to a Vec cannot fail");
    }

    fn write_row(&self, req: &Request, lag_ns: Option<u64>, o: &Outcome, recorded: bool) {
        let Some(ref log) = self.log else {
            return;
        };
        let lag = lag_ns.map_or(String::new(), |l| l.to_string());
        let mut w = log.lock().unwrap();
        if let Err(e) = writeln!(
            w,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            req.req_id,
            req.timestamp,
            lag,
            req.input_tokens,
            req.chunks.len(),
            o.lookup_hit,
            o.hit,
            o.cached_tokens,
            o.new_tokens,
            o.gets,
            o.get_nil,
            o.sets,
            o.lookup_ns,
            o.load_ns,
            o.store_ns,
            u8::from(recorded),
        ) {
            tracing::error!("kv_replay requests_out: {e}");
        }
    }
}

/// The model geometry `cfg` describes.
///
/// Hybrid models (linear-attention layers with a recurrent state) are
/// rejected: kvtrace's hybrid geometry is an approximation whose object
/// sizes, allowed chunk length and snapshot count differ from vLLM and
/// LMCache (kvtrace `objects` module docs, "Hybrid models").
pub fn geometry(cfg: &crate::config::KvReplay) -> Result<Geometry, String> {
    let sizes = match (&cfg.model_config, cfg.kv_bytes_per_token) {
        (Some(path), _) => ModelSizes::from_config_file(path, cfg.kv_dtype.as_deref())
            .map_err(|e| format!("kv_replay model_config {}: {e}", path.display()))?,
        (None, Some(bytes)) => {
            let mut sizes = ModelSizes::explicit(bytes, 0);
            sizes.mla = cfg.mla;
            sizes
        }
        (None, None) => return Err("kv_replay: no model geometry".into()),
    };
    if sizes.state_bytes_per_sequence > 0 {
        return Err(format!(
            "kv_replay: the model is a hybrid with {} recurrent-state bytes per sequence. \
             Hybrid models are not supported: kvtrace's hybrid geometry is an approximation \
             (object sizes, chunk length and snapshot count differ from vLLM and LMCache)",
            sizes.state_bytes_per_sequence
        ));
    }
    let mut geometry = Geometry::new(sizes);
    geometry.tp = cfg.tp;
    // Checks the TP degree against the head counts.
    geometry
        .sharding()
        .map_err(|e| format!("kv_replay geometry: {e}"))?;
    Ok(geometry)
}

/// Chunks of the lookup's prefix hit: the chunks before the first one with a
/// missing object on any rank. `exists` is in `ObjectMapper::objects` order,
/// chunk by chunk with `ranks` objects each.
pub fn lookup_prefix(exists: &[bool], ranks: usize) -> usize {
    exists
        .chunks(ranks.max(1))
        .take_while(|chunk| chunk.iter().all(|&e| e))
        .count()
}

/// Prompt tokens served from the cache when the first `hit_chunks` chunks of
/// a prompt of `input_tokens` were loaded.
///
/// `hit_chunks * chunk_tokens`, except when that covers the whole prompt:
/// vLLM must compute at least the last prompt token to produce output, so
/// LMCache reports one token fewer on a full-prompt hit
/// (`lmcache/integration/vllm/vllm_v1_adapter.py:1443-1445`, "In full-prompt-hit
/// case, we need to recompute the last token"). A trace whose `input_tokens`
/// is below its full chunks' tokens is capped the same way, and a prompt of 0
/// tokens has 0 cached: kvtrace keeps `input_tokens` as the source gives it,
/// so it can disagree with the chunks. The result never exceeds
/// `input_tokens`.
pub fn cached_tokens(hit_chunks: u64, chunk_tokens: u64, input_tokens: u64) -> u64 {
    let hit = hit_chunks.saturating_mul(chunk_tokens);
    if hit >= input_tokens {
        input_tokens.saturating_sub(1)
    } else {
        hit
    }
}

/// What one trace request did.
#[derive(Debug, Default)]
struct Outcome {
    /// Chunks of the lookup's prefix hit.
    lookup_hit: usize,
    /// Chunks of the prefix actually loaded: the lookup's, cut at the first
    /// GET that returned nil or an error.
    hit: usize,
    cached_tokens: u64,
    new_tokens: u64,
    /// The loaded prefix covered the whole prompt.
    full_hit: bool,
    gets: usize,
    get_nil: usize,
    sets: usize,
    lookup_ns: u64,
    load_ns: u64,
    store_ns: u64,
}

/// Why a trace request did not complete.
enum Failure {
    /// The connection must be dropped.
    Disconnect(DisconnectReason),
    /// The server answered the lookup with an error, or the lookup could not
    /// be sent; the connection is still usable.
    Request,
}

// ── Dispatcher ─────────────────────────────────────────────────────────────

/// A trace request handed to a connection.
#[derive(Debug, Clone, Copy)]
struct KvJob {
    index: usize,
    /// When the trace says it starts; `None` in closed loop.
    scheduled: Option<Instant>,
}

/// One idle connection waiting for a trace request.
struct KvSlot {
    job: Cell<Option<KvJob>>,
    /// Set when the dispatcher is done with the slot, with or without a job.
    done: Cell<bool>,
    waker: RefCell<Option<Waker>>,
}

impl KvSlot {
    fn complete(&self, job: Option<KvJob>) {
        self.job.set(job);
        self.done.set(true);
        if let Some(w) = self.waker.borrow_mut().take() {
            w.wake();
        }
    }
}

/// Worker-local queue of idle connections, served by `run_dispatcher`. The
/// same shape as the rate-limit `TokenDispatcher` in `worker.rs`: connections
/// wait untimed on a slot, and the one dispatcher task per worker holds the
/// only timer.
#[derive(Default)]
struct KvDispatcher {
    waiting: RefCell<VecDeque<Rc<KvSlot>>>,
    /// Set once the run stopped, or the dispatcher task failed to start.
    /// Sticky, so a connection that asks later does not wait for a dispatcher
    /// that is gone.
    closed: Cell<bool>,
    /// The dispatcher's waker while no connection is waiting.
    idle: RefCell<Option<Waker>>,
}

impl KvDispatcher {
    /// Wait for the next trace request. Once the trace is exhausted this
    /// waits until the run stops, then returns `None`.
    async fn next_job(&self) -> Option<KvJob> {
        if self.closed.get() {
            return None;
        }
        let slot = Rc::new(KvSlot {
            job: Cell::new(None),
            done: Cell::new(false),
            waker: RefCell::new(None),
        });
        self.waiting.borrow_mut().push_back(Rc::clone(&slot));
        if let Some(w) = self.idle.borrow_mut().take() {
            w.wake();
        }
        SlotWait {
            slot: Rc::clone(&slot),
        }
        .await;
        slot.job.get()
    }

    /// Release every waiting connection without a job, for good.
    fn close(&self) {
        self.closed.set(true);
        for slot in self.waiting.borrow_mut().drain(..) {
            slot.complete(None);
        }
    }
}

struct SlotWait {
    slot: Rc<KvSlot>,
}

impl Future for SlotWait {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.slot.done.get() {
            return Poll::Ready(());
        }
        *self.slot.waker.borrow_mut() = Some(cx.waker().clone());
        Poll::Pending
    }
}

thread_local! {
    /// This worker's dispatcher; `None` unless the run replays a KV trace.
    static DISPATCHER: RefCell<Option<Rc<KvDispatcher>>> = const { RefCell::new(None) };
}

/// Install this worker's dispatcher and spawn its task. Called from the
/// worker's `on_start`, before any connection task runs.
pub(crate) fn start_dispatcher(kv: Arc<KvReplay>, shared: Arc<SharedState>, worker_id: usize) {
    let dispatch = Rc::new(KvDispatcher::default());
    DISPATCHER.with(|d| *d.borrow_mut() = Some(Rc::clone(&dispatch)));
    if let Err(e) = ringline::spawn(run_dispatcher(dispatch, kv, shared)) {
        // Without it no connection on this worker gets a trace request, and
        // the run would wait for requests nobody hands out. A closed
        // dispatcher sends this worker's connections idle instead, and the
        // other workers replay the whole trace.
        tracing::error!(
            worker = worker_id,
            "failed to spawn the kv replay dispatcher: {e}; this worker's connections \
             replay nothing"
        );
        DISPATCHER.with(|d| {
            if let Some(d) = d.borrow().as_ref() {
                d.close();
            }
        });
    }
}

/// How often a dispatcher checks whether the replay clock has started. At
/// `IDLE_SLEEP_MAX` (50 ms) the first request of an open-loop replay started
/// up to that late; 1 ms costs a thousand wakeups a second per worker, only
/// until the replay starts.
const START_POLL: Duration = Duration::from_millis(1);

fn dispatcher() -> Option<Rc<KvDispatcher>> {
    DISPATCHER.with(|d| d.borrow().clone())
}

/// Hand trace requests to this worker's idle connections until the run
/// stops.
///
/// A request is claimed only once a connection is waiting, so a worker whose
/// connections are all busy leaves the next request to another worker. In
/// open loop the dispatcher then sleeps until the request is due, in steps of
/// at most `IDLE_SLEEP_MAX` so a stop is noticed; a request claimed after it
/// was due starts at once, and `kv_schedule_lag` records how late.
async fn run_dispatcher(dispatch: Rc<KvDispatcher>, kv: Arc<KvReplay>, shared: Arc<SharedState>) {
    loop {
        if shared.phase().should_stop() {
            dispatch.close();
            return;
        }
        // The runner starts the clock (`KvReplay::start_at`). Polled at
        // `START_POLL` until then, which bounds how late the first request
        // can be.
        let Some(&start) = kv.start.get() else {
            ringline::sleep(START_POLL).await;
            continue;
        };
        let Some(slot) = dispatch.waiting.borrow_mut().pop_front() else {
            // No idle connection. Wait for one to ask, with `IDLE_SLEEP_MAX`
            // as a backstop so a stop is noticed while all are busy.
            let mut backstop = std::pin::pin!(ringline::sleep(IDLE_SLEEP_MAX));
            std::future::poll_fn(|cx| {
                if !dispatch.waiting.borrow().is_empty() {
                    return Poll::Ready(());
                }
                *dispatch.idle.borrow_mut() = Some(cx.waker().clone());
                backstop.as_mut().poll(cx)
            })
            .await;
            dispatch.idle.borrow_mut().take();
            continue;
        };
        let Some(index) = kv.claim() else {
            // The trace is exhausted. Idle connections stay parked, untimed,
            // until the run stops: woken now, each would poll on its own timer
            // while other connections finish the last requests.
            dispatch.waiting.borrow_mut().push_front(slot);
            while !shared.phase().should_stop() {
                ringline::sleep(IDLE_SLEEP_MAX).await;
            }
            dispatch.close();
            return;
        };
        let scheduled = kv.scheduled_at(start, index);
        if let Some(due) = scheduled {
            loop {
                if shared.phase().should_stop() {
                    slot.complete(None);
                    dispatch.close();
                    return;
                }
                let now = Instant::now();
                if now >= due {
                    break;
                }
                ringline::sleep((due - now).min(IDLE_SLEEP_MAX)).await;
            }
        }
        slot.complete(Some(KvJob { index, scheduled }));
    }
}

// ── Connection side ──────────────────────────────────────────────────────

/// Per-connection buffers reused across trace requests.
struct Scratch {
    objects: Vec<KvObject>,
    /// Every object's key, back to back; `key_ends[i]` is where key `i` ends.
    keys: Vec<u8>,
    key_ends: Vec<usize>,
    exists: Vec<bool>,
    /// Indices into `objects` of the SETs to send.
    sets: Vec<usize>,
}

impl Scratch {
    fn key(&self, i: usize) -> &[u8] {
        let start = if i == 0 { 0 } else { self.key_ends[i - 1] };
        &self.keys[start..self.key_ends[i]]
    }
}

/// What the connection task hands the replay loop.
pub(crate) struct KvConn<'a> {
    pub kv: &'a Arc<KvReplay>,
    pub shared: &'a Arc<SharedState>,
    pub value_pool: &'a Arc<Vec<u8>>,
    pub pipeline_depth: usize,
    pub request_timeout: Duration,
}

/// Replay trace requests on one connected RESP client until the run stops or
/// the connection fails.
pub(crate) async fn drive(
    client: &mut ringline_redis::Client,
    conn: ConnCtx,
    ctx: &KvConn<'_>,
    rng: &mut Xoshiro256PlusPlus,
) -> Result<(), DisconnectReason> {
    let Some(dispatch) = dispatcher() else {
        // `on_start` installs it before spawning connections.
        unreachable!("kv replay connection without a dispatcher");
    };
    let mut inflight = InFlight::new(ctx.request_timeout, ctx.pipeline_depth);
    let mut scratch = Scratch {
        objects: Vec::new(),
        keys: Vec::new(),
        key_ends: Vec::new(),
        exists: Vec::new(),
        sets: Vec::new(),
    };
    loop {
        if ctx.shared.phase().should_stop() {
            return Ok(());
        }
        if !client.is_alive() {
            return Err(DisconnectReason::Eof);
        }
        let Some(job) = dispatch.next_job().await else {
            // The run stopped. Otherwise this worker's dispatcher failed to
            // start; stay connected until the run stops, so the connection
            // count the runner reports is the one the run had.
            if ctx.shared.phase().should_stop() {
                return Ok(());
            }
            ringline::sleep(IDLE_SLEEP_MAX).await;
            continue;
        };
        let req = &ctx.kv.requests[job.index];
        let started = Instant::now();
        let lag_ns = job
            .scheduled
            .map(|due| started.saturating_duration_since(due).as_nanos() as u64);
        let result = run_request(client, conn, ctx, rng, &mut inflight, &mut scratch, req).await;
        ctx.kv.mark_finished();
        match result {
            Ok(outcome) => {
                // Recorded with the rest of the request's samples, when it
                // finishes, so all of them fall in the same phase.
                if let Some(lag) = lag_ns {
                    let _ = metrics::KV_SCHEDULE_LAG.increment(lag);
                }
                record_outcome(req, &outcome);
                let recorded = ctx.shared.phase() == Phase::Running;
                ctx.kv.write_row(req, lag_ns, &outcome, recorded);
            }
            Err(Failure::Request) => metrics::KV_REQUESTS_FAILED.increment(),
            Err(Failure::Disconnect(reason)) => {
                metrics::KV_REQUESTS_FAILED.increment();
                return Err(reason);
            }
        }
    }
}

fn record_outcome(req: &Request, o: &Outcome) {
    metrics::KV_REQUESTS.increment();
    metrics::KV_INPUT_TOKENS.add(u64::from(req.input_tokens));
    metrics::KV_CACHED_TOKENS.add(o.cached_tokens);
    metrics::KV_NEW_TOKENS.add(o.new_tokens);
    if req.chunks.is_empty() {
        metrics::KV_NO_FULL_CHUNK.increment();
        return;
    }
    metrics::KV_LOOKUPS.increment();
    let _ = metrics::KV_LOOKUP_TIME.increment(o.lookup_ns);
    if o.lookup_hit == 0 {
        metrics::KV_CHUNK0_MISSES.increment();
    }
    if o.full_hit {
        metrics::KV_FULL_HITS.increment();
    }
    if o.gets > 0 {
        metrics::KV_LOADS.increment();
        let _ = metrics::KV_LOAD_TIME.increment(o.load_ns);
        // Only a load that left cached tokens has a time per token. A nil GET
        // at the first chunk loads nothing.
        if let Some(per_1k) = o.load_ns.saturating_mul(1000).checked_div(o.cached_tokens) {
            metrics::KV_LOADS_WITH_CACHED.increment();
            let _ = metrics::KV_LOAD_TIME_PER_1K_TOKENS.increment(per_1k);
        }
    }
    if o.sets > 0 {
        metrics::KV_STORES.increment();
        let _ = metrics::KV_STORE_TIME.increment(o.store_ns);
    }
}

/// Look up, load and store one trace request.
async fn run_request(
    client: &mut ringline_redis::Client,
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

    // The trace was checked against the mapper at load.
    s.objects.clear();
    s.objects.extend(
        kv.mapper
            .objects(&req.chunks)
            .expect("chunk ids checked at load"),
    );
    s.keys.clear();
    s.key_ends.clear();
    for obj in &s.objects {
        kv.push_key(&mut s.keys, obj.obj_id);
        s.key_ends.push(s.keys.len());
    }
    let ranks = kv.mapper.sharding().ranks as usize;
    let n = s.objects.len();

    // 1. Lookup: every EXISTS in one pipeline.
    let mut pipeline = client.pipeline();
    let mut tx_bytes = 0u64;
    for i in 0..n {
        let key = s.key(i);
        // `*2\r\n$6\r\nEXISTS\r\n` then `$<len>\r\n<key>\r\n`
        tx_bytes += 16 + (1 + decimal_len(key.len()) + 2 + key.len() + 2) as u64;
        pipeline = pipeline.cmd(&resp_proto::Request::cmd(b"EXISTS").arg(key));
    }
    let fired_at = Instant::now();
    for _ in 0..n {
        inflight.push(fired_at);
    }
    let replies = recv_with_timeout(pipeline.execute(), inflight, conn)
        .await
        .map_err(Failure::Disconnect)?;
    inflight.forget_newest(n);
    o.lookup_ns = fired_at.elapsed().as_nanos() as u64;
    let values = match replies {
        Ok(values) => values,
        Err(ringline_redis::Error::Io(_)) => {
            // `execute` sends nothing when the send pool cannot take the whole
            // pipeline.
            metrics::REQUESTS_SEND_FAILED.add(n as u64);
            return Err(Failure::Request);
        }
        Err(ringline_redis::Error::Redis(msg)) => {
            // `execute` read every reply before returning the error.
            tracing::debug!("kv replay: EXISTS error reply: {msg}");
            note_lookup(n, tx_bytes);
            metrics::REQUEST_ERRORS.increment();
            return Err(Failure::Request);
        }
        Err(ringline_redis::Error::ConnectionClosed) => {
            return Err(Failure::Disconnect(DisconnectReason::Eof));
        }
        Err(e) => {
            // `execute` closed the connection: replies are positional, and a
            // failed parse leaves the stream at an unknown position.
            tracing::debug!("kv replay: EXISTS pipeline failed: {e}");
            return Err(Failure::Disconnect(DisconnectReason::RecvError));
        }
    };
    note_lookup(n, tx_bytes);
    s.exists.clear();
    for v in values {
        match v {
            resp_proto::Value::Integer(c) => s.exists.push(c > 0),
            other => {
                tracing::debug!("kv replay: unexpected EXISTS reply {other:?}");
                metrics::REQUEST_ERRORS.increment();
                return Err(Failure::Request);
            }
        }
    }
    o.lookup_hit = lookup_prefix(&s.exists, ranks);
    o.hit = o.lookup_hit;

    // 2. Load: GET every object of the hit prefix.
    let gets = o.lookup_hit * ranks;
    if gets > 0 {
        let t0 = Instant::now();
        let mut next = 0;
        let mut done = 0;
        while done < gets {
            let fired_at = Instant::now();
            let mut fired = 0u64;
            while next < gets && client.pending_count() < ctx.pipeline_depth {
                let before = client.pending_count();
                if client.fire_get(s.key(next), next as u64).is_err() {
                    note_failed_fire(before, client.pending_count(), inflight, true);
                    metrics::REQUESTS_SENT.add(fired);
                    return Err(send_failed(conn));
                }
                inflight.push(fired_at);
                next += 1;
                fired += 1;
            }
            flush(client, conn, inflight, fired)?;
            let meta = recv_with_timeout(client.recv_meta(), inflight, conn)
                .await
                .map_err(Failure::Disconnect)?
                .map_err(recv_failed)?;
            inflight.pop();
            done += 1;
            let chunk = s.objects[meta.user_data as usize].chunk_index as usize;
            let result = map_respmeta(meta);
            record_counters(&result);
            if !result.success || result.hit == Some(false) {
                if result.success {
                    o.get_nil += 1;
                    metrics::KV_GET_NIL.increment();
                }
                o.hit = o.hit.min(chunk);
            }
        }
        o.gets = gets;
        o.load_ns = t0.elapsed().as_nanos() as u64;
    }

    // 3. Store.
    s.sets.clear();
    match kv.write_rule {
        KvWriteRule::SetFromMiss => s.sets.extend(o.lookup_hit * ranks..n),
        KvWriteRule::InsertIfAbsent => {
            s.sets.extend(
                s.exists
                    .iter()
                    .enumerate()
                    .filter(|(_, e)| !**e)
                    .map(|(i, _)| i),
            );
        }
    }
    let sets = s.sets.len();
    if sets > 0 {
        let pool_len = ctx.value_pool.len();
        let t0 = Instant::now();
        let mut next = 0;
        let mut done = 0;
        while done < sets {
            let fired_at = Instant::now();
            let mut fired = 0u64;
            while next < sets && client.pending_count() < ctx.pipeline_depth {
                let i = s.sets[next];
                let len = s.objects[i].size as usize;
                let before = client.pending_count();
                let sent = if len >= ZC_VALUE_THRESHOLD {
                    let guard = make_value_guard(rng, ctx.value_pool, len, pool_len);
                    client.fire_set_with_guard(s.key(i), guard, i as u64)
                } else {
                    let value = value_slice(rng, ctx.value_pool, len, pool_len);
                    client.fire_set(s.key(i), value, i as u64)
                };
                if sent.is_err() {
                    note_failed_fire(before, client.pending_count(), inflight, true);
                    metrics::REQUESTS_SENT.add(fired);
                    return Err(send_failed(conn));
                }
                inflight.push(fired_at);
                next += 1;
                fired += 1;
            }
            flush(client, conn, inflight, fired)?;
            let meta = recv_with_timeout(client.recv_meta(), inflight, conn)
                .await
                .map_err(Failure::Disconnect)?
                .map_err(recv_failed)?;
            inflight.pop();
            done += 1;
            record_counters(&map_respmeta(meta));
        }
        o.sets = sets;
        o.store_ns = t0.elapsed().as_nanos() as u64;
    }

    o.cached_tokens = cached_tokens(o.hit as u64, kv.chunk_tokens, input);
    // `cached_tokens` never exceeds its input; the subtraction is checked so
    // a regression there fails loudly in tests instead of panicking a
    // connection task in a debug build or wrapping the counter in release.
    debug_assert!(
        o.cached_tokens <= input,
        "cached {} > input {input}",
        o.cached_tokens
    );
    o.new_tokens = input.saturating_sub(o.cached_tokens);
    o.full_hit = input > 0 && (o.hit as u64).saturating_mul(kv.chunk_tokens) >= input;
    Ok(o)
}

/// Count `n` EXISTS sent and answered. The pipeline has no result callback,
/// so the bytes are counted here: `tx_bytes` as encoded, and a 4-byte
/// `:0\r\n` or `:1\r\n` reply each.
fn note_lookup(n: usize, tx_bytes: u64) {
    let n = n as u64;
    metrics::REQUESTS_SENT.add(n);
    metrics::RESPONSES_RECEIVED.add(n);
    metrics::KV_EXISTS_COUNT.add(n);
    metrics::BYTES_TX.add(tx_bytes);
    metrics::BYTES_RX.add(4 * n);
}

fn decimal_len(mut v: usize) -> usize {
    let mut digits = 1;
    while v >= 10 {
        v /= 10;
        digits += 1;
    }
    digits
}

/// Send what the fire loop buffered and count it.
fn flush(
    client: &mut ringline_redis::Client,
    conn: ConnCtx,
    inflight: &mut InFlight,
    fired: u64,
) -> Result<(), Failure> {
    let before = client.pending_count();
    if client.flush().is_err() {
        let discarded = note_discarded(before, client.pending_count(), inflight, true);
        metrics::REQUESTS_SENT.add(fired - discarded as u64);
        return Err(send_failed(conn));
    }
    metrics::REQUESTS_SENT.add(fired);
    Ok(())
}

/// A GET or SET of a request could not be sent. Earlier ones of the same
/// request are on the wire, so the request cannot finish in order; drop the
/// connection and let it reconnect.
fn send_failed(conn: ConnCtx) -> Failure {
    conn.close();
    Failure::Disconnect(DisconnectReason::SendError)
}

fn recv_failed(e: ringline_redis::Error) -> Failure {
    match e {
        ringline_redis::Error::ConnectionClosed => Failure::Disconnect(DisconnectReason::Eof),
        _ => Failure::Disconnect(DisconnectReason::RecvError),
    }
}

// ── Results ──────────────────────────────────────────────────────────────

/// KV replay counters and histograms at the start of the recording phase,
/// subtracted from the final values as the runner does for every other
/// metric.
pub struct KvBaseline {
    counters: [u64; 14],
    lookup: Option<Histogram>,
    load: Option<Histogram>,
    store: Option<Histogram>,
    load_per_1k: Option<Histogram>,
    lag: Option<Histogram>,
}

fn kv_counters() -> [u64; 14] {
    [
        metrics::KV_REQUESTS.value(),
        metrics::KV_REQUESTS_FAILED.value(),
        metrics::KV_INPUT_TOKENS.value(),
        metrics::KV_CACHED_TOKENS.value(),
        metrics::KV_NEW_TOKENS.value(),
        metrics::KV_FULL_HITS.value(),
        metrics::KV_CHUNK0_MISSES.value(),
        metrics::KV_NO_FULL_CHUNK.value(),
        metrics::KV_GET_NIL.value(),
        metrics::KV_EXISTS_COUNT.value(),
        metrics::KV_LOOKUPS.value(),
        metrics::KV_LOADS.value(),
        metrics::KV_STORES.value(),
        metrics::KV_LOADS_WITH_CACHED.value(),
    ]
}

impl KvBaseline {
    pub fn take() -> Self {
        Self {
            counters: kv_counters(),
            lookup: metrics::KV_LOOKUP_TIME.load(),
            load: metrics::KV_LOAD_TIME.load(),
            store: metrics::KV_STORE_TIME.load(),
            load_per_1k: metrics::KV_LOAD_TIME_PER_1K_TOKENS.load(),
            lag: metrics::KV_SCHEDULE_LAG.load(),
        }
    }
}

impl KvReplay {
    /// Totals and distributions since `base`.
    pub fn results(&self, base: &KvBaseline) -> KvReplayResults {
        let now = kv_counters();
        let d = |i: usize| now[i].saturating_sub(base.counters[i]);
        let stats = |h: &metriken::AtomicHistogram, b: &Option<Histogram>| -> LatencyStats {
            crate::runner::delta_latency_stats(h, b)
        };
        KvReplayResults {
            closed_loop: self.closed_loop(),
            speedup: self.speedup,
            write_rule: self.write_rule.as_str(),
            trace_requests: self.requests.len() as u64,
            requests: d(0),
            requests_failed: d(1),
            input_tokens: d(2),
            cached_tokens: d(3),
            new_tokens: d(4),
            full_hits: d(5),
            chunk0_misses: d(6),
            no_full_chunk: d(7),
            get_nil: d(8),
            exists: d(9),
            lookups: d(10),
            loads: d(11),
            stores: d(12),
            loads_with_cached_tokens: d(13),
            lookup_time: stats(&metrics::KV_LOOKUP_TIME, &base.lookup),
            load_time: stats(&metrics::KV_LOAD_TIME, &base.load),
            store_time: stats(&metrics::KV_STORE_TIME, &base.store),
            load_time_per_1k_tokens: stats(&metrics::KV_LOAD_TIME_PER_1K_TOKENS, &base.load_per_1k),
            schedule_lag: stats(&metrics::KV_SCHEDULE_LAG, &base.lag),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_prefix_is_the_minimum_over_ranks() {
        // 3 chunks x 2 ranks, chunk-major.
        assert_eq!(lookup_prefix(&[true, true, true, true, true, true], 2), 3);
        // Rank 1 is missing chunk 1: the prefix ends there although rank 0
        // has every chunk.
        assert_eq!(lookup_prefix(&[true, true, true, false, true, true], 2), 1);
        // A chunk after the gap does not extend the prefix.
        assert_eq!(lookup_prefix(&[false, true, true, true], 1), 0);
        assert_eq!(lookup_prefix(&[], 2), 0);
    }

    #[test]
    fn a_full_prompt_hit_leaves_the_last_token_to_compute() {
        // 4 chunks of 16 tokens, prompt of 64: every token is cached but one.
        assert_eq!(cached_tokens(4, 16, 64), 63);
        // A partial tail is never stored, so a hit on every full chunk of a
        // 70-token prompt is not a full-prompt hit.
        assert_eq!(cached_tokens(4, 16, 70), 64);
        assert_eq!(cached_tokens(2, 16, 70), 32);
        assert_eq!(cached_tokens(0, 16, 70), 0);
        // The trace's chunks cover more than its input_tokens.
        assert_eq!(cached_tokens(4, 16, 50), 49);
        assert_eq!(cached_tokens(0, 16, 0), 0);
        // A 0-token prompt with chunks: nothing cached, whatever was found.
        assert_eq!(cached_tokens(2, 16, 0), 0);
        assert_eq!(cached_tokens(1, 16, 1), 0);
        for (h, input) in [(4, 64), (4, 50), (2, 0), (3, 1), (0, 0), (u64::MAX, 7)] {
            assert!(
                cached_tokens(h, 16, input) <= input,
                "{h} chunks, {input} tokens"
            );
        }
    }

    fn cfg(kv_bytes_per_token: Option<u64>) -> crate::config::KvReplay {
        crate::config::KvReplay {
            trace: "unused".into(),
            speedup: 0.0,
            model_config: None,
            kv_dtype: None,
            kv_bytes_per_token,
            mla: false,
            tp: 1,
            write_rule: KvWriteRule::SetFromMiss,
            key_prefix: "kv:".into(),
            key_format: KvKeyFormat::Hex,
            requests_out: None,
        }
    }

    #[test]
    fn an_empty_trace_is_rejected() {
        let path = std::env::temp_dir().join(format!(
            "cachecannon-kv-empty-{}.parquet",
            std::process::id()
        ));
        kvtrace::RequestWriter::create(&path, 16, 1)
            .unwrap()
            .finish()
            .unwrap();
        let mut c = cfg(Some(4));
        c.trace = path.clone();
        let err = KvReplay::load(&c).err().unwrap();
        std::fs::remove_file(&path).unwrap();
        assert!(err.ends_with("no requests"), "{err}");
    }

    #[test]
    fn tp_must_fit_the_geometry() {
        let mut c = cfg(Some(10));
        c.tp = 4;
        // 10 bytes per token do not split four ways.
        let err = geometry(&c).unwrap_err();
        assert!(err.contains("split evenly"), "{err}");
        c.tp = 2;
        assert_eq!(geometry(&c).unwrap().sharding().unwrap().ranks, 2);
        // MLA: one storing rank whatever the TP degree.
        c.tp = 4;
        c.mla = true;
        assert_eq!(geometry(&c).unwrap().sharding().unwrap().ranks, 1);
    }
}
