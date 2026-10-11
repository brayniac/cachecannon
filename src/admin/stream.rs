//! `GET /metrics/stream`: the registry as dendro replication frames, through
//! metriken-exposition's stream route.
//!
//! The response is a status line and headers with no `Content-Length` and
//! `Connection: close`, then the subscription's opening (dendro's preamble
//! and the handshake) and one frame per interval until a write fails. This is
//! what `rezolus record` reads to put cachecannon into a `.dendro` archive as
//! it puts an agent there: rows stamped here at read time, histograms as
//! histograms.
//!
//! Every metric goes through [`DefaultRouter`], so cachecannon's metrics and
//! the ringline ones linked into it land in one windowless group,
//! `cachecannon/main`.

use std::collections::BTreeMap;
use std::io;
use std::time::{Duration, SystemTime};

use metriken_exposition::group_builder::{DefaultRouter, GroupBuilder};
use metriken_exposition::stream::{Refusal, RegistryPasses, StreamRequest, StreamSubscription};
use tokio::io::{AsyncWrite, AsyncWriteExt};

/// The namespace every group is under: `cachecannon/main`.
pub const NAMESPACE: &str = "cachecannon";

/// How long a built pass is reused. Well below the shortest interval a
/// recorder asks for: at an interval equal to the floor, wake-time jitter can
/// hand a subscriber the previous boundary's pass, and that interval's frame
/// is empty (`RegistryPasses::new`). rezolus uses the same value under
/// one-second intervals.
pub const UPDATE_FLOOR: Duration = Duration::from_millis(10);

/// The passes every subscription on this admin server shares: one registry
/// read and one encode per pass, however many recorders are subscribed.
pub type Passes = RegistryPasses<DefaultRouter>;

pub fn passes() -> Passes {
    RegistryPasses::new(
        GroupBuilder::new(DefaultRouter::new(NAMESPACE)),
        UPDATE_FLOOR,
    )
}

/// The handshake's labels: what a recorder names this source by.
fn labels() -> BTreeMap<String, String> {
    [("source".to_string(), "cachecannon".to_string())].into()
}

/// The handshake's metadata. The version is here, not in `labels`, so two
/// builds recorded side by side are the same source kind.
fn metadata() -> BTreeMap<String, String> {
    [("version".to_string(), env!("CARGO_PKG_VERSION").to_string())].into()
}

/// Serve one stream request on `socket` until a write fails, which is how a
/// client that disconnects ends it. `query` is the request target's query
/// string without the `?`.
pub async fn serve<S: AsyncWrite + Unpin>(
    socket: &mut S,
    query: &str,
    passes: &Passes,
) -> io::Result<()> {
    let request = match StreamRequest::parse(query) {
        Ok(request) => request,
        Err(refusal) => return socket.write_all(&refusal_response(&refusal)).await,
    };
    let layout = request.layout;
    let mut subscription = StreamSubscription::new(request, passes.floor(), labels(), metadata());

    socket.write_all(&head(&subscription)).await?;
    socket.write_all(&subscription.opening()?).await?;

    loop {
        let before = SystemTime::now();
        let deadline = subscription.next_deadline(before);
        tokio::time::sleep(deadline.duration_since(before).unwrap_or_default()).await;
        // Read on waking and before the pass: `seq` names the boundary woken
        // for.
        let now = SystemTime::now();
        let pass = passes.pass(layout, now);
        let frame = subscription.frame_at(pass.as_ref(), now)?;
        socket.write_all(&frame).await?;
    }
}

/// The status line and headers: `Content-Type` and the `x-metriken-*`
/// headers from the subscription. No `Content-Length`: the body runs until
/// the connection closes.
fn head(subscription: &StreamSubscription) -> Vec<u8> {
    let mut head = String::from("HTTP/1.1 200 OK\r\n");
    for (name, value) in subscription.headers() {
        head.push_str(name);
        head.push_str(": ");
        head.push_str(&value);
        head.push_str("\r\n");
    }
    head.push_str("Connection: close\r\n\r\n");
    head.into_bytes()
}

/// A refused request (a malformed or zero interval, an unknown layout, a
/// parameter given twice), answered with the status and message the route
/// gives.
fn refusal_response(refusal: &Refusal) -> Vec<u8> {
    let reason = match refusal.status {
        400 => "Bad Request",
        _ => "Error",
    };
    format!(
        "HTTP/1.1 {} {}\r\n\
         Content-Type: text/plain\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {}",
        refusal.status,
        reason,
        refusal.message.len(),
        refusal.message
    )
    .into_bytes()
}
