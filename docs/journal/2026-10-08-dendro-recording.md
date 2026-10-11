# Recording cachecannon into `.dendro`

**Status:** OPEN. Plan opened 2026-10-08. Steps 2 and 3 built 2026-10-11 on
branch `feat/metriken-stream` ("As built"), against unreleased ringline main
(dfff2e5) through `[patch.crates-io]`; steps 1 and 4 to 6 remain. Decided 2026-10-08: `cachecannon view` is
bumped to read `.dendro`, not retired.

Related entries opened the same day: rezolus
`docs/journal/2026-10-08-6-0-release-readiness.md`, metriken
`docs/journal/2026-10-08-one-recording-stack.md` (one recording, exposition
and viewing stack for every metriken producer), systemslab
`docs/journal/2026-10-08-dendro-artifacts.md`. #176 proposes recording to an
archive in process; this entry updates its route and adds the stream.

## Today

- `metriken` 0.9, `metriken-exposition` 0.16, `metriken-query` 0.9
  (`Cargo.toml`).
- `--parquet` / `[admin] parquet`: `run_parquet_recorder`
  (`src/admin/mod.rs`) appends a msgpack snapshot to a temporary file on a
  `sleep(interval)` loop and converts it to parquet at exit. #176 lists what
  follows: nothing is readable until the run ends, a killed run leaves only
  the temporary file, the cadence drifts, and there are no acquisition
  windows.
- `[admin] listen` serves `/metrics` (Prometheus text) and `/metrics/binary`
  (a msgpack snapshot).
- `cachecannon view` reads parquet through `metriken-query` 0.9
  (`src/viewer/`).
- `docs/guide.md`, "One archive per run", records cachecannon beside two
  agents with
  `rezolus record --endpoint http://localhost:9090,source=cachecannon,role=loadgen ... -o run.rez`.

## What rezolus 6.0 changes

6.0's `record` writes `.dendro` when `-o` is not given. With `.dendro`
output, `record` probes `/metrics/binary`, takes cachecannon for a Rezolus
agent, asks it for `/metrics/stream`, gets a 404 and refuses the whole run at
startup. The guide's command works only because it names `-o run.rez`. Two
fixes, either sufficient:

- the guide names the Prometheus endpoint:
  `--endpoint http://localhost:9090/metrics,source=cachecannon,role=loadgen,protocol=prometheus`.
- rezolus scrapes `/metrics` from a source that serves neither the stream nor
  a Rezolus agent's `/status` (rezolus entry, "Endpoint detection").

Either way cachecannon's `/metrics` renders histograms as summaries
(`write_histogram_summary`, `src/admin/mod.rs`), so latency arrives as
quantile gauges, which the viewer cannot read as distributions, until step 3.

## Plan

1. **Guide.** Change the guide's example to the Prometheus form above, or keep
   `-o run.rez`, and say why. Remove the workaround when rezolus's detection
   fix ships.
2. **metriken 0.9 → 0.11**, `metriken-exposition` 0.16 → 0.21 (then the
   release carrying the stream route),
   `metriken-query` 0.9 → 0.34. `metriken-core` declares `links`, so a build
   holds one metriken-core version: metriken 0.9 is on core 0.2, and
   metriken-archive 0.3 needs metriken 0.11, on core 0.3.
3. **Serve `/metrics/stream`** through `metriken-exposition`'s stream route
   for a registry (metriken entry, piece 1). Once rezolus detects a source by
   its stream (rezolus entry, "Endpoint detection"), `rezolus record` records
   cachecannon as it records an agent: rows stamped by cachecannon at read
   time, native histograms, into the same `.dendro` as the hosts.
   `/metrics/binary` stays for `.rez` output and older recorders.
4. **Record in process with `metriken-recorder`**, recording cachecannon's own
   registry, for runs without a rezolus recorder (metriken entry, piece 2).
   This is #176's route 1 with a published crate in place of rezolus's
   unpublished `rez`. It addresses three of the problems
   #176 lists: the archive is readable while the run is going, it is complete
   after a crash, and the snapshot timer can be fixed-rate. Acquisition windows
   also need cachecannon's metrics to report one (metriken entry, piece 1).
   `--parquet` stays as an option for one release.
5. **`cachecannon view` reads `.dendro`**: open archives through
   `metriken-archive` 0.3's `ArchiveReader` (a `metriken_query::MetricsSource`),
   and later through `metriken-query` over `metriken-storage`, beside parquet.
   Later, `cachecannon view` mounts `metriken-viewer` in place of its own
   dashboards (metriken entry, piece 5), after rezolus 6.0.0.
6. **The template moves here.** cachecannon's dashboard template
   (`cachecannon.json`, today in rezolus's `crates/dashboard/templates/`) is
   owned by cachecannon and written into the archive: in the stream's
   handshake (step 3) and in its own source's metadata (step 4). The viewer
   loads it from the archive (metriken entry, piece 3).

Steps 3 to 6 need step 2 (metriken-query 0.34's default features pull in
metriken 0.11). Step 3 also waits for the metriken entry's path step 3 (the
stream route), step 4 for path step 4 (`metriken-recorder`), and step 6 for
path step 5 (templates); step 5's `.dendro` reading needs only this entry's
step 2. Step 3 comes first among them: it is the piece every producer shares,
and it puts cachecannon in the same archive as the hosts.

## As built

Steps 2 and 3, 2026-10-11, on `feat/metriken-stream` off upstream/main
809533a. metriken's side is its `docs/journal/2026-10-11-stream-route.md`
(released as metriken-exposition 0.22.0, metriken-storage 0.2.0,
metriken-query 0.36.0, dendro 0.4.0).

**Step 2 needed ringline on metriken 0.11.** metriken-core declares `links`,
so no graph holds metriken 0.9 (core 0.2) and 0.11 (core 0.3), and ringline
1be7049, the rev upstream/main pinned, depends on `metriken = "0.9"`. The
branch first built against 1be7049 plus that one-line bump through a local
`path` patch. ringline-rs/ringline#649 (dfff2e5, 2026-10-11) moved ringline
main to metriken 0.11.3, and `[patch.crates-io]` now pins
`git = "https://github.com/ringline-rs/ringline", rev = "dfff2e5"` for
ringline, ringline-redis, ringline-memcache and ringline-ping. Their versions
at dfff2e5 (0.7.0-alpha.0, 0.7.0, 0.7.2, 0.6.0) are the same as at 1be7049
and match cachecannon's requirements unchanged.

**Step 2, ringline 1be7049 → dfff2e5.** 76 ringline commits; the breaking
ones that reach cachecannon:

- #533, `connect` returns a builder resolving to `Connection`.
  `establish_connection` builds `connect(endpoint)` with `.tls(name)` when TLS
  is on and keeps `try_timeout` around the whole future, because the
  builder's `timeout` covers the TCP connect only and the connect future
  resolves after the TLS handshake. The client crates take the `Connection`
  and their `build` no longer returns `Result`, so the five "client setup
  failed" arms are gone; each task keeps `Connection::as_conn()` for close
  and logging. A connect that cannot be submitted now fails on first poll and
  is counted as a failed connect (`disconnects_connect_failed` too), where it
  was a separate warning before.
- #539, `ShutdownHandle` → `Runtime`: cachecannon never names the type.
- #557, `JoinHandle` resolves to `Result<T, JoinError>`: one site,
  `valkey-lab search`'s query fan-out. The default `TaskPanicPolicy` is
  `Contain`, as before.
- #593, quic `now` arguments: cachecannon does not use ringline-quic.
- #647, one-shot TCP receives by default on io_uring.
  `ConfigBuilder::recv_multishot(true)` in `runner.rs` and `search/mod.rs`
  keeps the multishot receives earlier builds measured with. ringline's
  measurement for the default is a RESP server with 2048 connections and
  64 KiB values, where multishot let some connections hold the ring and 4.2%
  of requests timed out; it gives no guidance for clients. Whether one-shot
  serves cachecannon better at high connection counts with large values is
  not measured here. mio (macOS) ignores the setting, and with
  `[timestamps] mode = "software"` (Linux) every receive is multishot
  `recvmsg` either way. Other ringline changes in this range change the
  default io_uring path, including #637's merged copy sends and #639 and
  #640's receive throttle (ringline's CHANGELOG has the rest); no io_uring
  run has compared results before and after.

**Step 2, versions.** metriken 0.11.3, metriken-exposition 0.22
(`msgpack`, `stream`, no defaults), metriken-storage 0.2 (`convert` only),
metriken-query 0.36 (`lz4` only), dendro 0.4 as a dev-dependency with no
features. `cargo tree -e normal -i rusqlite` finds nothing: the build compiles
no SQLite until step 5 turns on metriken-query's `archive`.

**Step 2, API changes taken.**

- `MsgpackToParquet` and `ParquetOptions` are imported from
  `metriken_storage::convert`.
- The snapshot `Counter`, `Gauge` and `Histogram` are `#[non_exhaustive]`
  (they gained `window`), so `create_snapshot` builds them with `new`.
- metriken-query 0.36 has no `Tsdb`, no public `QueryEngine` and no `http`
  feature. The viewer opens files with `ParquetReader::open` and serves the
  five `/api/v1` routes from `src/viewer/api.rs`, with 0.9's parameters,
  bodies and error types; `labels` and `label/{name}/values` keep 0.9's fixed
  lists, which the frontend does not call.

**Step 2, viewer comparison.** Two 6 s ping runs at 2000 req/s, one written
by the 0.9-era release binary and one by this branch, were each opened by
both viewers (`/usr/bin/open` denied with `sandbox-exec`). For both files the
five dashboard sections and the error body of an unknown metric are
byte-identical between viewers. Query bodies differ where the engine changed
between 0.9 and 0.36: `sum(irate(responses_received[5s]))` reads 1997–2001
where 0.9 read 2003–2007, at the recording's own timestamps rather than a
one-second grid, and 0.9's trailing point (992, a part-filled step past the
last sample) is gone; `histogram_quantile(0.5, response_latency)` gives the
same six values at the sample timestamps; `metadata` gives exact first and
last sample times where 0.9 gave whole seconds; matrix results carry
`intervals` and `bands`, which `script.js` does not read.

**Step 3.** `src/admin/stream.rs`. One `RegistryPasses` over
`GroupBuilder::new(DefaultRouter::new("cachecannon"))` per admin server,
floor 10 ms, shared by every subscription. The admin server's hand-written
HTTP/1 loop routes `GET /metrics/stream` before the whole-body paths:
`StreamRequest::parse` on the query string (a `Refusal` is answered with its
status and message; an interval below the 10 ms floor gets a 400 before any
stream bytes), `StreamSubscription::new` with labels
`source=cachecannon` and metadata `version=<CARGO_PKG_VERSION>`, a status
line with the subscription's headers and `Connection: close` and no
`Content-Length`, the opening, then `next_deadline` / `pass` / `frame_at`
until a write fails. `request_target` now returns the query string beside the
path. `into_stream` (feature `stream-tokio`) was not used: the loop is eight
lines, and it would add futures-util for one consumer.

Every metric, ringline's included, lands in `cachecannon/main`, windowless,
sent wide (the group mixes counters, gauges, histograms and counter groups,
so the long layout sends it wide too). Plain counters still carry no
acquisition window; whether a load generator needs one is not measured (the
metriken entry's "Left alone").

**Step 3, checks.**

- `metrics_stream_serves_handshake_and_rows` (`src/admin/mod.rs`) starts the
  admin server on an ephemeral port, sends
  `GET /metrics/stream?interval=100ms`, checks the content type, the three
  `x-metriken-*` headers and the absence of `Content-Length`, then decodes with
  dendro's `FrameReader`: a handshake with `source=cachecannon` and the
  version, then three rows frames, each with a `cachecannon/main` row, the
  first naming `response_latency` in its schema, `seq` increasing. Setting the
  source label to another value fails it, and so does dropping every pass.
  `metrics_stream_refuses_a_zero_interval` and
  `metrics_stream_refuses_an_interval_below_the_floor` check the 400s, and
  `metrics_stream_accepts_an_interval_at_the_floor` that 10 ms is served.
- End to end on macOS: cachecannon `ping` at 2000 req/s against a local
  PING/PONG server, recorded by rezolus `feat/stream-route-adopt` (bf365042,
  debug) with `rezolus record --url http://127.0.0.1:19090 -o x.dendro
  --duration 5s`: `cachecannon/main 7 rows ~1.000s`;
  `sum(rate(responses_received[2s]))` read min 2000, mean 2161, max 2805;
  `histogram_quantile(0.5, response_latency)` averaged 110975 ns, matching the
  ~111–157 µs p50 cachecannon printed. rezolus probes `/metrics/binary`, takes
  the benchmark for a Rezolus agent, and with `--url` names the recording
  `rezolus` with `source=rezolus`, not the handshake's `cachecannon`; with
  `--endpoint http://127.0.0.1:19090,source=cachecannon,role=loadgen` it is
  `recording cachecannon [source=cachecannon]`.
- `cargo fmt --all -- --check`, `cargo clippy --all-targets -- -D warnings`,
  `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps`, `cargo test --all` and
  `cargo test --all --release` pass on macOS (164 lib tests, 1 ignored).
  Linux CI has not run.
- Re-run on ringline dfff2e5 (macOS): the same gates, plus
  `cargo clippy --features search --all-targets -- -D warnings`, pass (162
  lib tests, 1 ignored), and so does `metrics_stream_serves_handshake_and_rows`.
  End to end with the same ping config and the same rezolus debug binary,
  `rezolus record --endpoint http://127.0.0.1:19090,source=cachecannon,role=loadgen
  -o x.dendro --duration 6s` wrote `recording cachecannon [source=cachecannon]`,
  `cachecannon/main 8 rows ~1.000s`; `sum(rate(responses_received[2s]))`
  read min 1999.6, mean 2000.2, max 2000.8 over 6 points, and
  `histogram_quantile(0.5, response_latency)` averaged 123220 ns against a
  123 µs p50 printed by cachecannon. This exercises mio only: the io_uring
  code, including `recv_multishot(true)` and the `timestamps` builders, is
  compiled and tested by Linux CI alone.

Step 1 (the guide) is not done: the guide still shows `-o run.rez` and now
says a `.dendro` needs a recorder that reads the metriken content type, which
no rezolus release does as of 2026-10-11.

## GO criteria

- A `rezolus record` run with an agent and cachecannon as endpoints, and no
  `-o`, writes one `.dendro` (`rezolus.dendro`) in which cachecannon's latency
  is a histogram, and `rezolus view rezolus.dendro` shows the cachecannon
  dashboard for the cachecannon recording, with no cachecannon template in the
  viewer.
- `cachecannon --dendro run.dendro` (or the chosen flag) leaves a readable
  archive after `kill -9` partway through a run.
- `cachecannon view run.dendro` shows the same dashboards as for the
  equivalent parquet.
