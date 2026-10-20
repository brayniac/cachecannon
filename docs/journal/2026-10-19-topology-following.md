# Following cluster topology changes mid-run

- **Opened:** 2026-10-19
- **Status:** OPEN — step 2 (routed open loop) built; steps 3-6 not started.
  Records how cachecannon fixes its node list at startup, what that gets wrong
  in both cluster modes, a proposed design for adding and removing nodes and
  their connections during a run, and the key-selection modes that design
  forces a decision on. See "Step 2, as built" for where the build differs
  from the design.
- **PRs:** #239 (startup-only ElastiCache auto discovery, merged as `f37e532`),
  #240 (guide correction, step 1), #241 (this entry); step 2 in review

Code references are pinned to `f37e532`.

## Problem

cachecannon has two modes in which the server tells it which nodes exist:

- Valkey cluster mode (`target.cluster = true`) reads `CLUSTER SLOTS` from a
  seed and builds a 16384-entry slot table (`src/cluster.rs`).
- ElastiCache for Memcached auto discovery (`target.auto_discovery = true`,
  #239) reads `config get cluster` from the configuration endpoint
  (`src/autodiscovery.rs`).

Both run once, in `resolve_endpoints` (`src/runner.rs:1192`), before workers
start. The node list they produce is fixed for the rest of the run. A real
client of either system rebuilds its node list during operation:

- AWS documents that auto discovery clients poll the configuration endpoint,
  once a minute in its own clients, and rebuild their node list when the config
  version changes. The version increments by one on every node addition or
  removal. A failed node that is being replaced stays in the list for several
  minutes, so clients must retry connections to it.
- A Valkey cluster client updates its slot map on a `MOVED` reply, reconnects
  to nodes it has not seen, and re-reads the topology when a slot moves to an
  unknown node.

A benchmark that cannot follow these changes measures the wrong thing during a
scale-out, a scale-in, a failover or a reshard. Those events are among the main
reasons to run a cache benchmark against a managed cluster.

## What the code does today

### The node list is positional and fixed

`TaskSharedState` (`src/worker.rs:656`) is built once per worker. It holds
`endpoints: Vec<SocketAddr>`, the ketama `ring` built from those addresses, and
`slot_table: RwLock<Option<Vec<u16>>>`. Everything else refers to a node by its
index into `endpoints`:

- `connection_endpoints` (`src/worker.rs:1071`) assigns each connection an
  endpoint index at startup, round-robin or by `key_share`.
- Each connection task reads `state.task_state.endpoints[endpoint_idx]` once
  (`src/worker.rs:1330` for RESP, at 2275, 2841, 3246 and 3462 for the
  other task kinds, and `src/dma/connection.rs:153`) and reconnects to that
  address for the life of the run.
- `prefill_queues` and `append_queues` are one queue per endpoint index, shared
  across workers. The runner thread routes each append batch into
  `append_queues` with the startup endpoints and slot table
  (`enqueue_append_batch`, `src/runner.rs:819-826`), and append writers are
  assigned to nodes round-robin at startup (`src/worker.rs:3124`).
- `endpoint_keys` (`src/worker.rs:682`) is the keyspace partitioned by endpoint
  index, and `EndpointZipf` (`src/keydist.rs:312`) is built on it. Both are
  immutable `Arc`s.

Adding a node means appending to every one of these and spawning connections.
Removing a node means retiring an index without shifting the others, because
in-flight requests and queued keys refer to it by position.

### MOVED handling does not add nodes

`check_resp_redirect_parsed` (`src/worker.rs:2164`) handles a `MOVED` reply.
If the target address is already in `endpoints`, it rewrites that slot in the
worker's own slot table. If it is not, it logs `MOVED redirect to unknown
endpoint (cannot add dynamically)` and does nothing else. Three more limits
apply:

- The slot table is per worker, so each worker learns a moved slot separately,
  one `MOVED` reply at a time.
- A main-workload request is not retried. `check_resp_redirect_parsed` counts
  the redirect in `cluster_redirects`, and `record_counters`
  (`src/worker.rs:3944`) counts the reply as a response and in the GET, SET or
  DELETE count, not in `REQUEST_ERRORS`. ringline-redis passes error replies to
  the `on_result` callback, so the reply's latency is recorded in the GET, SET
  or DELETE histogram, `RESPONSE_LATENCY` and `PERCEIVED_LATENCY` (statics in
  `src/metrics.rs`). Prefill and append SETs that get `MOVED` are queued again
  and re-routed when popped (`src/worker.rs:1564-1569`, `3376-3381`), so they
  reach the new owner if it is a known node. For an unknown node the slot
  table is unchanged and the key returns to the old node indefinitely: the
  runner's stall check or `prefill_timeout` (`src/runner.rs:645-660`) fails
  the run with "prefill failed: timed out or stalled" (`src/runner.rs:980`),
  and an append batch never commits.
- The uniform and zipf key-selection paths (`src/worker.rs:1715-1729`) pick a
  key from the connection's own endpoint bucket and send it without routing it.
  After a slot moves, those paths keep sending its keys to the old node, which
  answers `MOVED` every time. Only the rejection-sampling path for other
  distributions calls `route_key`.

`docs/guide.md:721-726` says an unknown endpoint gets a new connection and the
request is retried on the correct node. Neither is true. #240 corrects that
section, independently of this design.

### Other startup-only assumptions

- `cluster.rs` connects with a plain `TcpStream` (`src/cluster.rs:46`), so
  cluster discovery does not work against a TLS-only cluster. It parses node
  addresses as `SocketAddr`, so a cluster that announces hostnames fails.
- `standalone_task_capacity` and `timer_slots` (`src/runner.rs:1417`,
  `src/runner.rs:1441`) size ringline's per-worker pools from the startup
  connection count, plus a headroom of 16 (`POOL_HEADROOM`), with a floor of
  256. Connections added mid-run can exhaust them. When the task slab is full,
  `ringline::spawn` returns `Err`; startup logs it and counts the connection in
  `CONNECTIONS_FAILED` (`src/worker.rs:1033-1063`), and a mid-run spawn needs
  the same handling. When the timer pool is exhausted, `ringline::sleep` and
  `ringline::timeout` panic, while the request deadline (`InFlight::arm`) and
  the connect bound use `try_sleep`/`try_timeout` and get
  `Err(TimerExhausted)`. The line numbers here are at f37e532. Since
  cachecannon moved to ringline ab81ceb, the timer pool is ringline's fixed
  65,536 slots per worker (`ringline::TIMER_SLOTS`) and cachecannon no longer
  sizes it; `timer_slots` is gone.
- `check_node_counts` (`src/runner.rs:1220`) checks prefill and append writer
  counts against the startup node count only.

ringline already has what a mid-run change needs: `ringline::spawn` works from
inside a task, and `WakeHandle` with the `on_notify` handler callback signals a
worker from another thread (ringline `dfff2e5`, `ringline/src/wakeup.rs:75`,
`ringline/src/runtime/handler.rs:109`).

## Key selection is the larger change

Following topology requires routing each request at the time it is sent, since
the owner of a key can change. That is not how keys are chosen today, and the
difference changes what a run measures.

### Today: each connection draws keys for its own node

A connection is bound to one node and draws only keys that node owns. For
uniform keys it indexes that node's `endpoint_keys` bucket; for zipf it samples
`EndpointZipf`, the zipf distribution restricted to that node's keys. The comment
at `src/worker.rs:1696` records why: drawing a global key and rejecting it until
one routed to the connection's node made CRC16 slot routing about 40% of CPU at
high pipeline depth.

Under `connection.endpoint_allocation = "even"` (the default), each node gets
the same number of connections, so in a closed-loop run each node receives
about the same request rate regardless of how popular its keys are. With zipf
keys this removes the effect that matters most on a real cluster: the node that
owns the hottest keys receives more traffic than the others.

How large that effect is, for θ = 0.99 over 1M keys (computed by summing the
zipf probabilities directly, not measured):

- key 0 carries 6.5% of requests, and the top 100 keys carry 34.4%.
- With keys placed on 8 nodes uniformly at random, as a model of CRC16 slots
  or ketama, the hottest node carries 15.5% to 24.0% of requests over 20
  placements (median 17.4%), against 12.5% for an even split.

`endpoint_allocation = "key_share"` gives each node connections in proportion
to its key share. It is quantized to whole connections and config validation
only accepts it with zipf. It reproduces the per-node load correctly only when
the run is rate-limited; in closed loop, more connections to a node is not the
same as more requests waiting on it.

### Proposed: separate load models

The load models answer different questions, so they are separate settings.
Their names are placeholders; "cluster" is already taken by `target.cluster`.

**Per-node** loads each node as if it were a single-node benchmark. Each
connection is bound to a node and draws only that node's keys, as every
multi-endpoint run does today. Open and closed loop behave as they do against
one server, applied to each node separately. The result is the sum of per-node
capacities: a number for sizing nodes, not the throughput a client would see.

**Routed** loads the cluster the way a client population does. A request draws
its key from the global distribution, routes it with the current topology, and
is sent on a connection to the owning node. One route per request costs one
CRC16 or one ketama lookup. The 40% figure above came from about one route per
node per request under rejection sampling, so it does not apply, but the single
route has not been measured.

In the routed model, open and closed loop have to be implemented differently:

- **Open loop.** The dispatcher funds a request, draws its key and routes it,
  and the node receives `share × rate`. A hot node receives more requests than
  it can serve; its latency rises until its queue is full, and further
  requests are dropped and counted. The offered load does not depend on how
  fast any node answers.
- **Closed loop.** A fixed number of outstanding requests, each of which draws
  a key, routes, waits for the reply, and draws again. Requests accumulate on
  the slowest node, so the hot node bounds total throughput. This also holds
  for uniform keys when one node is degraded. It is how an application with a
  fixed thread pool behaves against a cluster.

Both routed regimes change how connections are used. Today a connection's
pipeline is filled by the task that owns it. In the routed model a request is
not tied to a connection until it is routed, so both regimes need one queue
per node, with each connection task draining its node's queue.

In open loop the dispatcher changes shape. Today `TokenDispatcher` is
pull-based: a connection queues a claim with `acquire`, and the dispatcher
funds claims in order from the worker's limiter (`src/worker.rs:430-481`). In
the routed model, each time the limiter allows a request, the dispatcher draws
a key, routes it and pushes it onto the owning node's queue. Three
requirements follow:

- The queues are per worker, because a queue shared across workers would put a
  cross-thread handoff on the hot path. Every worker therefore needs at least
  one connection to every node (connections ≥ threads × nodes).
  `check_node_counts` enforces that for the routed model after discovery, and
  reconciliation re-checks it when a node is added. Today a worker with fewer
  connections than nodes has no connection to some of them, because
  connections are assigned by global index (`src/worker.rs:1004-1029`).
- A request's latency starts when the dispatcher funds it, not when it is
  sent. ringline-redis measures `latency_ns` from send to parsed reply, and a
  push onto a queue never makes the limiter slip, so time spent waiting in a
  hot node's queue would otherwise reach neither the latency histograms nor
  `PERCEIVED_LATENCY`. Saturation search reads those histograms
  (`src/saturation.rs:318-339`) and would not see the hot node break the SLO.
  Today the GET, SET, DELETE, `RESPONSE_LATENCY` and `PERCEIVED_LATENCY`
  histograms for RESP and memcache are written in the `on_result` callbacks
  (`src/worker.rs:1259-1309`), and `CommandResult` in ringline-redis and
  ringline-memcache has no `user_data`, so the callback cannot find a
  request's funding time. Latency recording for RESP and memcache moves into
  the recv loop, which has `user_data`.
- The per-node queue is bounded. A request that finds it full, or whose node
  has no live connection, is dropped and counted in a new `routed_queue_dropped`
  counter. Saturation search counts responses, not requests sent, so when
  drops push the achieved rate below `min_throughput_ratio` of the target
  (`src/saturation.rs:389-394`), the step fails the same way a throughput
  shortfall does.

Closed loop adds one cap on top of the same queues: a per-worker limit on
outstanding requests, sized `connections × pipeline_depth`. Closed loop has no
dispatcher, so its latency starts when the key is drawn. memtier_benchmark
uses a similar cap, per cluster client (`hold_pipeline()` in
`cluster_client.cpp`). The per-node handoff is the part of the design with the
most risk to the hot path, and it should be built and measured before the
topology work depends on it.

**Decision (2026-10-19): routed closed loop is a real mode, built after routed
open loop.** Reasons for building it:

- memtier_benchmark's cluster mode and YCSB both report routed closed-loop
  throughput. Comparing cachecannon against either needs the same model.
- An application with a fixed connection or thread pool behaves this way: a
  slow or hot node accumulates requests while the other nodes idle. Open loop
  does not show this, because it keeps sending to healthy nodes at their share
  of the rate.

Reasons for building it second:

- For cluster capacity, routed open loop with `saturation_search` is the better
  measurement. It raises the rate until the hot node breaks the SLO under the
  real key skew. Its latency includes queueing at the hot node, provided
  latency is measured from the time the dispatcher funds the request (above).
- A routed closed-loop throughput number mostly measures the hot node, and it
  depends on the outstanding-request limit the user picks: throughput is that
  limit divided by latency, and at high limits most of it waits on the hot node.
- The per-node queues are the expensive part and open loop needs them anyway.
  Closed loop is then one added cap.

Until routed closed loop exists, config validation rejects the routed model
unless `workload.rate_limit > 0` or `[workload.saturation_search]` is set, the
same condition that creates the ratelimiter (`src/runner.rs:125`), so a
closed-loop run cannot fall back to per-node behaviour without saying so.

**Mirrored** (a third model, raised after the first two) sends every node the
whole keyspace with the same distribution: N copies of a single-node
benchmark, run in parallel. It differs from per-node in two ways:

- It only works where a node accepts any key. Memcached nodes, including
  ElastiCache nodes found by auto discovery, and standalone Valkey servers do.
  A Valkey cluster node answers `MOVED` for every key outside its slots, so
  config validation would reject mirrored with `target.cluster`.
- Each node holds the whole keyspace, not its 1/N share. Prefill writes every
  key to every node, and a node's working set is N times what it would be in
  the routed or per-node models. Hit rate and memory results are for a node
  holding the full keyspace.

A topology change in mirrored needs no key re-partitioning: a new node gets
the same workload and is prefilled, and a removed node's load stops. Prefill
today is a phase that runs before the measured run, so a node added mid-run
needs a decision: hold its steady-state load until its own prefill completes,
or start it cold and report its hit rate separately.

Per-node stays the default. Existing multi-endpoint results keep their meaning,
and choosing routed is a deliberate change of question, so it does not need a
CHANGELOG warning about changed numbers.

`endpoint_allocation = "key_share"` exists to make per-node open loop give each
node its key share of the rate. Routed open loop gives that load exactly,
without quantizing to whole connections, so `key_share` can be deprecated once
routed open loop exists.

### Topology changes in each model

- **Routed:** a change replaces the ring or slot table. New requests route to
  the new owner, connections are added for new nodes, and a removed node's
  connections drain.
- **Per-node:** a change adds or removes a node's load. When slots move between
  nodes that both remain, each node's key set changes, so `endpoint_keys` and
  `EndpointZipf` have to be rebuilt for the new partition. Per-node open loop
  also needs a rule for the rate when the node count changes: hold the
  per-node rate, so the total grows, or hold the total, so each node's share
  shrinks.
- **Mirrored:** a change adds or removes one copy of the workload, with the
  prefill decision above.

### Other load generators

Read from source on 2026-10-19: memtier_benchmark `58487baf39`, valkey
`319db007f3` (`src/valkey-benchmark.c`), mutilate `d65c6ef7c2`. YCSB is from
memory and was not checked. None offers more than one of these models.

- memtier_benchmark (`--cluster-mode`, `cluster_client.cpp`) is routed. Each
  client draws keys from its own generator (`client.cpp`) and queues each key
  on its connection to the owning shard (`m_key_index_pools`). Each client caps
  its outstanding requests at `pipeline × its connection count`
  (`hold_pipeline()`). A `MOVED` reply triggers a `CLUSTER SLOTS` re-read.
  `handle_cluster_slots()` builds the new map before swapping it in, keeps
  connections whose address is unchanged, opens connections to new nodes, and
  retires the rest. A redirected request is retried only with
  `--retry-on-error`. `--rate-limiting` applies per connection, so in cluster
  mode every node gets the same rate rather than its key share.
- valkey-benchmark and redis-benchmark (`--cluster`, `valkey-benchmark.c`) are
  per-node. Each client connects to one node, and `setClusterKeyHashTag()`
  rewrites a `{tag}` in the key to hash to a slot that node owns. `MOVED` or
  `ASK` triggers a `CLUSTER SLOTS` re-read that updates slot ranges of known
  nodes only; an unknown node fails with "could not find node with ID ... in
  current configuration" and the benchmark exits. A redirected request is
  counted as finished.
- mutilate (`mutilate.cc`, `Connection.cc`) is mirrored with a smaller
  keyspace: `--records` is divided by the server count, and every connection
  to every server draws from `lrand48() % options.records`. Each server gets
  the same keys and holds 1/N of the requested records, which keeps its working
  set at the size it would have in a sharded deployment. Mirrored here could
  offer the same option.
- YCSB routes through its client library (JedisCluster, spymemcached) and runs
  closed-loop threads, optionally throttled with `-target`. It follows topology
  changes as far as the client library does.

## Proposed design for topology following

### Shared topology with stable node ids

Replace positional `endpoints` with a `Topology` value: a generation number, a
map from node id to address, and the ketama ring or slot table. The ring is
hashed from node addresses as today, so key placement matches a ketama client,
with each point mapping to a node id. A
node id is assigned once and never reused within a run, so a queued key or an
in-flight request that refers to a removed node can be detected.

The current `Topology` is held in one shared `Arc`, swapped on change. Each
worker keeps a local `Arc` clone and its generation, and compares generations
before routing. That replaces today's `RwLock` read in `route_key`
(`src/worker.rs:3976`) with an atomic load per request.

### A watcher thread per run

One thread outside the ringline workers owns discovery:

- Auto discovery polls the configuration endpoint at
  `target.discovery_interval` (default 60 s, the interval AWS uses) and
  publishes a new `Topology` when the config version changes. It reuses
  `discover` (`src/autodiscovery.rs:47`) and its per-seed deadline.
- Cluster mode re-reads `CLUSTER SLOTS` on a timer and when a worker reports a
  `MOVED` to an unknown node or a slot table that disagrees with a reply. A
  `MOVED` to a known node is applied to the shared table, so each worker no
  longer learns it separately.

After publishing, the watcher calls each worker's `WakeHandle`. The worker's
`on_notify` reconciles its connections against the new `Topology`. On mio,
`on_notify` runs on every loop iteration, not only after a wake, so it
compares the `Topology` generation first.

The runner's append scheduler also routes with the current `Topology`, not the
startup slot table it uses today (`src/runner.rs:823`).

### Reconciling connections

- **Added node:** the worker spawns connections to it. The proposal is a fixed
  connection count per node, set at startup as `connections / initial nodes`,
  so total connections grow with the cluster. That matches a client with one
  pool per node. The alternative is to hold the total fixed and spread it more
  thinly. The new node's connections are spread round-robin across workers, so
  every worker gets one when the count is at least `threads`. In the routed
  model it always is, because the startup check implies
  `connections / initial nodes ≥ threads`. The per-worker limiter weights,
  fixed at startup today (`src/limiters.rs:280-333`), are rebuilt from the new
  per-worker connection counts. An append writer is spawned for the node
  before any append key is routed to it, because a node with no writer never
  drains its append queue and the batch never commits.
- **Removed node:** its connection tasks stop taking new work and exit after
  in-flight requests complete or time out. Queued prefill and append keys for
  the node are re-routed. `requeue_drained_prefill` (`src/worker.rs:3986`)
  already re-routes keys on disconnect and can be generalised.
- **Listed but unreachable node:** connections keep retrying with the existing
  backoff, since a replaced ElastiCache node stays listed for minutes. Requests
  routed to it are dropped and counted in `routed_queue_dropped`, and that is
  the correct result during a replacement.
- **Pools:** `standalone_task_capacity` needs a ceiling that covers growth.
  The simplest option is a `target.max_nodes` setting that sizes the pools for
  `max_nodes × per-node connections`, with a warning when a topology change
  would exceed it.

### Redirects in cluster mode

With a shared slot table and global draws, a `MOVED` reply should re-route and
resend the request once. The redirect reply should then be counted only in
`cluster_redirects`, and the latency recorded should run from the first send
to the final reply. An `ASK` reply needs `ASKING` sent on the target connection
first. Both change what the response count and the latency histograms contain
during a reshard, so they belong in the same CHANGELOG entry. `CommandResult`
carries `success` but not the error text, so `on_result` cannot tell a
redirect from another error; step 2 has already moved latency recording into
the recv loop, which has `RespMeta.error`.

### Metrics

Add counters for topology generations, nodes added and nodes removed, and keep
`cluster_redirects`. Per-node request and latency metrics are needed to see a
hot node at all. Whether they belong in the parquet snapshot depends on how
many nodes a run can have, and that has not been decided.

## Order of work

1. Correct `docs/guide.md:721-726` (#240). Docs only, independent of
   everything else.
2. Routed open loop with the per-node queues, on a fixed node list. Config
   validation rejects routed unless a ratelimiter is created;
   `check_node_counts` requires connections ≥ threads × nodes. Latency
   recording for RESP and memcache moves from the `on_result` callback into the
   recv loop, which has `user_data` to look up the funding time. Step 6 reuses
   this for redirects. Measure client CPU per request against the per-node path
   at the highest pipeline depth and connection count in CI, on io_uring.
3. Routed closed loop: the outstanding-request cap over the same queues, and
   removal of the ratelimiter rule from step 2; the threads × nodes
   requirement stays.
4. The `Topology` type with stable ids, replacing positional `endpoints`.
5. The watcher and worker reconciliation for auto discovery. A fake
   configuration endpoint whose version changes mid-run can test this; the
   test servers in `src/autodiscovery.rs` already serve the protocol.
6. The same for cluster mode, with `MOVED`/`ASK` retry and TLS and hostname
   support in `cluster.rs`. This can be tested against a local Valkey cluster
   resharded with `valkey-cli --cluster reshard` during a run.

## Step 2, as built

Built on branch `routed-open-loop` as `[workload] routing = "pinned" |
"routed"`, default `pinned`. Where it differs from the design above:

- The drop counter is `routed_queue_dropped`. The design called it
  `requests_dropped`, which already names the rate limiter's shed count
  (`src/metrics.rs`).
- The queue wait reaches the recv loop through `InFlight`, which keeps a
  queue wait per request parallel to its fire times, not through
  `user_data`. Replies on a connection are FIFO, so the front entry belongs
  to the reply just received.
- A node whose connections on a worker are all reconnecting is not dropped
  at once: its requests queue until the queue fills, and the reconnect time
  counts as latency.
- Each node queue holds 16 × (this worker's connections to the node ×
  `pipeline_depth`) requests. The multiple is a guess; nothing has measured
  how often it fills on a healthy node.

What review found that the design did not anticipate:

- A connection that is waiting for a reply cannot send. The ringline-redis
  and ringline-memcache clients take `&mut self` for both `fire_*` and
  `recv`, and `recv_meta` pops the pending operation before awaiting
  (ringline `dfff2e5`, `ringline-redis/src/lib.rs:1731`), so dropping a
  `recv` to send something else loses the reply's bookkeeping. A request
  queued while every connection to its node waits on a reply therefore waits
  for a reply even with free pipeline slots. Measured by the reviewer on mio
  against a fixed 2 ms server, one connection, pipeline 32, 2000 req/s:
  pinned response p50 2.70 ms; routed p50 3.75 ms, p99 5.24 ms, with
  `routed_queue_wait` p50 1.20 ms. Eight connections with pipeline 1 brought
  the wait to 15 µs. Pinned connections have the same constraint, but a
  pinned request's clock starts at send, so the wait is not in the response
  latency; it reaches `schedule_slip` and `perceived_latency` only when the
  limiter holds more than one claim. Pipelining shortens the wait while the
  connections are short of capacity for the rate (one connection at 2000
  req/s: `routed_queue_wait` p50 39.8 ms at depth 1 and 4, 1.16 ms at depth
  32), but not below about one reply time.
- The first waiter list was LIFO. With 8 connections, pipeline 1 and 500
  req/s, two connections carried every request (`[0, 0, 0, 754, 754, 0, 0,
  0]` GETs per server-side connection, against 192-195 each pinned). It is
  now FIFO with one entry per task.

Not measured: client CPU per request against pinned on io_uring, which needs
a Linux host; and the cost of `RoutedQueues::wait`, which scans the node's
waiter list on every poll (`will_wake` for the one-entry rule, `retain` on
return). With hundreds of idle connections to one node per worker that scan
is a per-request cost that grows with the connection count. If it measures
badly, a per-task registered flag makes both checks O(1).

## 2026-10-20: sending while a reply is pending

ringline #682 (merged as ab81ceb) added `Client::recv_ready` to
ringline-redis and ringline-memcache: it flushes, then returns a future that
resolves when the next reply starts arriving and borrows nothing from the
client. Using it needed the ringline pin moved from dfff2e5 to ab81ceb, which
brought three breaking changes and went up as its own change
(`timestamps.mode = "software"` rejected, `get_ttfb` from the arrival log,
timer slots fixed at 65,536 per worker).

In both routed recv loops (`drive_resp_workload`, `drive_memcache_workload`),
a connection with replies pending and room for a batch now waits on
`ringline::select(queues.wait(node), client.recv_ready())`, inside the
request-timeout bound (`routed_wait_send_or_recv`). A queued request wins
and is sent; a reply wins and is received. `select` polls the queue first,
so a push's wake is not spent on a wait that then returns for the reply.

The wait is dropped whenever the reply wins, so the waiter list could no
longer rely on entries being removed when a wait returns. Each wait now
registers an `Rc<WaitSlot>` with a `live` flag; dropping or completing the
wait clears it, `push` skips dead entries, and the list is compacted to its
live entries at `2 x connections + 16`. That also makes the re-poll check
O(1), which answers the scan cost recorded above. A unit test drops a
registered wait and checks the next push reaches the other connection; with
the `Drop` made a no-op it fails, as does the compaction test.

The batch gate is unchanged, and it decides everything. `batch_size`
defaults to `pipeline_depth`, so by default a connection with anything in
flight has no room for a batch and never reaches the new wait. Measured
(macOS, mio, one connection, pipeline 4, 200 req/s, a stub answering each
GET 12 ms after it arrived, so about 2.4 requests in flight):

| batch_size | `routed_queue_wait` p50 |
|---|---|
| 1, RESP | 21 µs |
| 1, memcache | 26 µs |
| 4 (default), RESP | 6.6 ms |
| 1, RESP, new wait disabled | 6.5 ms |
| 1, memcache, new wait disabled | 6.8 ms |

One run each. The first stub tried was the serial one the queue-wait tests
use (4 ms per GET, one at a time) at 200 req/s, and the default-batch
control read 18.8 µs where I predicted over 1 ms: the limiter spaces requests
5 ms apart, so each reply finished before the next request was funded and
the connection was rarely busy when one was queued. The stub that answers
from arrival keeps replies overlapping.

On io_uring (SystemsLab 01a15a2b, Linux 6.12.63, 16 vCPU guest, branch at
516dcb4), five repeats each: batch 1 read 5.8-6.4 µs (RESP) and 5.8-6.3 µs
(memcache); the default-batch control 6.06-6.29 ms; with the new wait
disabled the batch-1 tests failed at 6.16 ms and 5.96 ms. An earlier run
(01a15a1e, f140eb9) read the control at 650 ms on Linux. The stub wrote each
small reply with Nagle on while earlier replies were unacknowledged, and at
the default batch the client sends nothing that carries an ACK, so each
reply waited for a delayed ACK. 516dcb4 sets `TCP_NODELAY` on the stub, and
the control fell to the 6 ms above, which is consistent with that
explanation.

Against valkey 8.1.1 on the same guest (three instances on loopback, routed,
2 threads, 6 connections, pipeline 8, 50,000 req/s, 90% GET, 20 s after 5 s
warmup, three interleaved rounds; A is 707a044 without the new wait, B is
516dcb4; both release builds with thin LTO). Times in µs, one value per
round; CPU is user+sys seconds for the whole 25 s process:

| arm | queue wait p50 | queue wait p99 | latency p50 | latency p99 | CPU |
|---|---|---|---|---|---|
| B, batch 1 | 7.0, 7.1, 6.9 | 104, 122, 22 | 61, 62, 60 | 362, 397, 162 | 38.5, 38.2, 38.6 |
| A, batch 1 | 8.5, 8.3, 9.1 | 293, 266, 614 | 73, 72, 74 | 578, 578, 1253 | 37.6, 37.7, 38.6 |
| B, batch 8 | 8.6, 8.0, 7.9 | 295, 215, 201 | 73, 70, 70 | 602, 461, 477 | 37.0, 37.5, 37.6 |
| A, batch 8 | 8.1, 8.3, 8.0 | 276, 182, 204 | 71, 71, 70 | 541, 391, 457 | 37.6, 37.6, 37.5 |

Every arm delivered 50,000 req/s with no drops. At batch 8, A and B match
within the round-to-round spread, as the unchanged gate predicts. At batch 1,
B lowered response latency p50 by 10-14 µs and p99 by 31-87% against A, round
by round. The queue-wait p50 is small in every arm because valkey on loopback
answers in tens of microseconds; the effect is in the tail.

CPU does not resolve the cost of the new wait: the process used about 1.5
cores in every arm, because ringline workers spin while tasks are runnable
(see CLAUDE.md on per-worker CPU), and B at batch 1 read 38.4 s mean against
38.0 s for A, within a spread of 1 s. A cost measurement needs a metric
that is not pinned, such as achieved throughput at a fixed worker count
near saturation.

## Open questions for the owner

- Routed latency includes the wait above. Options: document it and size
  connections per node so the wait stays small (what step 2 does now), or
  change the ringline clients so a connection can wait on a reply and on its
  node queue at once (a `recv` that does not pop the pending operation until
  the reply is complete, or a split send and receive half). The second option
  is built, as `Client::recv_ready` rather than either mechanism named, for
  `batch_size` below `pipeline_depth` (2026-10-20 above). Open:
  whether routed should ignore the batch gate by default.

- When a node is added, should total connections grow with the node count or
  stay fixed? In the routed model a fixed total stops meeting threads × nodes
  once nodes are added, so the fixed-total option needs a rule for that case:
  refuse the change, or drop below one connection per worker per node and
  route across workers.
- In mirrored, does a node added mid-run wait for its own prefill before
  taking load, or start cold with its hit rate reported separately?
- Names for the load models (routed, per-node and mirrored are working names;
  "pinned" is a candidate for per-node), and whether the setting belongs under
  `[target]` or `[workload]`.
- In per-node open loop, should a node change hold the per-node rate or the
  total rate?
- Should a redirected request be retried, or should only the redirect be
  recorded? Retrying matches a real client; not retrying keeps every request a
  single round trip.

## Not yet known

- The per-request cost of draw-then-route against the per-node path.
- The per-request cost of the outstanding-request slots and per-node queues
  against the current per-connection pipeline.
- How ElastiCache behaves mid-change. AWS documents that a node is listed in
  `config get cluster` once it is available and removed from the list before
  it leaves the cluster; that has not been observed against a real cluster.
  Whether a replaced node keeps its IP is not documented. The IP field can be
  empty, so the watcher resolves the hostname, as `parse_node` does at
  startup.
