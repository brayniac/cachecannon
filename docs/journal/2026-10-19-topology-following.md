# Following cluster topology changes mid-run

- **Opened:** 2026-10-19
- **Status:** OPEN — design, pre-build. No code yet. Records how cachecannon
  fixes its node list at startup, what that gets wrong in both cluster modes,
  a proposed design for adding and removing nodes and their connections during
  a run, and the key-selection modes that design forces a decision on.
- **PRs:** #239 (startup-only ElastiCache auto discovery, merged as `f37e532`)

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
  (`src/worker.rs:1330` for RESP, and at 2275, 2841, 3246 and 3462 for the
  other task kinds) and reconnects to that address for the life of the run.
- `prefill_queues` and `append_queues` are one queue per endpoint index, shared
  across workers.
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
- The redirected request is not retried. `record_counters`
  (`src/worker.rs:3944`) counts the redirect reply as a response and in
  `cluster_redirects`, not in `REQUEST_ERRORS`. ringline-redis passes error
  replies to the `on_result` callback, so the reply's latency is recorded in
  the GET, SET or DELETE histogram and in `RESPONSE_LATENCY` (statics in `src/metrics.rs`).
- The uniform and zipf key-selection paths (`src/worker.rs:1715-1729`) pick a
  key from the connection's own endpoint bucket and send it without routing it.
  After a slot moves, those paths keep sending its keys to the old node, which
  answers `MOVED` every time. Only the rejection-sampling path for other
  distributions calls `route_key`.

`docs/guide.md:721-726` says an unknown endpoint gets a new connection and the
request is retried on the correct node. Neither is true. That section should
be corrected now, independently of this design.

### Other startup-only assumptions

- `cluster.rs` connects with a plain `TcpStream` (`src/cluster.rs:46`), so
  cluster discovery does not work against a TLS-only cluster. It parses node
  addresses as `SocketAddr`, so a cluster that announces hostnames fails.
- `standalone_task_capacity` and `timer_slots` (`src/runner.rs:1417`,
  `src/runner.rs:1441`) size ringline's per-worker pools from the startup
  connection count, plus a headroom of 16 (`POOL_HEADROOM`). Connections added
  mid-run would exhaust them. The task slab fails silently when full; the timer
  pool panics the worker.
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

- **Open loop.** The dispatcher funds a request, the request draws and routes,
  and the node receives `share × rate`. A hot node receives more requests than
  it can serve and its latency rises. The offered load does not depend on how
  fast any node answers.
- **Closed loop.** A fixed number of outstanding requests, each of which draws
  a key, routes, waits for the reply, and draws again. Requests accumulate on
  the slowest node, so the hot node bounds total throughput. This also holds
  for uniform keys when one node is degraded. It is how an application with a
  fixed thread pool behaves against a cluster.

Both routed regimes change how connections are used. Today a connection's
pipeline is filled by the task that owns it. In the routed model a request is
not tied to a connection until it is routed, so both regimes need one queue
per node, with each connection task draining its node's queue. In open loop
the dispatcher draws the key, routes it, and funds the owning node's queue.
Closed loop adds one cap on top of the same queues: a per-worker limit on
outstanding requests, sized `connections × pipeline_depth`.
memtier_benchmark's cluster mode uses that structure (`hold_pipeline()` in
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
  real key skew, and its latency does not carry closed loop's coordinated
  omission.
- A routed closed-loop throughput number mostly measures the hot node, and it
  depends on the outstanding-request limit the user picks: throughput is that
  limit divided by latency, and at high limits most of it waits on the hot node.
- The per-node queues are the expensive part and open loop needs them anyway.
  Closed loop is then one added cap.

Until routed closed loop exists, config validation rejects the routed model
without a rate limit or `saturation_search`, so a closed-loop run cannot fall
back to per-node behaviour without saying so.

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
the same workload and is prefilled, and a removed node's load stops.

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

### Other load generators

Read from source on 2026-10-19, except YCSB, which is from memory and was not
checked. None offers more than one of these models.

- memtier_benchmark (`--cluster-mode`, `cluster_client.cpp`) is routed. Keys
  come from one shared generator and are queued on the owning node's
  connection (`m_key_index_pools`). Closed loop caps all outstanding requests
  at `pipeline × connection_count` (`hold_pipeline()`). A `MOVED` reply
  triggers a `CLUSTER SLOTS` re-read. `handle_cluster_slots()` builds the new
  map before swapping it in, keeps connections whose address is unchanged,
  opens connections to new nodes, and retires the rest. A redirected request
  is retried only with `--retry-on-error`. `--rate-limiting` applies per
  connection, so in cluster mode every node gets the same rate rather than its
  key share.
- valkey-benchmark and redis-benchmark (`--cluster`, `valkey-benchmark.c`) are
  per-node. Each client connects to one node, and `setClusterKeyHashTag()`
  rewrites a `{tag}` in the key to hash to a slot that node owns. `MOVED` or
  `ASK` triggers a `CLUSTER SLOTS` re-read that updates slot ranges of known
  nodes only; an unknown node fails with "could not find node with ID ... in
  current configuration". A redirected request is counted as finished.
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
map from node id to address, and the ketama ring or slot table over node ids. A
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
`on_notify` reconciles its connections against the new `Topology`.

### Reconciling connections

- **Added node:** the worker spawns connections to it. The proposal is a fixed
  connection count per node, set at startup as `connections / initial nodes`,
  so total connections grow with the cluster. That matches a client with one
  pool per node. The alternative is to hold the total fixed and spread it more
  thinly.
- **Removed node:** its connection tasks stop taking new work and exit after
  in-flight requests complete or time out. Queued prefill and append keys for
  the node are re-routed. `requeue_drained_prefill` (`src/worker.rs:3986`)
  already re-routes keys on disconnect and can be generalised.
- **Listed but unreachable node:** connections keep retrying with the existing
  backoff, since a replaced ElastiCache node stays listed for minutes. Requests
  routed to it count as connection failures, and that is the correct result
  during a replacement.
- **Pools:** `standalone_task_capacity` and `timer_slots` need a ceiling that
  covers growth. The simplest option is a `target.max_nodes` setting that sizes
  the pools for `max_nodes × per-node connections`, with a warning when a
  topology change would exceed it.

### Redirects in cluster mode

With a shared slot table and global draws, a `MOVED` reply should re-route and
resend the request once. The redirect reply should then be counted only in
`cluster_redirects`, and the latency recorded should run from the first send
to the final reply. An `ASK` reply needs `ASKING` sent on the target connection
first. Both change what the response count and the latency histograms contain
during a reshard, so they belong in the same CHANGELOG entry.

### Metrics

Add counters for topology generations, nodes added and nodes removed, and keep
`cluster_redirects`. Per-node request and latency metrics are needed to see a
hot node at all. Whether they belong in the parquet snapshot depends on how
many nodes a run can have, and that has not been decided.

## Order of work

1. Correct `docs/guide.md:721-726`. Docs only, independent of everything else.
2. Routed open loop with the per-node queues, on a fixed node list. Config
   validation rejects routed without a rate limit or `saturation_search`.
   Measure client CPU per request against the per-node path at the highest
   pipeline depth and connection count in CI, on io_uring.
3. Routed closed loop: the outstanding-request cap over the same queues, and
   removal of the validation rule from step 2.
4. The `Topology` type with stable ids, replacing positional `endpoints`.
5. The watcher and worker reconciliation for auto discovery. A fake
   configuration endpoint whose version changes mid-run can test this; the
   test servers in `src/autodiscovery.rs` already serve the protocol.
6. The same for cluster mode, with `MOVED`/`ASK` retry and TLS and hostname
   support in `cluster.rs`. This can be tested against a local Valkey cluster
   resharded with `valkey-cli --cluster reshard` during a run.

## Open questions for the owner

- When a node is added, should total connections grow with the node count or
  stay fixed?
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
- How ElastiCache behaves mid-change: whether `config get cluster` lists a node
  before it accepts connections, and whether a replaced node keeps its IP.
  These can only be observed against a real cluster.
