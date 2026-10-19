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
- The redirected request is not retried. `map_respmeta` records it with
  `success = false` and `is_error_response = true` (`src/worker.rs:2130`).
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

### Proposed: draw the key, then route it

A request draws its key from the global distribution, routes it with the
current topology, and is sent on a connection to the owning node. One route per
request costs one CRC16 or one ketama lookup. The 40% figure above came from
about one route per node per request under rejection sampling, so it does not
apply, but the single route has not been measured.

This makes the two load regimes behave differently, which they should:

1. **Open loop (rate-limited).** The dispatcher funds a request, the request
   draws and routes, and the node receives `share × rate`. A hot node receives
   more requests than it can serve and its latency rises. The offered load is
   independent of how fast any node answers.
2. **Closed loop, global keys (proposed default).** A fixed number of
   outstanding requests, each of which draws a key, routes, waits for the reply,
   and draws again. Requests accumulate on the slowest node, so the hot node
   bounds total throughput. This is how an application with a fixed thread
   pool behaves against a cluster.
3. **Closed loop, partitioned keys (opt-in, today's behaviour).** Each
   connection draws only its node's keys. Each node runs at its own capacity,
   so the result is the sum of per-node capacities. That is a useful number for
   node sizing, but it is not the throughput a client would see.

Mode 2 changes the closed-loop numbers of every existing multi-endpoint zipf
run: total throughput will fall by roughly the hot node's excess share. That
needs a CHANGELOG entry that says so, and mode 3 needs to stay available so old
results can be reproduced.

Mode 2 also changes how connections are used. Today a connection's pipeline is
filled by the task that owns it. With global draws, a request is not tied to a
connection until it is routed. The proposal is a per-worker set of
outstanding-request slots, sized `connections × pipeline_depth`, feeding one
queue per node. Each connection task drains its node's queue. In open loop
the dispatcher funds slots instead of connections. This is the part of the
design with the most risk to the hot path, and it should be built and measured
before the topology work depends on it.

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
resend the request once, counted in `cluster_redirects` and not as an error
response. An `ASK` reply needs `ASKING` sent on the target connection first.
Both change what the error count means during a reshard, so they belong in the
same CHANGELOG entry.

### Metrics

Add counters for topology generations, nodes added and nodes removed, and keep
`cluster_redirects`. Per-node request and latency metrics are needed to see a
hot node at all. Whether they belong in the parquet snapshot depends on how
many nodes a run can have, and that has not been decided.

## Order of work

1. Correct `docs/guide.md:721-726`. Docs only, independent of everything else.
2. Draw-then-route key selection with the three modes, on a fixed node list.
   Measure client CPU per request against the partitioned path at the highest
   pipeline depth and connection count in CI, on io_uring.
3. The `Topology` type with stable ids, replacing positional `endpoints`.
4. The watcher and worker reconciliation for auto discovery. A fake
   configuration endpoint whose version changes mid-run can test this; the
   test servers in `src/autodiscovery.rs` already serve the protocol.
5. The same for cluster mode, with `MOVED`/`ASK` retry and TLS and hostname
   support in `cluster.rs`. This can be tested against a local Valkey cluster
   resharded with `valkey-cli --cluster reshard` during a run.

## Open questions for the owner

- When a node is added, should total connections grow with the node count or
  stay fixed?
- Should mode 2 become the closed-loop default, given that it changes existing
  results?
- Should a redirected request be retried, or should only the redirect be
  recorded? Retrying matches a real client; not retrying keeps every request a
  single round trip.

## Not yet known

- The per-request cost of draw-then-route against the partitioned path.
- The per-request cost of the outstanding-request slots and per-node queues
  against the current per-connection pipeline.
- How ElastiCache behaves mid-change: whether `config get cluster` lists a node
  before it accepts connections, and whether a replaced node keeps its IP.
  These can only be observed against a real cluster.
