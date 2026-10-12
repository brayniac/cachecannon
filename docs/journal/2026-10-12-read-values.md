# Reading received values (`workload.read_values`)

- **Opened:** 2026-10-12
- **Status:** OPEN — built and tested locally on mio (macOS); client CPU per
  request with the option on and off has not been measured.
- **Commits:** 50e8da1, 1c18cd3 and the docs follow-up on `feat/read-values`.

## Problem

The RESP client receives GET replies with ringline-redis `recv_meta`, which
parses the header and drains the value without assembling it
(`src/worker.rs`, `drive_resp_workload`). A client that uses the value at least
assembles it and reads every byte, so the generator's client CPU per request is
a lower bound on what that client spends. A comparison that charges client CPU
per request to the server under test, or to the client side of a transport, is
then biased toward whichever side moves value bytes the generator skips.

## What was built

`[workload] read_values = true` (off by default):

- RESP receives through ringline-redis `recv`, which holds each whole reply in
  ringline's per-connection accumulator and returns the value as a slice of it.
  On io_uring that is one userspace copy out of the provided buffers that
  `recv_meta` does not make; on mio both paths copy each 8 KiB read into the
  accumulator.
- Every GET hit's value gets a CRC-32c (`src/read_values.rs`), folded into a
  per-connection value passed to `black_box`. Memcache already received whole
  replies and gets only the CRC.
- `values_read_bytes` counts the bytes checksummed.

The first version copied the slice into a per-connection buffer before the CRC.
That was a second userspace copy a reading client does not make, and the buffer
kept the largest value's capacity for the connection's life; 1c18cd3 removed it.

## Verified locally (macOS, mio backend)

- `tests/read_values_*.rs` run one-second benchmarks against in-process fake
  RESP and memcache servers that miss every third GET, with 100 B and 256 KiB
  values. With the option on, bytes read equal hits × value length and misses
  are recorded; with it off, bytes read are 0.
- Each of these was mutated and the matching test went red: skipping the read
  on either protocol, taking `recv_meta` with the option on, reading only the
  first 16 KiB of a value, servers that never miss, servers that return an
  error in place of a miss, removing the CRC, and dropping the ping rejection.

## Not yet done

- The io_uring path has not been run; CI on Linux runs the same tests there.
- Client CPU per request with the option on versus off has not been measured.
  It needs a Linux host on io_uring, at fixed request rate, across value sizes,
  and with `timestamps.mode = "software"` so the CRC does not also appear in
  latency. Until then the size of the bias this option removes is unknown.
