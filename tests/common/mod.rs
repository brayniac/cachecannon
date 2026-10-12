//! Shared by the `read_values_*` tests: fake servers and a one-second
//! benchmark run against them.
//!
//! The runner can be started once per process (`init_config_channel`), and the
//! metrics it records are process-global, so each run is its own test binary.

// Each test binary uses part of this module.
#![allow(dead_code)]

use cachecannon::metrics::{CACHE_HITS, CACHE_MISSES, GET_COUNT, VALUES_READ_BYTES};
use cachecannon::{ColorMode, Config, OutputFormat, create_formatter, run_benchmark_full};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

/// Every third GET on a connection is answered with a miss.
pub const MISS_EVERY: u64 = 3;

/// Large enough to span many receive buffers: io_uring's provided buffers are
/// 16 KiB by default and mio reads 8 KiB at a time.
pub const LARGE_VALUE_LEN: usize = 256 * 1024;

/// What the fake servers return: a value of `value_len` bytes for each GET,
/// except every [`MISS_EVERY`]th GET on a connection, which misses.
#[derive(Clone, Copy)]
pub struct Server {
    pub value_len: usize,
}

impl Server {
    fn value(self) -> Vec<u8> {
        (0..self.value_len).map(|i| b'a' + (i % 26) as u8).collect()
    }

    pub fn spawn_resp(self) -> SocketAddr {
        spawn(move |stream| serve_resp(stream, self))
    }

    pub fn spawn_memcache(self) -> SocketAddr {
        spawn(move |stream| serve_memcache(stream, self))
    }
}

/// Accept connections forever, serving each on its own thread.
fn spawn(serve: impl Fn(TcpStream) + Clone + Send + 'static) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let serve = serve.clone();
            std::thread::spawn(move || serve(stream));
        }
    });
    addr
}

/// Read one CRLF-terminated line, without the CRLF. `None` at EOF.
fn read_line(reader: &mut impl BufRead) -> Option<Vec<u8>> {
    let mut line = Vec::new();
    if reader.read_until(b'\n', &mut line).ok()? == 0 {
        return None;
    }
    line.truncate(line.len().saturating_sub(2));
    Some(line)
}

/// A RESP2 server: GET hits or misses per [`Server`], DEL `:1`, PING `+PONG`,
/// anything else `+OK`.
fn serve_resp(stream: TcpStream, server: Server) {
    let mut writer = stream.try_clone().unwrap();
    let mut reader = BufReader::new(stream);
    let mut hit_reply = format!("${}\r\n", server.value_len).into_bytes();
    hit_reply.extend_from_slice(&server.value());
    hit_reply.extend_from_slice(b"\r\n");
    let mut gets = 0u64;
    while let Some(header) = read_line(&mut reader) {
        let n: usize = std::str::from_utf8(&header[1..]).unwrap().parse().unwrap();
        let mut args = Vec::with_capacity(n);
        for _ in 0..n {
            let len_line = read_line(&mut reader).unwrap();
            let len: usize = std::str::from_utf8(&len_line[1..])
                .unwrap()
                .parse()
                .unwrap();
            let mut arg = vec![0u8; len + 2];
            reader.read_exact(&mut arg).unwrap();
            arg.truncate(len);
            args.push(arg);
        }
        let reply: &[u8] = match args[0].to_ascii_uppercase().as_slice() {
            b"GET" => {
                gets += 1;
                if gets.is_multiple_of(MISS_EVERY) {
                    b"$-1\r\n"
                } else {
                    &hit_reply
                }
            }
            b"PING" => b"+PONG\r\n",
            b"DEL" => b":1\r\n",
            _ => b"+OK\r\n",
        };
        if writer.write_all(reply).is_err() {
            return;
        }
    }
}

/// A memcache ASCII server: `get` hits or misses per [`Server`], `set` is
/// `STORED`.
fn serve_memcache(stream: TcpStream, server: Server) {
    let mut writer = stream.try_clone().unwrap();
    let mut reader = BufReader::new(stream);
    let value = server.value();
    let mut gets = 0u64;
    while let Some(line) = read_line(&mut reader) {
        let mut words = line.split(|b| *b == b' ');
        let reply = match words.next().unwrap() {
            b"get" => {
                gets += 1;
                if gets.is_multiple_of(MISS_EVERY) {
                    b"END\r\n".to_vec()
                } else {
                    let key = words.next().unwrap();
                    let mut reply = b"VALUE ".to_vec();
                    reply.extend_from_slice(key);
                    reply.extend_from_slice(format!(" 0 {}\r\n", server.value_len).as_bytes());
                    reply.extend_from_slice(&value);
                    reply.extend_from_slice(b"\r\nEND\r\n");
                    reply
                }
            }
            b"set" => {
                let len: usize = std::str::from_utf8(line.rsplit(|b| *b == b' ').next().unwrap())
                    .unwrap()
                    .parse()
                    .unwrap();
                let mut data = vec![0u8; len + 2];
                reader.read_exact(&mut data).unwrap();
                b"STORED\r\n".to_vec()
            }
            b"version" => b"VERSION 1.0\r\n".to_vec(),
            _ => b"ERROR\r\n".to_vec(),
        };
        if writer.write_all(&reply).is_err() {
            return;
        }
    }
}

/// What one run recorded.
#[derive(Debug)]
pub struct Counts {
    pub gets: u64,
    pub hits: u64,
    pub misses: u64,
    pub read: u64,
}

/// Run a one-second, GET-only benchmark against `addr`.
pub fn run(addr: SocketAddr, protocol: &str, read_values: bool) -> Counts {
    let toml = format!(
        r#"
        [general]
        duration = "1s"
        warmup = "0s"
        threads = 1

        [target]
        endpoints = ["{addr}"]
        protocol = "{protocol}"

        [connection]
        connections = 2
        pipeline_depth = 4

        [workload]
        read_values = {read_values}

        [workload.keyspace]
        count = 100

        [workload.commands]
        get = 100
        set = 0

        [admin]
        format = "quiet"
        "#
    );
    let path = std::env::temp_dir().join(format!(
        "cachecannon-read-values-{}-{protocol}-{read_values}.toml",
        std::process::id()
    ));
    std::fs::write(&path, toml).unwrap();
    let config = Config::load(&path).unwrap();
    std::fs::remove_file(&path).unwrap();

    run_benchmark_full(
        config,
        None,
        create_formatter(OutputFormat::Quiet, ColorMode::Never),
        Arc::new(AtomicBool::new(true)),
    )
    .unwrap();
    Counts {
        gets: GET_COUNT.value(),
        hits: CACHE_HITS.value(),
        misses: CACHE_MISSES.value(),
        read: VALUES_READ_BYTES.value(),
    }
}

/// Run with `read_values = true` and check that every hit, and nothing else,
/// was read: some GETs missed, and bytes read are hits times the value length.
pub fn assert_every_hit_read(protocol: &str, server: Server) {
    let addr = match protocol {
        "resp" => server.spawn_resp(),
        "memcache" => server.spawn_memcache(),
        _ => unreachable!(),
    };
    let c = run(addr, protocol, true);
    assert!(c.hits > 0, "no hits recorded: {c:?}");
    assert!(c.misses > 0, "no misses recorded: {c:?}");
    assert_eq!(
        c.hits + c.misses,
        c.gets,
        "a GET was neither hit nor miss: {c:?}"
    );
    assert_eq!(
        c.read,
        c.hits * server.value_len as u64,
        "{} hits of {} bytes, but {} bytes read",
        c.hits,
        server.value_len,
        c.read
    );
}
