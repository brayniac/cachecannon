//! Shared by the `read_values_*` tests: fake servers and a one-second
//! benchmark run against them.
//!
//! The runner can be started once per process (`init_config_channel`), and the
//! metrics it records are process-global, so each run is its own test binary.

// Each test binary uses part of this module.
#![allow(dead_code)]

use cachecannon::metrics::{CACHE_HITS, VALUES_READ_BYTES};
use cachecannon::{ColorMode, Config, OutputFormat, create_formatter, run_benchmark_full};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

/// Length of the value the fake servers return for every GET.
pub const VALUE_LEN: usize = 100;

fn value() -> Vec<u8> {
    (0..VALUE_LEN).map(|i| b'a' + (i % 26) as u8).collect()
}

/// Accept connections forever, serving each on its own thread.
pub fn spawn_server(serve: fn(TcpStream)) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
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

/// A RESP server: GET hits with [`value`], SET/PING/anything else `+OK`/`+PONG`.
pub fn serve_resp(stream: TcpStream) {
    let mut writer = stream.try_clone().unwrap();
    let mut reader = BufReader::new(stream);
    let value = value();
    let mut get_reply = format!("${VALUE_LEN}\r\n").into_bytes();
    get_reply.extend_from_slice(&value);
    get_reply.extend_from_slice(b"\r\n");
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
            b"GET" => &get_reply,
            b"PING" => b"+PONG\r\n",
            b"DEL" => b":1\r\n",
            _ => b"+OK\r\n",
        };
        if writer.write_all(reply).is_err() {
            return;
        }
    }
}

/// A memcache ASCII server: every `get` hits with [`value`], everything else
/// is `STORED`.
pub fn serve_memcache(stream: TcpStream) {
    let mut writer = stream.try_clone().unwrap();
    let mut reader = BufReader::new(stream);
    let value = value();
    while let Some(line) = read_line(&mut reader) {
        let mut words = line.split(|b| *b == b' ');
        let reply = match words.next().unwrap() {
            b"get" => {
                let key = words.next().unwrap();
                let mut reply = b"VALUE ".to_vec();
                reply.extend_from_slice(key);
                reply.extend_from_slice(format!(" 0 {VALUE_LEN}\r\n").as_bytes());
                reply.extend_from_slice(&value);
                reply.extend_from_slice(b"\r\nEND\r\n");
                reply
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

/// Run a one-second, GET-only benchmark against `addr` and return how many
/// hits it recorded and how many value bytes it read.
pub fn run(addr: SocketAddr, protocol: &str, read_values: bool) -> (u64, u64) {
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

    let hits_before = CACHE_HITS.value();
    let read_before = VALUES_READ_BYTES.value();
    run_benchmark_full(
        config,
        None,
        create_formatter(OutputFormat::Quiet, ColorMode::Never),
        Arc::new(AtomicBool::new(true)),
    )
    .unwrap();
    (
        CACHE_HITS.value() - hits_before,
        VALUES_READ_BYTES.value() - read_before,
    )
}
