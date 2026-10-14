//! A rate-limited run starts at its rate. A fake RESP server timestamps every
//! SET of a 1 s run limited to 2,000 per second with no warmup; the first
//! 100 ms may carry about 200 of them. Each worker's limiter used to start
//! holding a second's share, and the run opened by sending about 2,000 at
//! once.

use cachecannon::{ColorMode, Config, OutputFormat, create_formatter, run_benchmark_full};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const RATE: usize = 2000;

fn read_line(reader: &mut impl BufRead) -> Option<Vec<u8>> {
    let mut line = Vec::new();
    if reader.read_until(b'\n', &mut line).ok()? == 0 {
        return None;
    }
    line.truncate(line.len().saturating_sub(2));
    Some(line)
}

/// Answer every command `+OK` (PING `+PONG`) and record when each SET arrived.
fn serve(stream: TcpStream, sets: &Mutex<Vec<Instant>>) {
    let mut writer = stream.try_clone().unwrap();
    let mut reader = BufReader::new(stream);
    while let Some(header) = read_line(&mut reader) {
        let n: usize = std::str::from_utf8(&header[1..]).unwrap().parse().unwrap();
        let mut command = Vec::new();
        for i in 0..n {
            let len_line = read_line(&mut reader).unwrap();
            let len: usize = std::str::from_utf8(&len_line[1..])
                .unwrap()
                .parse()
                .unwrap();
            let mut arg = vec![0u8; len + 2];
            reader.read_exact(&mut arg).unwrap();
            if i == 0 {
                arg.truncate(len);
                command = arg.to_ascii_uppercase();
            }
        }
        let reply: &[u8] = match command.as_slice() {
            b"SET" => {
                sets.lock().unwrap().push(Instant::now());
                b"+OK\r\n"
            }
            b"PING" => b"+PONG\r\n",
            _ => b"+OK\r\n",
        };
        if writer.write_all(reply).is_err() {
            return;
        }
    }
}

fn spawn() -> (SocketAddr, Arc<Mutex<Vec<Instant>>>) {
    let sets = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&sets);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let recorded = Arc::clone(&recorded);
            std::thread::spawn(move || serve(stream, &recorded));
        }
    });
    (addr, sets)
}

#[test]
fn a_rate_limited_run_does_not_open_with_a_burst() {
    let (addr, sets) = spawn();
    let toml = format!(
        r#"
        [general]
        duration = "1s"
        warmup = "0s"
        threads = 4

        [target]
        endpoints = ["{addr}"]
        protocol = "resp"

        [connection]
        connections = 16
        pipeline_depth = 4

        [workload]
        rate_limit = {RATE}

        [workload.keyspace]
        count = 1000

        [workload.commands]
        get = 0
        set = 100

        [admin]
        format = "quiet"
        "#
    );
    let path = std::env::temp_dir().join(format!(
        "cachecannon-rate-limit-start-{}.toml",
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

    let sets = sets.lock().unwrap();
    assert!(
        sets.len() > RATE / 2,
        "only {} SETs in a 1 s run",
        sets.len()
    );
    let first = sets[0];
    let early = sets
        .iter()
        .filter(|t| t.duration_since(first) < Duration::from_millis(100))
        .count();
    // 100 ms at 2,000/s is 200; allow one batch per connection on top.
    assert!(
        early <= RATE / 10 + 16 * 4,
        "{early} SETs in the first 100 ms"
    );
}
