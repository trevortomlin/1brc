use ahash::AHashMap;
use crossbeam::channel::unbounded;
use crossbeam::thread;
use memchr::memchr;
use std::fs::File;
use std::io::Read;
use std::io::{self, BufReader, Write};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Debug, Clone)]
struct TempStats {
    min: i16,
    max: i16,
    sum: i64,
    count: usize,
}

impl TempStats {
    fn new(temp: i16) -> Self {
        TempStats {
            min: temp,
            max: temp,
            sum: temp as i64,
            count: 1,
        }
    }

    fn update(&mut self, temp: i16) {
        self.min = self.min.min(temp);
        self.max = self.max.max(temp);
        self.sum += temp as i64;
        self.count += 1;
    }

    fn mean(&self) -> f64 {
        self.sum as f64 / (self.count as f64 * 10.0)
    }

    pub fn merge(&mut self, other: &TempStats) {
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.sum += other.sum;
        self.count += other.count;
    }
}

fn read_chunks(tx: crossbeam::channel::Sender<Vec<u8>>, chunk_size: usize) -> io::Result<()> {
    let file = File::open("data/measurements.txt")?;
    let mut reader = BufReader::new(file);
    let mut buffer = vec![0; chunk_size + 1024];
    let mut leftover = Vec::new();

    loop {
        let read_bytes = reader.read(&mut buffer)?;
        if read_bytes == 0 {
            if !leftover.is_empty() {
                tx.send(std::mem::take(&mut leftover)).unwrap();
            }
            break;
        }

        let mut chunk = Vec::with_capacity(leftover.len() + read_bytes);
        chunk.extend_from_slice(&leftover);
        chunk.extend_from_slice(&buffer[..read_bytes]);

        if let Some(last_nl) = chunk.iter().rposition(|&b| b == b'\n') {
            let full_chunk = chunk[..=last_nl].to_vec();
            leftover = chunk[last_nl + 1..].to_vec();
            tx.send(full_chunk).unwrap();
        } else {
            leftover = chunk;
        }
    }

    println!("\rReading progress: 100%");
    Ok(())
}

pub fn run_parallel(chunk_size: usize) -> io::Result<()> {
    let (tx, rx) = unbounded::<Vec<u8>>();
    let num_threads = num_cpus::get();
    let data = Arc::new(parking_lot::Mutex::new(Vec::with_capacity(num_threads)));

    thread::scope(|s| {
        for _ in 0..num_threads {
            let rx = rx.clone();
            let data = Arc::clone(&data);

            s.spawn(move |_| {
                let mut local_map: AHashMap<Box<[u8]>, TempStats> = AHashMap::default();

                for chunk in rx.iter() {
                    for line in chunk.split(|&b| b == b'\n') {
                        if line.is_empty() {
                            continue;
                        }

                        if let Some(idx) = memchr(b';', line) {
                            let station = &line[..idx];
                            let mut temp_bytes = &line[idx + 1..];

                            if temp_bytes.ends_with(b"\r") {
                                temp_bytes = &temp_bytes[..temp_bytes.len() - 1];
                            }

                            if let Some(temp) = parse_fixed_point(temp_bytes) {
                                let key = station.to_owned().into_boxed_slice();
                                local_map
                                    .entry(key)
                                    .and_modify(|s| s.update(temp))
                                    .or_insert_with(|| TempStats::new(temp));
                            }
                        }
                    }
                }

                data.lock().push(local_map);
            });
        }

        s.spawn(|_| {
            read_chunks(tx, chunk_size).unwrap();
        });
    })
    .unwrap();

    let mut merged: AHashMap<Box<[u8]>, TempStats> = AHashMap::default();
    for map in Arc::try_unwrap(data).unwrap().into_inner() {
        for (k, v) in map {
            merged.entry(k).and_modify(|s| s.merge(&v)).or_insert(v);
        }
    }

    let mut stations: Vec<_> = merged.into_iter().collect();
    stations.sort_by(|(a, _), (b, _)| a.cmp(&b));

    let stdout = io::stdout();
    let mut handle = stdout.lock();
    let mut first = true;

    for (station, stats) in stations {
        if !first {
            write!(handle, ", ")?;
        } else {
            first = false;
        }

        if let Ok(station_str) = std::str::from_utf8(&station) {
            write!(
                handle,
                "{}: {:.1}/{:.1}/{:.1}",
                station_str,
                stats.min,
                stats.mean(),
                stats.max
            )?
        }
    }

    writeln!(handle)?;
    Ok(())
}

fn parse_fixed_point(temp_bytes: &[u8]) -> Option<i16> {
    if temp_bytes.len() < 3 {
        return None;
    }

    let mut negative = false;
    let mut i = 0;

    if temp_bytes[0] == b'-' {
        negative = true;
        i += 1;
    }

    let mut value: i16 = 0;

    while i < temp_bytes.len() {
        match temp_bytes[i] {
            b'0'..=b'9' => {
                value = value * 10 + ((temp_bytes[i] - b'0') as i16);
            }
            b'.' => {
                i += 1;
                if i < temp_bytes.len() && temp_bytes[i].is_ascii_digit() {
                    value = value * 10 + ((temp_bytes[i] - b'0') as i16);
                    return Some(if negative { -value } else { value });
                } else {
                    return None;
                }
            }
            _ => return None,
        }

        i += 1;
    }

    None
}

fn main() -> std::io::Result<()> {
    let mut results = Vec::new();
    let mut chunk_size = 2 * 1024 * 1024;

    let attempts = 10;
    while chunk_size <= 1024 * 1024 * 64 {
        let mut total_duration = Duration::new(0, 0);

        for _ in 0..attempts {
            let start = Instant::now();
            run_parallel(chunk_size)?;
            total_duration += start.elapsed();
        }

        let avg_secs = total_duration.as_secs_f64() / attempts as f64;
        results.push((chunk_size, avg_secs));
        chunk_size *= 2;
    }

    println!("\nAverage execution times:");
    for (size, avg_time) in results {
        println!("Chunk size: {:>8} bytes -> {:.3} sec", size, avg_time);
    }

    Ok(())
}
