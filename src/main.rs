use ahash::AHashMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Write};
use std::time::Instant;

#[derive(Debug)]
struct TempStats {
    min: f64,
    max: f64,
    sum: f64,
    count: usize,
}

impl TempStats {
    fn new(temp: f64) -> Self {
        TempStats {
            min: temp,
            max: temp,
            sum: temp,
            count: 1,
        }
    }

    fn update(&mut self, temp: f64) {
        self.min = self.min.min(temp);
        self.max = self.max.max(temp);
        self.sum += temp;
        self.count += 1;
    }

    fn mean(&self) -> f64 {
        self.sum / self.count as f64
    }
}

fn run() -> Result<(), std::io::Error> {
    let file = File::open("data/measurements.txt")?;
    let mut reader = BufReader::new(file);
    let mut data: AHashMap<String, TempStats> = AHashMap::default();
    let mut line = String::new();

    while reader.read_line(&mut line)? != 0 {
        if let Some((station, temp_str)) = line.split_once(';') {
            if let Ok(temp) = fast_float::parse(&temp_str[..temp_str.len() - 1]) {
                if let Some(stats) = data.get_mut(station) {
                    stats.update(temp);
                } else {
                    data.insert(station.to_string(), TempStats::new(temp));
                }
            }
        }

        line.clear();
    }

    let mut stations: Vec<_> = data.into_iter().collect();
    stations.sort_by_key(|(name, _)| name.clone());

    let stdout = io::stdout();
    let mut handle = stdout.lock();
    let mut first = true;

    for (station, stats) in stations {
        if !first {
            write!(handle, ", ").unwrap();
        } else {
            first = false;
        }

        write!(
            handle,
            "{}: {:.1}/{:.1}/{:.1}",
            station,
            stats.min,
            stats.mean(),
            stats.max
        )
        .unwrap();
    }

    writeln!(handle).unwrap();

    Ok(())
}

fn main() -> std::io::Result<()> {
    let start = Instant::now();

    let result = run();

    let duration = start.elapsed();
    println!("Execution time: {:.3?}", duration);

    result
}
