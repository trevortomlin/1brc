use rustc_hash::FxHashMap;
use std::fmt::Write;
use std::fs::File;
use std::io::{BufRead, BufReader};
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
    let mut data: FxHashMap<String, TempStats> = FxHashMap::default();
    let mut line = String::new();

    while reader.read_line(&mut line)? != 0 {
        if let Some((station_raw, temp_str)) = line.trim_end().split_once(';') {
            let station = station_raw.trim();
            if let Ok(temp) = temp_str.trim().parse::<f64>() {
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

    let mut out = String::new();
    for (i, (station, stats)) in stations.into_iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        write!(
            &mut out,
            "{}: {:.1}/{:.1}/{:.1}",
            station,
            stats.min,
            stats.mean(),
            stats.max
        )
        .unwrap();
    }
    println!("{}", out);

    Ok(())
}

fn main() -> std::io::Result<()> {
    let start = Instant::now();

    let result = run();

    let duration = start.elapsed();
    println!("Execution time: {:.3?}", duration);

    result
}
