use std::collections::HashMap;
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
        if temp < self.min {
            self.min = temp;
        }
        if temp > self.max {
            self.max = temp;
        }
        self.sum += temp;
        self.count += 1;
    }

    fn mean(&self) -> f64 {
        self.sum / self.count as f64
    }
}

fn run() -> std::io::Result<()> {
    let file = File::open("data/measurements.txt")?;
    let reader = BufReader::new(file);

    let mut data: HashMap<String, TempStats> = HashMap::new();

    for line in reader.lines() {
        let line = line?;
        if let Some((station, temp_str)) = line.split_once(';') {
            if let Ok(temp) = temp_str.trim().parse::<f64>() {
                data.entry(station.trim().to_string())
                    .and_modify(|stats| stats.update(temp))
                    .or_insert_with(|| TempStats::new(temp));
            }
        }
    }

    let mut stations: Vec<_> = data.into_iter().collect();
    stations.sort_by_key(|(name, _)| name.clone());

    let output: Vec<String> = stations
        .into_iter()
        .map(|(station, stats)| {
            format!(
                
                "{}: {:.1}/{:.1}/{:.1}",
                station,
                stats.min,
                stats.mean(),
                stats.max
            )
        })
        .collect();

    println!("{}", output.join(", "));

    Ok(())
}

fn main() -> std::io::Result<()> {
    let start = Instant::now();

    let result = run();

    let duration = start.elapsed();
    println!("Execution time: {:.3?}", duration);

    result
}
