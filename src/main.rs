extern crate args;
extern crate chrono;
extern crate getopts;
extern crate postgres;

mod cli;
mod dsn;
mod fibonacci;
mod generic;
mod pg_sampler;
mod threader;

use crate::fibonacci::Fibonacci;
use crate::threader::workload::Workload;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = cli::Params::get_args();

    println!("Initializing");
    let (min_threads, max_threads) = args.range_min_max();
    let w: Workload = args.as_workload();
    println!("{}", w.as_string());
    let mut threader = threader::Threader::new(max_threads as usize, w);
    let mut sampler = pg_sampler::PgSampler::new(args.as_dsn())?;
    sampler.next()?;

    println!("min threads: {} max threads: {}", min_threads, max_threads);

    println!("|---------------------|---------|-------------------------------------|-----------------------|");
    println!("| Date       time     | Clients |              Performance            |       Postgres        |");
    println!("|                     |         |-------------|---------|-------------|-----------|-----------|");
    println!("|                     |         |    TPS      | Latency | TPS/Latency |   TPS     |    wal    |");
    println!("|                     |         |             | (usec)  |             |           |    kB/s   |");
    println!("|---------------------|---------|-------------|---------|-------------|-----------|-----------|");
    //        2019-06-24 11:33:23 |       1 | 2.105.090 |  10.121 | 2.168.312 | 1.105.131 |

    for num_threads in Fibonacci::new(1_u32, 1_u32).take_while(|v| *v < max_threads) {
        if num_threads < min_threads {
            continue;
        }
        threader.scaleup(num_threads);
        let d: chrono::Duration = chrono::Duration::from_std(args.max_wait.into())?;
        match threader.wait_stable(args.spread, args.min_samples as usize, d) {
            Some(result) => {
                sampler.next()?;
                let latency = result.latency.num_microseconds().unwrap() as f64;
                println!(
                    "| {0} | {1:7.5} | {2:>11.3} | {3:>7.1} | {4:>11.3} | {5:>9.3} | {6:>9.3} |",
                    chrono::offset::Local::now().format("%Y-%m-%d %H:%M:%S"),
                    num_threads,
                    result.tps,
                    latency,
                    result.tps / latency,
                    sampler.tps(),
                    sampler.wal_per_sec() as i32,
                )
            }
            None => println!(
                "| {0} | {1:7.5} | {2:>11.3} | {3:>7.1} | {4:>11.3} | {5:>9.3} | {6:>9.3} |",
                chrono::offset::Local::now().format("%Y-%m-%d %H:%M:%S"),
                num_threads,
                "?",
                "?",
                "?",
                "?",
                "?"
            ),
        }
    }
    println!("|---------------------|---------|-------------|---------|-------------|-----------|-----------|");

    println!("Stopping, but lets give the threads some time to stop");
    threader.finish();

    println!("Finished");
    ::std::process::exit(0);
}
