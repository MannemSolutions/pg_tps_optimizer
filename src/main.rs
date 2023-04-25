extern crate postgres;
extern crate args;
extern crate getopts;
extern crate chrono;

mod cli;
mod dsn;
mod pg_sampler;
mod generic;
mod threader;
mod fibonacci;

use crate::fibonacci::Fibonacci;
use crate::threader::workload::Workload;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = cli::Params::get_args();


    println!("Initializing");
    let (min_threads, max_threads) = args.range_min_max();
    let w: Workload = args.as_workload();
    let mut threader = threader::Threader::new(max_threads, w);
    let mut sampler = pg_sampler::PgSampler::new(args.as_dsn())?;
    sampler.next()?;

    println!("Date       time            | Clients |        Perfrormance        |         Postgres        |");
    println!("                           |         |   TPS     | Latency (msec) |    TPS     |  wal(kB)/s |");
    //        2019-06-24 11:33:23.437502 |       1 | 2.105.090 |     10.121     | 2.168.312  | 1.105.131  |

    for num_threads in Fibonacci::new(1_u32, 1_32) {
        if num_threads < min_threads {
            continue;
        }
        if num_threads > max_threads {
            break;
        }
        threader.scaleup(num_threads);
        let d: chrono::Duration = chrono::Duration::from_std(args.max_wait.into())?;
        match threader.wait_stable(args.spread, args.min_samples as usize, d) {
            Some(result) => {
                sampler.next()?;
                println!("{0} {1:15.6} {2:>12.3} {3:>13.3} {4:>14.3} {5:>16.3}",
                      chrono::offset::Local::now(), num_threads, result.tps, result.latency, sampler.tps(), sampler.wal_per_sec())
            },
            None =>
                println!("{0} {1:15.6} {2:>12.3} {3:>13.3} {4:>14.3} {5:>16.3}",
                      chrono::offset::Local::now(), num_threads, "?", "?", "?", "?"),
        }
    }


    println!("Stopping, but lets give the threads some time to stop");
    threader.finish();

    println!("Finished");
    ::std::process::exit(0);
}
