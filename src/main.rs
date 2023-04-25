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

use postgres::Client;

use crate::fibonacci::Fibonacci;
use crate::threader::workload::Workload;

const PROGRAM_DESC: &'static str = "generate cpu load on a Postgres cluster, and output the TPS.";
const PROGRAM_NAME: &'static str = "pg_cpu_load";

struct TransactDataSample {
    samplemoment: chrono::NaiveDateTime,
    lsn: String,
    wal_bytes: f32,
    num_transactions: f32,
}

fn duration(start: chrono::NaiveDateTime, end: chrono::NaiveDateTime) -> f32 {
    let duration_nanos = (end - start).num_nanoseconds().unwrap();
    duration_nanos as f32 / 10.0_f32.powi(9)
}

fn sample( client: &mut Client, query: &String, tps: u64, stype: &String, thread_id: u32) -> Result<u64, postgres::Error> {
    let mut num_queries = tps / 10;
    if num_queries < 1 {
        num_queries = 1;
    }
    for _x in 1..num_queries {
        if stype == "prepared" {
            let prep = client.prepare(query)?;
            let _row = client.query(&prep, &[&thread_id]);
        } else if stype == "transactional" {
            let mut trans = client.transaction()?;
            if query != "" {
                let _row = trans.query(query, &[&thread_id]);
            }
            let _res = trans.commit()?;
        } else if stype == "prepared_transactional" {
            let mut trans = client.transaction()?;
            if query != "" {
                let prep = trans.prepare(&query)?;
                let _row = trans.query(&prep, &[&thread_id]);
            }
            let _res = trans.commit()?;
        } else if query != "" {
            let _row = &client.query(query, &[&thread_id]);
        }
    }
    Ok(num_queries)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = cli::Params::get_args();


    println!("Initializing");
    let (min_threads, max_threads) = args.range_min_max();
    let w: Workload = args.as_workload();
    let threader = threader::Threader::new(max_threads, w);
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
        threader.rescale(num_threads);
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
