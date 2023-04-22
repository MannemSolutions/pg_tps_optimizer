extern crate postgres;
extern crate args;
extern crate getopts;
extern crate chrono;

mod cli;
mod dsn;
mod generic;
mod threader;
mod fibonacci;

use chrono::Utc;
use postgres::Client;
use std::time::Duration;
use std::sync::{mpsc, RwLock, Arc};

use crate::dsn::Dsn;
use crate::fibonacci::Fibonacci;

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
    let mut sum_trans: u64;
    let mut threads_avg_tps: f32;
    let args = cli::Params::get_args();

    let (tx, rx) = mpsc::channel();
    //let rw_lock = Arc::new(RwLock::new(true));
    let rw_lock = Arc::new(RwLock::new(false));
    let rw_downscaler_lock = Arc::new(RwLock::new(false));

    let client = Dsn::from_string(args.dsn.as_str()).client();
    let stat_databases_sttmnt = client.prepare(
        "SELECT now()::timestamp as samplemmoment,
        pg_current_wal_lsn()::varchar as lsn,
        (pg_current_wal_lsn() - $1::varchar::pg_lsn)::real as walbytes,
        (select sum(xact_commit+xact_rollback)::real
         FROM pg_stat_database) as transacts")?;

    println!("Initializing");
    let (min_threads, max_threads) = args.range_min_max();
    let mut threads = Vec::with_capacity(max_threads as usize);
    let num_samples: u32;
    let threader = threader::Threader::new(max_threads, args.as_workload());

    println!("Date       time (sec)      | Sample period |          Threads         |              Postgres         |");
    println!("                           |               | Average TPS | Total TPS  |        tps   |          wal/s |");
    //        2019-06-24 11:33:23.437502       1.018000      105.090     10508.950      16888.312            0.000

    for i in Fibonacci::new(u32::from(1), u32::from(1)) {
        if i < min_threads {
            continue;
        }
        if i > max_threads {
            break;
        }
        threader.rescale(num_threads);
        threader.wait_stable(spread, count, max_wait)

    }

            println!("{0} {1:15.6} {2:>12.3} {3:>13.3} {4:>14.3} {5:>16.3}", now, thread_duration, threads_avg_tps, calc_tps, postgres_tps, postgres_wps);

    println!("Stopping, but lets give the threads some time to stop");
    threader.finish();

    println!("Finished");
    ::std::process::exit(0);
}
