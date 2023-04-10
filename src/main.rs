extern crate postgres;
extern crate args;
extern crate getopts;
extern crate chrono;

mod cli;
mod dsn;
mod generic;
mod threader;

use chrono::Utc;
use postgres::{Client, tls};
use std::time::Duration;
use std::thread;
use std::sync::{mpsc, RwLock, Arc};

use crate::dsn::Dsn;

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

    println!("Initializing all threads");
    let (min_threads, max_threads) = args.range_min_max();
    let mut threads = Vec::with_capacity(max_threads as usize);
    let num_samples: u32;
    let num_threads: u32 = 0;
    let threader = threader::Threader::get_args(max_threads);

    println!("Date       time (sec)      | Sample period |          Threads         |              Postgres         |");
    println!("                           |               | Average TPS | Total TPS  |        tps   |          wal/s |");
    //        2019-06-24 11:33:23.437502       1.018000      105.090     10508.950      16888.312            0.000

    for num_threads in min_threads..max_threads {
        threader.rescale(num_threads);
    }

    if num_samples < 1 {
        num_samples = 1
    }
    let sample_period = chrono::Duration::seconds(1);

    let mut prev_sample = TransactDataSample {
        samplemoment: Utc::now().naive_utc(),
        lsn: "0/0".to_string(),
        wal_bytes: 0.0_f32,
        num_transactions: 0.0_f32,
    };
    let mut wait = Duration::from_millis(100);


    for x in 0..num_secs {
        let start = Utc::now().naive_utc();
        let finished = start + sample_period;
        sum_trans = 0;
        loop {
            for _ in 0..num_samples {
                match rx.recv_timeout(wait) {
                    Ok(sample_trans) => sum_trans += sample_trans,
                    Err(_error) => break,
                }
            }
            if Utc::now().naive_utc() > finished {
                break;
            }
        }
        let end = Utc::now().naive_utc();
        let calc_tps = sum_trans as f32 / duration(start, end);
        threads_avg_tps = calc_tps / num_threads as f32;

        let rows = client.query(&stat_databases_sttmnt, &[&prev_sample.lsn])?;
        assert_eq!(rows.len(), 1);
        let row = rows.get(0).unwrap();
        let sample = TransactDataSample {
            samplemoment: row.get(0),
            lsn: row.get(1),
            wal_bytes: row.get(2),
            num_transactions: row.get(3),
        };
        let now = sample.samplemoment;
        if x > 1 {
            let postgres_duration = duration(prev_sample.samplemoment, sample.samplemoment);
            let postgres_wps = (sample.wal_bytes - prev_sample.wal_bytes) as f32 / postgres_duration;
            let postgres_tps = (sample.num_transactions - prev_sample.num_transactions) as f32 / postgres_duration;
            let thread_duration = (end-start).num_milliseconds() as f32 / 1000_f32;
            println!("{0} {1:15.6} {2:>12.3} {3:>13.3} {4:>14.3} {5:>16.3}", now, thread_duration, threads_avg_tps, calc_tps, postgres_tps, postgres_wps);
        }
        prev_sample = sample;
    }

    println!("Stopping, but lets give the threads some time to stop");
    threader.finish();

    println!("Finished");
    ::std::process::exit(0);
}
