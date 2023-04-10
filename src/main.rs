extern crate postgres;
extern crate args;
extern crate getopts;
extern crate chrono;

mod cli;
mod dsn;
mod generic;

use chrono::Utc;
use postgres::{Client, tls};
use std::time::Duration;
use std::thread;
use std::sync::{mpsc, RwLock, Arc};
use std::str::FromStr;

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

fn postgres_param(argument: &Result<String, args::ArgsError>, env_var_key: &String, default: &String) -> String {
    let mut return_val: String;
    match env::var(env_var_key) {
        Ok(val) => return_val = val,
        Err(_err) => return_val = default.to_string(),
    }
    if return_val.is_empty() {
        return_val = default.to_string()
    }
    match argument {
        Ok(val) => return_val = val.to_string(),
        Err(_err) => (),
    }
    return_val
}

fn postgres_connect_string(args: args::Args) -> String {
    let mut connect_string: String;
    let pgport = postgres_param(&args.value_of("port"), &"PGPORT".to_string(), &"5432".to_string());
    let pguser = postgres_param(&args.value_of("user"), &"PGUSER".to_string(), &"postgres".to_string());
    let pghost = postgres_param(&args.value_of("host"), &"PGHOST".to_string(), &"localhost".to_string());
    let pgpassword = postgres_param(&args.value_of("password"), &"PGPASSWORD".to_string(), &"".to_string());
    let pgdatabase = postgres_param(&args.value_of("dbname"), &"PGDATABASE".to_string(), &pguser);
//  postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]
    connect_string = "postgres://".to_string();
    if ! pguser.is_empty() {
        connect_string.push_str(&pguser);
        if ! pgpassword.is_empty() {
            connect_string.push_str(":");
            connect_string.push_str(&pgpassword);
        }
        connect_string.push_str("@");
    }
    connect_string.push_str(&pghost);
    if ! pgport.is_empty() {
        connect_string.push_str(":");
        connect_string.push_str(&pgport);
    }
    if ! pgdatabase.is_empty() {
        connect_string.push_str("/");
        connect_string.push_str(&pgdatabase);
    }
    connect_string
}

fn connect(connect_string: String, initialization: u8, thread_id: u32) -> Result<Client, postgres::Error> {

    let mut client: Client;
    loop {
        match Client::connect(connect_string.as_str(), tls::NoTls) {
            Ok(my_conn) => client = my_conn,
            Err(_) => {
                //println!("Error: {}", &err);
                continue;
            },
        };
        break;
    }

    if initialization == 1 {
        client.execute("create temporary table my_temp_table (id oid)", &[])?;
        client.execute("insert into my_temp_table values($1)", &[&thread_id])?;
    } else if initialization == 2 {
        client.execute(&format!("create table if not exists my_table_{} (id oid)", thread_id), &[])?;
        client.execute(&format!("truncate my_table_{}", thread_id), &[])?;
        client.execute(&format!("insert into my_table_{} values($1)", thread_id), &[&thread_id])?;
    }

    Ok(client)
}

fn reconnect(connect_string: &String, initialization: u8, thread_id: u32) -> Client {

    let client: Client;
    loop {
        match connect(connect_string.clone(), initialization, thread_id) {
            Ok(my_client) => client = my_client,
            Err(_) => {
                //println!("Error: {}", &err);
                continue;
            },
        };
        break;
    }

    client
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

fn thread_procedure(thread_id: u32, tx: mpsc::Sender<u64>, thread_lock: std::sync::Arc<std::sync::RwLock<bool>> ) -> Result<(), Box<dyn std::error::Error>>{
    // println!("Thread {} started", thread_id);
    let args = parse_args()?;

    let qtype: String = args.value_of("query_type")?;
    let stype: String = args.value_of("statement_type")?;
    let query: String;
    match qtype.as_ref() {
        "empty" => query = "".to_string(),
        "simple" => query = "SELECT $1".to_string(),
        "temp_read" => query = "SELECT ID from my_temp_table WHERE ID = $1".to_string(),
        "temp_write" => query = "UPDATE my_temp_table set ID = $1 WHERE ID = $1".to_string(),
        "read" => query = format!("SELECT ID from my_table_{} WHERE ID = $1", thread_id).to_string(),
        "write" => query = format!("UPDATE my_table_{} set ID = $1 WHERE ID = $1", thread_id).to_string(),
        _ => panic!("Option QTYPE should be one of empty, simple, read, write (not {}).", qtype),
    }

    let connect_string = postgres_connect_string(args);
    if thread_id == 0 {
        println!("Connectstring: {}", connect_string);
        println!("Query: {}", query);
        println!("SType: {}", stype);
    }
    let mut tps: u64 = 1000;
    let mut initialization: u8 = 0;

    if qtype == "temp_read" || qtype == "temp_write" {
        initialization = 1;
    } else if qtype == "read" || qtype == "write" {
        initialization = 2;
    }

    let mut conn: Client;
    let mut num_queries: u64 = 0;
    //Sleep 100 milliseconds
    let sleeptime = std::time::Duration::from_millis(100);
    conn = reconnect(&connect_string, initialization, thread_id);

    loop {
        if let Ok(done) = thread_lock.read() {
            // done is true when main thread decides we are there
            if *done {
                break;
            }
        }
        let start = Utc::now().naive_utc();
        match sample(&mut conn, &query, tps, &stype, thread_id) {
            Ok(sample_tps) => {
                tx.send(sample_tps)?;
                num_queries = sample_tps;
            },
            Err(_) => {
                //println!("Error: {}", &err);
                thread::sleep(sleeptime);
                conn = reconnect(&connect_string, initialization, thread_id);
            },
        };
        let end = Utc::now().naive_utc();
        tps = (num_queries as f32 / duration(start, end)) as u64;
    }
    Ok(())
}

fn downscale(rx: mpsc::Receiver<u64>, tx: mpsc::Sender<u64>, thread_lock: std::sync::Arc<std::sync::RwLock<bool>>) -> Result<(), Box<dyn std::error::Error>>{
    //With more threads (> 500) we have some issues, where the one main thread cannot consume messages fast enough.
    //This function can downscal from 25 messages to 1 message.
    let mut sum: u64 = 0;
    let wait = Duration::from_millis(10);
    loop {
        match thread_lock.read() {
            Ok(done) => {
                if *done {
                        break;
                }
            },
            Err(_err) => (),
        };
        for _ in 0..25 {
            match rx.recv_timeout(wait) {
                Ok(sample_tps) => {
                    sum += sample_tps;
                },
                Err(_err) => (),
            };
        }
        match tx.send(sum) {
            Ok(_) => sum = 0,
            Err(_err) => (),
        };
    }
    Ok(())
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
    let num_threads: u32 = 0;
    let (min_threads, max_threads) = args.range_min_max();
    let mut threads = Vec::with_capacity(max_threads as usize);
    if num_threads < 200 {
        for thread_id in 0..num_threads {
            let thread_tx = tx.clone();
            let thread_lock = rw_lock.clone();
            let thread_handle =  thread::Builder::new().name(format!("child{}", thread_id).to_string()).spawn(move || {
                thread_procedure(thread_id, thread_tx, thread_lock).unwrap();
            }).unwrap();
            threads.push(thread_handle);
        }
        num_samples = num_threads / 10;
    } else {
        let (tmp_tx, tmp_rx) = mpsc::channel();
        #[allow(unused_assignments)]
        let mut downscale_rx: mpsc::Receiver<u64> = tmp_rx;
        let mut downscale_tx: mpsc::Sender<u64> = tmp_tx;
        for thread_id in 0..num_threads {
            if thread_id % 100 == 0 {
                let (tmp_tx, tmp_rx) = mpsc::channel();
                downscale_rx = tmp_rx;
                downscale_tx = tmp_tx;
                let thread_lock = rw_downscaler_lock.clone();
                let thread_tx = tx.clone();
                let thread_handle =  thread::Builder::new().name(format!("downscale{}", thread_id).to_string()).spawn(move || {
                    downscale(downscale_rx, thread_tx, thread_lock).unwrap();
                }).unwrap();
                downscale_threads.push(thread_handle);
            }
            let thread_tx = downscale_tx.clone();
            let thread_lock = rw_lock.clone();
            let thread_handle =  thread::Builder::new().name(format!("child{}", thread_id).to_string()).spawn(move || {
                thread_procedure(thread_id, thread_tx, thread_lock).unwrap();
            }).unwrap();
            threads.push(thread_handle);
        }
        num_samples = num_threads / 250;
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

    println!("Date       time (sec)      | Sample period |          Threads         |              Postgres         |");
    println!("                           |               | Average TPS | Total TPS  |        tps   |          wal/s |");
    //        2019-06-24 11:33:23.437502       1.018000      105.090     10508.950      16888.312            0.000

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

    let main_lock = rw_lock.clone();
    if let Ok(mut done) = main_lock.write() {
        *done = true;
    }

    wait = num_threads * wait / 10;

    println!("Lets give the threads some time to stop");
    thread::sleep(wait);

    println!("Finished");
    ::std::process::exit(0);

    //Ok(())
}
