use std::sync::mpsc;
use crate::threader::samples::Sample;
use chrono::Utc;
use std::thread;
use postgres::Client;

use super::workload::{Workload, WorkloadType};

pub struct Thread {
    id: u32,
    tx: mpsc::Sender<Sample>,
    thread_lock: std::sync::Arc<std::sync::RwLock<bool>>,
    workload: Workload,
}

impl Thread {
    pub fn new(id: u32,
               tx: mpsc::Sender<Sample>,
               thread_lock: std::sync::Arc<std::sync::RwLock<bool>>,
               workload: Workload,
               ) -> Thread {
        Thread {id, tx, thread_lock, workload}

    }
    pub fn procedure(self) -> Result<(), Box<dyn std::error::Error>>{

        if self.id == 0 {
            println!("{0}", self.workload.as_string());
        }
        let mut tps: f64 = 1000_f64;

        //Sleep 100 milliseconds
        let mut client = self.workload.client();

        loop {
            if let Ok(done) = self.thread_lock.read() {
                // done is true when main thread decides we are there
                if *done {
                    break;
                }
            }
            match sample(&mut client, self.workload.w_type(), tps/10_f64, self.id) {
                Ok(samples) => {
                    //tps = samples.tot_tps_singlethread() as u64;
                    self.tx.send(samples)?;
                    tps = samples.tps();
                },
                Err(_) => {
                    //println!("Error: {}", &err);
                    let sleeptime = std::time::Duration::from_millis(100);
                    thread::sleep(sleeptime);
                    client = self.workload.client();
                },
            };
        }
        Ok(())
    }
}


fn sample(client: &mut Client, w_type: WorkloadType, mut num_queries: f64, thread_id: u32) -> Result<Sample, postgres::Error> {
    if num_queries < 1_f64 {
        num_queries = 1_f64;
    }
    let mut s = Sample::new();
    let query = "";

    for _x in 1..(num_queries as u64) {
        let start = Utc::now();
        match w_type {
            WorkloadType::Prepared => {
                let prep = client.prepare(query)?;
                let _row = client.query(&prep, &[&thread_id]);
            }
            WorkloadType::Transactional => {
                let mut trans = client.transaction()?;
                if query != "" {
                    let _row = trans.query(query, &[&thread_id]);
                }
                let _res = trans.commit()?;
            }
            WorkloadType::PreparedTransactional => {
                let mut trans = client.transaction()?;
                if query != "" {
                    let prep = trans.prepare(&query)?;
                    let _row = trans.query(&prep, &[&thread_id]);
                }
                let _res = trans.commit()?;
            }
            WorkloadType::Default => {
                let _row = &client.query(query, &[&thread_id]);
            }
        }
        s.increment(Utc::now()-start);
    }
    s.end();

    Ok(s)
}
