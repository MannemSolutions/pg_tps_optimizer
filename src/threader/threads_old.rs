use std::sync::mpsc;
use crate::dsn;
use crate::threader::samples::Sample;
use chrono::Utc;
use std::thread;
use postgres::Client;

pub struct Thread {
    id: u32,
    tx: mpsc::Sender<Sample>,
    thread_lock: std::sync::Arc<std::sync::RwLock<bool>>,
    query: String,
    stype: String,
    dsn: dsn::Dsn,
}

impl Thread {
    fn procedure(self) -> Result<(), Box<dyn std::error::Error>>{

        if self.id == 0 {
            println!("Query: {}", self.query);
            println!("SType: {}", self.stype);
        }
        let mut tps: u64 = 1000;

        //Sleep 100 milliseconds
        let client = self.dsn.client();

        loop {
            if let Ok(done) = self.thread_lock.read() {
                // done is true when main thread decides we are there
                if *done {
                    break;
                }
            }
            match sample(&mut client, &self.query, tps/10, &self.stype, self.id) {
                Ok(sample) => {
                    tps = sample.tps() as u64;
                    self.tx.send(sample)?;
                },
                Err(_) => {
                    //println!("Error: {}", &err);
                    let sleeptime = std::time::Duration::from_millis(100);
                    thread::sleep(sleeptime);
                    client = self.dsn.client();
                },
            };
        }
        Ok(())
    }
}


fn sample(client: &mut Client, query: &String, num_queries: u64, stype: &String,
          thread_id: u32) -> Result<Sample, postgres::Error> {
    if num_queries < 1 {
        num_queries = 1;
    }
    let s = Sample::new();

    for _x in 1..num_queries {
        let start = Utc::now();
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
        s.increment(Utc::now()-start);
    }
    s.end();

    Ok(s)
}

