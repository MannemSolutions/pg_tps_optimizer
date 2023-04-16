use std::sync::{mpsc, RwLock, Arc};
use std::thread;
use std::thread::JoinHandle;
use crate::dsn;
use crate::threader::threads::Thread;
use crate::threader::samples::Samples;
use chrono::Utc;

mod threads;
mod samples;

pub struct Threader {
    pub num_threads: u32,
    pub max_threads: u32,
    pub num_samples: u32,
    downscale: bool,
    query: String,
    s_type: String,
    tx: mpsc::Sender<Samples>,
    rx: mpsc::Receiver<Samples>,
    downscaler_lock: Arc<RwLock<bool>>,
    threads: Vec<JoinHandle<()>>,
    dsn: dsn::Dsn,
}

impl Threader {
    pub fn new(mut max_threads: u32, query: String, s_type: String, dsn: dsn::Dsn) -> Threader {
        let downscale: bool;
        let downscaler_lock: Arc<RwLock<bool>>;
        if max_threads < 1 {
            max_threads = 1000
        }
        if max_threads > 200 {
            downscale = true;
            downscaler_lock = Arc::new(RwLock::new(false));
        }
        let (tx, rx) = mpsc::channel();
        let mut threads = Vec::with_capacity(max_threads as usize);
        Threader{
            query,
            s_type,
            num_threads: 0,
            max_threads,
            num_samples: 0,
            downscale,
            tx,
            rx,
            downscaler_lock,
            threads,
            dsn,
        }
    }
    pub fn rescale(&self, new_threads: u32)  {
        if self.downscale {
            let (tmp_tx, tmp_rx) = mpsc::channel();
            #[allow(unused_assignments)]
            let mut downscale_rx: mpsc::Receiver<Samples> = tmp_rx;
            let mut downscale_tx: mpsc::Sender<Samples> = tmp_tx;
            for thread_id in self.num_threads..new_threads {
                if thread_id % 100 == 0 {
                    let (tmp_tx, tmp_rx) = mpsc::channel();
                    downscale_rx = tmp_rx;
                    downscale_tx = tmp_tx;
                    let thread_lock = self.downscaler_lock.clone();
                    let thread_tx = self.tx.clone();
                    let thread_handle =  thread::Builder::new()
                        .name(format!("downscale{}", thread_id).to_string())
                        .spawn(move || {
                            downscale(downscale_rx, thread_tx, thread_lock).unwrap();
                        }).unwrap();
                    self.threads.push(thread_handle);
                }
                let thread_tx = downscale_tx.clone();
                let thread_lock = self.downscaler_lock.clone();
                let thread_handle = thread::Builder::new()
                    .name(format!("child{}", thread_id).to_string())
                    .spawn(move || {
                        Thread::new(thread_id,
                                    thread_tx,
                                    thread_lock,
                                    self.query.as_str(),
                                    self.s_type.as_str(),
                                    self.dsn.clone())
                            .procedure().unwrap();
                    }).unwrap();
                self.threads.push(thread_handle);
            }
            self.num_threads = new_threads;
            self.num_samples = new_threads / 250;
        } else {
            for thread_id in self.num_threads..new_threads {
                let thread_tx = self.tx.clone();
                let thread_lock = self.downscaler_lock.clone();
                let thread_handle =  thread::Builder::new()
                    .name(format!("child{}", thread_id).to_string())
                    .spawn(move || {
                    let t = Thread::new(thread_id,
                                        thread_tx,
                                        thread_lock,
                                        self.query.as_str(),
                                        self.s_type.as_str(),
                                        self.dsn.clone() );
                    t.procedure().unwrap();
                }).unwrap();
                self.threads.push(thread_handle);
            }
            self.num_samples = new_threads / 10;
        }
    }
    pub fn finish(&self) {
        let main_lock = self.downscaler_lock.clone();
        if let Ok(mut done) = main_lock.write() {
            *done = true;
        }

        let wait = self.num_threads * std::time::Duration::from_millis(100) / 10;

        thread::sleep(wait);
    }

    fn wait_stable(self, max_wait: i64) {
        let start_time = Utc::now();
        while (Utc::now() - start_time).num_seconds() < max_wait {
            let sum_trans = 0;
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
    }
}

fn downscale(rx: mpsc::Receiver<Samples>, tx: mpsc::Sender<Samples>, thread_lock: std::sync::Arc<std::sync::RwLock<bool>>) -> Result<(), Box<dyn std::error::Error>>{
    //With more threads (> 500) we have some issues, where the one main thread cannot consume messages fast enough.
    //This function can downscale from 25 messages to 1 message.
    let mut s = Samples::new();
    let wait = std::time::Duration::from_millis(10);
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
                Ok(samples) => {
                    s.add(samples);
                },
                Err(_err) => (),
            };
        }
        match tx.send(s) {
            Ok(_) => s = Samples::new(),
            Err(_err) => (),
        };
    }
    Ok(())
}

