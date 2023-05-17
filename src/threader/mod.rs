use crate::threader::samples::{ParallelSamples, Sample, TestResult};
use crate::threader::threads::Thread;
use crate::threader::workload::Workload;
use chrono::{Duration, Utc};
use std::sync::{mpsc, Arc, RwLock};
use std::thread;

mod samples;
mod threads;
pub mod workload;

pub struct Threader {
    pub num_threads: u32,
    pub max_threads: u32,
    pub num_samples: u32,
    workload: Workload,
    tx: mpsc::Sender<Sample>,
    rx: mpsc::Receiver<Sample>,
    thread_lock: Arc<RwLock<bool>>,
    threads: Vec<thread::JoinHandle<()>>,
}

impl Threader {
    pub fn new(mut max_threads: u32, workload: Workload) -> Threader {
        if max_threads < 1 {
            max_threads = 1000
        }
        let thread_lock = Arc::new(RwLock::new(false));
        let (tx, rx) = mpsc::channel();
        let threads = Vec::with_capacity(max_threads as usize);
        Threader {
            workload,
            num_threads: 0,
            max_threads,
            num_samples: 0,
            tx,
            rx,
            thread_lock,
            threads,
        }
    }
    pub fn scaleup(&mut self, new_threads: u32) {
        let mut thread_lock: Arc<RwLock<bool>>;
        let mut thread_handle: thread::JoinHandle<()>;
        for thread_id in self.num_threads..new_threads {
            let thread_tx = self.tx.clone();
            thread_lock = self.thread_lock.clone();
            let workload: Workload = self.workload.clone();
            thread_handle = thread::Builder::new()
                .name(format!("child{}", thread_id).to_string())
                .spawn(move || {
                    Thread::new(thread_id, thread_tx, thread_lock, workload)
                        .procedure()
                        .unwrap();
                })
                .unwrap();
            self.threads.push(thread_handle);
            thread::sleep(std::time::Duration::from_millis(10));
        }
        self.num_threads = new_threads;
        self.num_samples = self.num_threads / 10;
    }
    pub fn finish(&self) {
        let main_lock = self.thread_lock.clone();
        if let Ok(mut done) = main_lock.write() {
            *done = true;
        }

        let wait = self.num_threads * std::time::Duration::from_millis(100) / 10;

        thread::sleep(wait);
    }

    pub fn wait_stable(
        &mut self,
        spread: f64,
        count: usize,
        max_wait: Duration,
    ) -> Option<TestResult> {
        let end_time = Utc::now() + max_wait;
        let mut parallel_samples = ParallelSamples::new();
        let i: usize = 0;
        loop {
            if i > count && Utc::now() > end_time {
                break;
            }
            let s = self.consume();
            parallel_samples = parallel_samples.append(s);
            let test_result = parallel_samples.as_results(count, count + 1);
            //            let stddev = test_result.std_deviation_absolute().unwrap();
            //            println!("tps: {}, latency: {}", stddev.tps, stddev.latency);
            match test_result.verify(spread) {
                Some(test_result) => {
                    return Some(test_result);
                }
                None => {
                    continue;
                }
            }
        }
        None
    }

    fn consume(&mut self) -> ParallelSamples {
        //With more threads (> 500) we have some issues, where the one main thread cannot consume messages fast enough.
        //This function can downscale from 25 messages to 1 message.
        let wait = std::time::Duration::from_millis(10);
        let timeout = std::time::SystemTime::now() + std::time::Duration::from_millis(200);
        let mut parallel_samples = ParallelSamples::new();

        match self.thread_lock.read() {
            Ok(_done) => (),
            Err(_err) => (),
        };
            loop {
 //               println!("looping");
                match self.rx.recv_timeout(wait) {
                    Ok(samples) => {
//        println!("adding");
                        parallel_samples.add(samples.to_parallel_sample());
                    }
                    Err(_err) => (),
                };
                        if std::time::SystemTime::now() > timeout {
                            break;
                }
            }
            //println!("{}", parallel_samples
            //         .clone()
            //         .into_iter()
            //         .map(|s| s.avg_latency().num_milliseconds() as f64)
            //         .sum::<f64>());
        return parallel_samples;
    }
}
