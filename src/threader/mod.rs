use crate::threader::sample::{ParallelSamples, TestResult};
use crate::threader::workload::Workload;
use crate::threader::consumer::{Consumer, THREADS_PER_CONSUMER};
use chrono::{Duration, Utc};
use std::sync::{mpsc, Arc, RwLock};
use std::thread;

mod sample;
mod worker;
mod consumer;
pub mod workload;

pub struct Threader {
    pub num_workers: usize,
    pub max_workers: usize,
    //pub num_samples: u32,
    workload: Workload,
    tx: mpsc::Sender<ParallelSamples>,
    rx: mpsc::Receiver<ParallelSamples>,
    done: Arc<RwLock<bool>>,
    consumers: Vec<Consumer>,
}

impl Threader {
    pub fn new(mut max_workers: usize, workload: Workload) -> Threader {
        if max_workers < 1 {
            max_workers = 1000
        }
        max_workers /= THREADS_PER_CONSUMER as usize;
        max_workers += 1;
        let done = Arc::new(RwLock::new(false));
        let (tx, rx) = mpsc::channel();
        let consumers = Vec::with_capacity(max_workers as usize);
        Threader {
            workload,
            num_workers: 0,
            max_workers,
            //num_samples: 0,
            tx,
            rx,
            done,
            consumers
        }
    }
    pub fn scaleup(&mut self, new_workers: u32) {
        let mut extra_workers = new_workers - self.num_workers as u32;
        //println!("New worker: {}, extra workers: {}", new_workers, extra_workers);
        match self.consumers.pop() {
            Some(mut last_consumer) => {
                extra_workers = last_consumer.scaleup(
                    extra_workers,
                    self.done.clone(),
                    self.workload.clone());
                self.consumers.push(last_consumer);
            }
            None => (),
        }
        for id in self.consumers.len()..self.max_workers {
            if extra_workers == 0 {
                break;
            }
            let mut new_consumer = Consumer::new(id as u32, self.tx.clone());
            extra_workers = new_consumer.scaleup(
                extra_workers,
                self.done.clone(),
                self.workload.clone(),
                );
            self.consumers.push(new_consumer);
        }
        self.num_workers = new_workers as usize;
    }
    pub fn finish(&self) {
        if let Ok(mut done) = self.done.clone().write() {
            *done = true;
        }

        let wait = self.num_workers as u32 * std::time::Duration::from_millis(100) / 10;

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
            parallel_samples = parallel_samples.append(&s);
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
        let wait = std::time::Duration::from_millis(10);
        let timeout = std::time::SystemTime::now() + std::time::Duration::from_millis(200);
        let mut parallel_samples = ParallelSamples::new();

        match self.done.read() {
            Ok(_done) => (),
            Err(_err) => (),
        };
        loop {
            //               println!("looping");
            match self.rx.recv_timeout(wait) {
                Ok(pss) => {
                    //        println!("adding");
                    parallel_samples = parallel_samples.append(&pss);
                }
                Err(_err) => (),
            };
            if std::time::SystemTime::now() > timeout {
                break;
            }
        }
        return parallel_samples;
    }
}
