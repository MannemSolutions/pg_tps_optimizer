use std::collections::HashMap;
use std::collections::hash_map::OccupiedEntry;
use std::sync::{mpsc, RwLock, Arc};
use std::thread;
use std::thread::JoinHandle;
use crate::dsn;
use crate::threader::threads::Thread;
use crate::threader::samples::{MultiSamples, Sample, current_timeslice};
use chrono::{Utc, Duration};

mod threads;
mod samples;

pub struct Threader {
    pub num_threads: u32,
    pub max_threads: u32,
    pub num_samples: u32,
    query: String,
    s_type: String,
    tx: mpsc::Sender<Sample>,
    rx: mpsc::Receiver<Sample>,
    thread_lock: Arc<RwLock<bool>>,
    threads: Vec<JoinHandle<()>>,
    dsn: dsn::Dsn,
    sliced_samples: HashMap<u32, MultiSamples>,
}


fn mean(data: Vec<f64>) -> Option<f64> {
    let sum = data.iter().sum::<f64>() as f64;
    let count = data.len();

    match count {
        positive if positive > 0 => Some(sum / count as f64),
        _ => None,
    }
}

fn std_deviation(data: Vec<f64>) -> Option<f64> {
    match (mean(data), data.len()) {
        (Some(data_mean), count) if count > 0 => {
            let variance = data.iter().map(|value| {
                let diff = data_mean - (*value as f64);

                diff * diff
            }).sum::<f64>() / count as f64;

            Some(variance.sqrt())
        },
        _ => None
    }
}

impl Threader {
    pub fn new(mut max_threads: u32, query: String, s_type: String, dsn: dsn::Dsn) -> Threader {
        if max_threads < 1 {
            max_threads = 1000
        }
        let thread_lock = Arc::new(RwLock::new(false));
        let (tx, rx) = mpsc::channel();
        let mut threads = Vec::with_capacity(max_threads as usize);
        Threader{
            query,
            s_type,
            num_threads: 0,
            max_threads,
            num_samples: 0,
            tx,
            rx,
            thread_lock,
            threads,
            dsn,
            sliced_samples: HashMap::new(),
        }
    }
    pub fn rescale(&self, new_threads: u32)  {
        let mut thread_lock: Arc<RwLock<bool>>;
        let (thread_tx, rx) = mpsc::channel();
        let mut thread_handle: JoinHandle<()>;
        for thread_id in self.num_threads..new_threads {
            thread_tx = self.tx.clone();
            thread_lock = self.thread_lock.clone();
            let d = self.dsn.clone();
            let q = self.query.as_str();
            let t = self.s_type.as_str();
            thread_handle =  thread::Builder::new()
                .name(format!("child{}", thread_id).to_string())
                .spawn(move || {
                    Thread::new(thread_id,
                                thread_tx,
                                thread_lock,
                                q,
                                t,
                                d)
                        .procedure().unwrap();
                }).unwrap();
            self.threads.push(thread_handle);
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

    fn wait_stable(&self, spread: f64, count: usize, max_wait: Duration) {
        let timeslice = current_timeslice();
        let current_mult_sample: MultiSamples = MultiSamples::new(timeslice);
        let end_time = Utc::now()+max_wait;
        let tps_list: Vec<f64>;
        let latency_list: Vec<f64>;
        while Utc::now() < end_time {
            let multisample = self.consume();
            self.sliced_samples
                .entry(multisample.timeslice)
                .and_modify(|s| s.add(multisample).unwrap())
                .or_insert(multisample);
            let current_mult_sample = self.sliced_samples
                .entry(timeslice)
                .or_insert(current_mult_sample);
            if current_mult_sample.num_samples < self.num_samples {
                tps_list.clear();
                latency_list.clear();
                continue;
            }
            let tps = current_mult_sample.tot_tps();
            tps_list.insert(tps_list.len(), tps);
            let latency = current_mult_sample.avg_latency();
            latency_list.insert(latency_list.len(), latency.num_microseconds().unwrap() as f64);
            if tps_list.len() < count {
                continue
            }
            if tps_list.len() > count {
                tps_list.remove(0);
                latency_list.remove(0);
            }

            if std_deviation(tps_list).unwrap() < spread && std_deviation(latency_list).unwrap() < spread {
            }

        }
    }

    fn consume(self) -> MultiSamples{
        //With more threads (> 500) we have some issues, where the one main thread cannot consume messages fast enough.
        //This function can downscale from 25 messages to 1 message.
        let mut s = MultiSamples::new();
        let wait = std::time::Duration::from_millis(10);
        loop {
            match self.thread_lock.read() {
                Ok(done) => {
                    if *done {
                        break;
                    }
                },
                Err(_err) => (),
            };
            for _ in 0..25 {
                match self.rx.recv_timeout(wait) {
                    Ok(samples) => {
                        s.add(samples.to_multi_samples());
                    },
                    Err(_err) => (),
                };
            }
        }
            s
    }
}


