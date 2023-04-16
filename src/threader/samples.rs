use chrono::{Duration,DateTime,Utc};

// A sample is one thread trying to run as many transactions as possible
// for 100msec and keeping track of results
pub struct Sample {
    transactions: u32,
    wait: Duration,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
}

impl Sample {
    // initialize a new sample with no data
    pub fn new() -> Sample {
        Sample{
            transactions: 0,
            wait: Duration::zero(),
            start: chrono::Utc::now(),
            end: chrono::Utc::now(),
        }
    }
    // start sampling
    pub fn start(&self) {
        self.start = chrono::Utc::now();
    }
    // add a transaction (with the duration of it)
    pub fn increment(&self, wait: Duration) {
        self.transactions += 1;
        self.wait = self.wait + wait;
    }
    // stop sampling
    pub fn end(&self) {
        self.end = chrono::Utc::now();
    }
    // how many transactions did we process per second
    pub fn tps(self) -> f64 {
        let mut duration: f64 = (self.end-self.start).num_nanoseconds().unwrap() as f64;
        duration = duration / 1_000_000_000_f64;
        f64::from(self.transactions) / duration
    }
    // how many seconds did we waited for a transaction to return
    pub fn waits(self) -> Duration {
        self.end-self.start
    }
    // what latency did we perceive (on average)
    pub fn avg_latency(self) -> Duration {
        let num = self.transactions as i32;
        (self.end-self.start)/num
    }
    // You can materialize a Sample into A Samples struct
    pub fn to_samples(self) -> Samples {
        Samples{
            total_transactions: self.transactions,
            total_waits: self.wait,
            total_duration: self.end - self.start,
            num_samples: 1,
        }
    }
}

pub struct Samples {
    total_transactions: u32,
    total_waits: Duration,
    total_duration: Duration,
    num_samples: u32,
}

impl Samples {
    // initialize a new without data
    pub fn new() -> Samples {
        Samples{
            total_transactions: 0,
            total_waits: Duration::zero(),
            total_duration: Duration::zero(),
            num_samples: 0,
        }
    }
    // Combine two Samples (same time slice, different threads) into one
    pub fn add(&self, samples: Samples) {
        self.total_transactions += samples.total_transactions;
        self.total_waits = self.total_waits + samples.total_waits;
        self.total_duration = self.total_duration + samples.total_duration;
        self.num_samples += samples.num_samples;
    }
    // tot_tps is a sum of all tps's from all samples expecting they where
    // running in tandem on a single threads
    pub fn tot_tps_singlethread(self) -> f64 {
        let mut duration: f64 = self.total_duration.num_nanoseconds().unwrap() as f64;
        duration = duration / 1_000_000_000_f64;
        f64::from(self.total_transactions) / duration
    }
    // tot_tps is a sum of all tps's from all samples expecting they where
    // running simultaneously on seperate threads
    pub fn tot_tps_multithread(self) -> f64 {
        let num_samples = self.num_samples as i32;
        let mut duration: f64 = (self.total_duration / num_samples)
            .num_nanoseconds().unwrap() as f64;
        duration = duration / 1_000_000_000_f64;
        f64::from(self.total_transactions) / duration
    }
    // avg latency is the average amount of waits that one transaction took
    pub fn avg_latency(self) -> Duration {
        let num_transactions = self.total_transactions as i32;
        self.total_waits/num_transactions
    }
}
