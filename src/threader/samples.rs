use chrono::{Duration,DateTime,Utc};

// A sample is one thread trying to run as many transactions as possiblei
// in 100msec and keeping track in this struct.
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
}

// Samples is a collection of many samples processed by multiple threads
pub struct Samples {
    total_tps: f64,
    total_waits: Duration,
    num_transactions: u32,
}

impl Samples {
    // initialize a new without data
    pub fn new() -> Samples {
        Samples{
            total_tps: 0_f64,
            total_waits: Duration::zero(),
            num_transactions: 0_u32,
        }
    }
    // add a sample:
    // - tps is accumulated (expecting that all samples ran in parallel)
    // - waits is also accumulated (we waited this period on all transactions)
    // - transactions is also accumulated
    pub fn append(&self, sample: Sample) {
        self.total_tps += sample.tps();
        self.total_waits = self.total_waits + sample.waits();
        self.num_transactions += sample.transactions;
    }
    pub fn add(&self, samples: Samples) {
        self.total_tps += samples.total_tps;
        self.total_waits = self.total_waits + samples.total_waits;
        self.num_transactions += samples.num_transactions;
    }
    // tot_tps is a sum of all tps's from all samples
    pub fn tot_tps(self) -> f64 {
        self.total_tps
    }
    // all waits divided by num transactions is wait/transaction
    pub fn avg_latency(self) -> f64 {
        let num = self.num_transactions as f64;
        let mut waits: f64 = self.total_waits.num_nanoseconds().unwrap() as f64;
        waits / num / 1_000_000_000_f64
    }
}
