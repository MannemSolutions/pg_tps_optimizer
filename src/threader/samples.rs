/*
This module helps to go from one sample (one timeslice, single thread) to
a TestResult (TPS and average latency) once 'it is stable'.
Therefore we have multiple structures. How it works (and some definitions):

* Every thread tries to run as many queries as it can in 200ms and keeps track of duration,
  number of transactions and total waits (from run query to get results). With this:
  * The struct where we track duration, tps and waits is called a Sample.
  * A period of 200ms is called a timeslice. Every Sample belongs to a specific timeslice.
* When it has finished a Sample, it is sent to the master thread with a MPSC channel.
* The master thread collects all samples and combines them into a ParallelSample.
  A ParallelSample is a container for all Samples of a specific timeslice combined.
  As such a ParallelSample can be seen as the total performance on that timeslice.
  It only holds the number of Samples, sum of transactions, sum of waits and sum of duration.
  When the program runs 20 threads, 1000 TPS and latency of 10ms,
  the ParallelSample would look something like
  ParallelSample{
    timeslice: 8414426000, # This is the first timeslice of April 30th, 2023 at 22:06:39
    total_transactions: 200, # 1000TS is 200 transactions in 200ms
    total_waits: 2000ms, # For 200 transactions with latency 10ms, we expect total of 2000ms
    total_duration: 4000ms, # We expect 200ms has elapsed for 20 threads, so total of 4000ms
    num_samples: 20, # For 20 threads we expect 20 samples
    }
* ParallelSamples can collect data (Samples can be added until we have all of them).
  Once we expect we have all of them, we freeze the info into a TestResult.
  A TestResult still holds a combination of all samples for a TimeSlice, but
  the link to the exact timeslice is left out. We keep multiple TestResults together
  and calculate standard deviation. Once we have enough samples, and stddev is
  within parameters, we return a summary (mean TPS and mean latency) as a final TestResult.
*/

use std::collections::BTreeMap;
use std::vec::Vec;

use chrono::{Duration,DateTime,Utc, TimeZone};


// A sample is one thread trying to run as many transactions as possible
// for 100msec and keeping track of results
pub struct Sample {
    transactions: u32,
    wait: Duration,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
}

fn timeslice(when: DateTime<Utc>) -> u32 {
    ((when - Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap()).num_milliseconds()/200) as u32
}

fn current_timeslice() -> u32 {
    timeslice(chrono::Utc::now())
}

impl Copy for Sample { }

impl Clone for Sample {
    fn clone(&self) -> Sample {
        *self
    }
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
    // timeslices are defined as 'pockets of 200 miliseconds'.
    // this function returns the numer of pockets since EPOCH
    pub fn timeslice(&self) -> u32 {
        timeslice(self.start)
        }
    // add a transaction (with the duration of it)
    pub fn increment(&mut self, wait: Duration) {
        self.transactions += 1;
        self.wait = self.wait + wait;
    }
    // stop sampling
    pub fn end(&mut self) {
        self.end = chrono::Utc::now();
    }
    // how many transactions did we process per second
    pub fn tps(self) -> f64 {
        let mut duration: f64 = (self.end-self.start).num_nanoseconds().unwrap() as f64;
        duration = duration / 1_000_000_000_f64;
        f64::from(self.transactions) / duration
    }
    /*
    // how many seconds did we waited for a transaction to return
    pub fn waits(self) -> Duration {
        self.end-self.start
    }
    // what latency did we perceive (on average)
    pub fn avg_latency(self) -> Duration {
        let num = self.transactions as i32;
        (self.end-self.start)/num
    }
    */
    // You can materialize a Sample into A ParallelSample struct
    pub fn to_multi_samples(self) -> ParallelSample {
        ParallelSample{
            timeslice: self.timeslice(),
            total_transactions: self.transactions,
            total_waits: self.wait,
            total_duration: self.end - self.start,
            num_samples: 1,
        }
    }
}

// ParallelSample are meant as a set of multiple samples within the same period
// run on multiple threads. For efficiency it has a totally different memory structure,
// which only has the summaries data from all added samples.
pub struct ParallelSample {
    pub timeslice: u32,
    total_transactions: u32,
    total_waits: Duration,
    total_duration: Duration,
    pub num_samples: u32,
}

impl Copy for ParallelSample { }

impl Clone for ParallelSample {
    fn clone(&self) -> ParallelSample {
        *self
    }
}


impl ParallelSample {
    /*
    // initialize a new without data
    pub fn new(timeslice: u32) -> ParallelSample {
        ParallelSample{
            timeslice,
            total_transactions: 0,
            total_waits: Duration::zero(),
            total_duration: Duration::zero(),
            num_samples: 0,
        }
    }
    */
    // Combine two ParallelSample (same time slice, different threads) into one
    pub fn add(&mut self, samples: ParallelSample) -> Result<(), &'static str>{
        if self.timeslice != samples.timeslice {
            return Err("trying to combine samples of different timeslices")
        }
        self.total_transactions += samples.total_transactions;
        self.total_waits = self.total_waits + samples.total_waits;
        self.total_duration = self.total_duration + samples.total_duration;
        self.num_samples += samples.num_samples;
        Ok(())
    }

    // tot_tps is a sum of all tps's from all samples expecting they where
    // running simultaneously on seperate threads
    pub fn tot_tps(&self) -> f64 {
        let num_samples = self.num_samples as i32;
        let mut duration: f64 = (self.total_duration / num_samples)
            .num_nanoseconds().unwrap() as f64;
        duration = duration / 1_000_000_000_f64;
        f64::from(self.total_transactions) / duration
    }
    // avg latency is the average amount of waits over all samples contained
    pub fn avg_latency(&self) -> f64 {
        (self.total_waits.num_microseconds().unwrap() as f64)/(self.total_transactions as f64)
    }
    pub fn as_testresult(&self) -> TestResult {
        TestResult{
            tps: self.tot_tps(),
            latency: self.avg_latency(),
        }
    }
}

pub struct ParallelSamples {
    samples: BTreeMap<u32, ParallelSample>,
}

impl Iterator for ParallelSamples {
    type Item = ParallelSample;
    fn next(&mut self) -> Option<Self::Item> {
        if self.samples.len() == 1 {
            return None;
        }
        match self.samples.pop_first() {
            Some((_, sample)) => Some(sample),
            _ => None,
        }
    }
}

impl ParallelSamples {
    // initialize a new without data
    pub fn new() -> ParallelSamples {
        ParallelSamples{
            samples: BTreeMap::new(),
        }
    }
    pub fn add(&mut self, sample: ParallelSample) {
        self.samples
            .entry(sample.timeslice)
            .and_modify(|s| { s.add(sample).unwrap() })
            .or_insert(sample);
    }
    pub fn append(mut self, samples: ParallelSamples) -> ParallelSamples {
        for (_, sample) in samples.samples {
            self.add(sample);
        }
        self
    }
    pub fn as_results(&self, min: usize, max: usize) -> TestResults {
        let previous_timeslice = current_timeslice()-1;
        let mut results = TestResults::new(min, max);
        for (_, sample) in self.samples.clone() {
            if sample.timeslice > previous_timeslice {
                break;
            }
            results.append(sample.as_testresult());
        }
        results
    }
}

pub struct TestResult {
    pub tps: f64,
    pub latency: f64,
}

impl TestResult {
    fn between_spread(&self, spread: f64) -> bool {
        if self.tps > spread || self.latency > spread {
            return false;
        }
        true
    }
}

pub struct TestResults {
    min: usize,
    max: usize,
    results: Vec<TestResult>,
}

impl TestResults {
    pub fn new(min: usize, max: usize) -> TestResults {
        TestResults{
            min,
            max,
            results: Vec::new(),
        }
    }
    fn mean(&self) -> Option<TestResult> {
        let sum_tps = self.results.iter().map(|tr| tr.tps).sum::<f64>() as f64;
        let sum_latency = self.results.iter().map(|tr| tr.latency).sum::<f64>() as f64;
        let count = self.results.len();

        match count {
            positive if positive > 0 => Some(
                TestResult {
                    tps: sum_tps/(count as f64),
                    latency: sum_latency/(count as f64)
                }
                ),
            _ => None,
        }
    }

    fn std_deviation(&self) -> Option<TestResult> {
        match (self.mean(), self.results.len()) {
            (Some(results), count) if count > 0 => {
                let tps_variance = self.results.iter().map(|tr| {
                    let tps_diff = results.tps -tr.tps;
                    tps_diff * tps_diff
                }).sum::<f64>() / count as f64;
                let lat_variance = self.results.iter().map(|tr| {
                    let lat_diff = results.latency -tr.latency;
                    lat_diff * lat_diff
                }).sum::<f64>() / count as f64;

                Some(TestResult{
                    tps: tps_variance.sqrt(),
                    latency: lat_variance.sqrt(),
                })
            },
            _ => None
        }
    }
    /*
    pub fn clear(&mut self) {
        self.results.clear();
    }
    */
    pub fn append(&mut self, result: TestResult) {
        self.results.insert(self.results.len(), result);
        if self.results.len() > self.max {
            self.results.remove(0);
        }
    }
    pub fn verify(&self, spread: f64) -> Option<TestResult> {
            if self.results.len() < self.min {
                return None
            }
            let stdev = self.std_deviation().unwrap();
            if stdev.between_spread(spread) {
                return Some(stdev);
            }
            None
    }
}
