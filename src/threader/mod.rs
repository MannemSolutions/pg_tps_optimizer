use std::sync::{mpsc, RwLock, Arc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

pub struct Threader {
    pub num_threads: u32,
    pub max_threads: u32,
    pub num_samples: u32,
    downscale: bool,
    tx: mpsc::Sender<u64>,
    rx: mpsc::Receiver<u64>,
    downscaler_lock: Arc<RwLock<bool>>,
    threads: Vec<JoinHandle<()>>,
}

impl Threader {
    pub fn get_args(mut max_threads: u32) -> Threader {
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
            num_threads: 0,
            max_threads,
            num_samples: 0,
            downscale,
            tx,
            rx,
            downscaler_lock,
            threads,
        }
    }
    pub fn rescale(&self, new_threads: u32)  {
        if self.downscale {
            let (tmp_tx, tmp_rx) = mpsc::channel();
            #[allow(unused_assignments)]
            let mut downscale_rx: mpsc::Receiver<u64> = tmp_rx;
            let mut downscale_tx: mpsc::Sender<u64> = tmp_tx;
            for thread_id in self.num_threads..new_threads {
                if thread_id % 100 == 0 {
                    let (tmp_tx, tmp_rx) = mpsc::channel();
                    downscale_rx = tmp_rx;
                    downscale_tx = tmp_tx;
                    let thread_lock = self.downscaler_lock.clone();
                    let thread_tx = self.tx.clone();
                    let thread_handle =  thread::Builder::new().name(format!("downscale{}", thread_id).to_string()).spawn(move || {
                        downscale(downscale_rx, thread_tx, thread_lock).unwrap();
                    }).unwrap();
                    self.threads.push(thread_handle);
                }
                let thread_tx = downscale_tx.clone();
                let thread_lock = self.downscaler_lock.clone();
                let thread_handle =  thread::Builder::new().name(format!("child{}", thread_id).to_string()).spawn(move || {
                    thread_procedure(thread_id, thread_tx, thread_lock).unwrap();
                }).unwrap();
                self.threads.push(thread_handle);
            }
            self.num_threads = new_threads;
            self.num_samples = new_threads / 250;
        } else {
            for thread_id in self.num_threads..new_threads {
                let thread_tx = self.tx.clone();
                let thread_lock = self.downscaler_lock.clone();
                let thread_handle =  thread::Builder::new().name(format!("child{}", thread_id).to_string()).spawn(move || {
                    thread_procedure(thread_id, thread_tx, thread_lock).unwrap();
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

        let wait = self.num_threads * Duration::from_millis(100) / 10;

        thread::sleep(wait);
    }
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

