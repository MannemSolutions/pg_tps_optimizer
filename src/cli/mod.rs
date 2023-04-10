use crate::generic;
use structopt::StructOpt;
use regex;

/// Search for a pattern in a file and display the lines that contain it.

#[derive(StructOpt)]
#[structopt(about = "I detect maximum TPS with minimal latency. Pass `-h` for more info.")]
pub struct Params {
    /// Connection string
    #[structopt(default_value, short, long, help = "the DSN to connect to (or use env vars PG...)")]
    pub dsn: String,

    /// Query
    #[structopt(default_value, short, long, help = "the query to run")]
    pub query: String,

    /// Prepared queries
    #[structopt(short, long, help = "you can run prepared statements, or run direct statements")]
    #[structopt(long)]
    pub prepare: bool,

    /// Transactional workload
    #[structopt(short, long, help = "you can run inside of a transaction or direct")]
    pub transactional: bool,

    /// Testrange
    #[structopt(short, long, help = "you can set min and max of numclients if you know (default 1:1000)")]
    pub range: String,
}

impl Params {
    fn from_args() -> Params {
        <Params as StructOpt>::from_args()
    }
    pub fn get_args() -> Params {
        let mut args = Params::from_args();
        args.dsn = generic::get_env_str(
            &args.dsn,
            &String::from("PGTPSSOURCE"),
            &String::from(""),
        );
        args.query = generic::get_env_str(
            &args.query,
            &String::from("PGTPSQUERY"),
            &String::from("select * from pg_tables"),
        );
        args.prepare = generic::get_env_bool(
            args.prepare,
            &String::from("PGTPSPREPARED"),
        );
        args.transactional = generic::get_env_bool(
            args.transactional,
            &String::from("PGTPSTRANSACTIONAL"),
        );
        args.range = generic::get_env_str(
            &args.range,
            &String::from("PGTPSRANGE"),
            &String::from("1:1000"),
        );
        args
    }
    pub fn range_min_max(self) -> (u32, u32) {
        let re = regex::Regex::new(r"\d+)").unwrap();
        let values: Vec<u32> = re.find_iter(self.range.as_str())
            .filter_map(|digits| ( digits.as_str().parse().ok()))
            .collect();
        match values.len(){
            0=>(1,1000),
            1=>(1,values[0]),
            _=>(values[0], values[values.len()-1])
        }
    }
}
