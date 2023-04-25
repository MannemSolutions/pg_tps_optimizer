use postgres::{Client, Statement, Error};
use crate::dsn::Dsn;
use chrono::Utc;

const SAMPLE_QUERY: &str = "
SELECT now()::timestamp as samplemmoment,
pg_current_wal_lsn()::varchar as lsn,
(pg_current_wal_lsn() - $1::varchar::pg_lsn)::real as walbytes,
(select sum(xact_commit+xact_rollback)::real
 FROM pg_stat_database) as transacts";


// This struct can run a query against postgres and see
pub struct PgSampler {
    client: Client,
    statement: Statement,
    previous: TransactDataSample,
    latest: TransactDataSample,
}


impl PgSampler {
    pub fn new(dsn: Dsn) -> Result<PgSampler, Error> {
        let mut client: Client = dsn.client();
        let statement: Statement = client.prepare(SAMPLE_QUERY)?;
        Ok(PgSampler {
            client,
            statement,
            previous: TransactDataSample::new() ,
            latest: TransactDataSample::new() ,
        })
    }
    pub fn next(&mut self) -> Result<(), Error> {
        let rows = self.client.query(&self.statement, &[&self.previous.lsn])?;
        assert_eq!(rows.len(), 1);
        let row = rows.get(0).unwrap();
        self.previous = self.latest.clone();
        self.latest = TransactDataSample {
            samplemoment: row.get(0),
            lsn: row.get(1),
            wal_bytes: row.get(2),
            num_transactions: row.get(3),
        };
        Ok(())
    }
    pub fn duration(&self) -> f32 {
        (self.latest.samplemoment - self.previous.samplemoment).num_nanoseconds().unwrap() as f32 / 10.0_f32.powi(9)
    }
    pub fn wal_per_sec(&self) -> f32 {
        (self.latest.wal_bytes - self.previous.wal_bytes) / self.duration()
    }
    pub fn tps(&self) -> f32 {
        (self.latest.num_transactions - self.previous.num_transactions) / self.duration()
    }
}

struct TransactDataSample {
    samplemoment: chrono::NaiveDateTime,
    lsn: String,
    wal_bytes: f32,
    num_transactions: f32,
}

impl TransactDataSample {
    fn new() -> TransactDataSample {
        TransactDataSample {
            samplemoment: Utc::now().naive_utc(),
            lsn: "0/0".to_string(),
            wal_bytes: 0.0_f32,
            num_transactions: 0.0_f32,
        }
    }
    fn clone(&self) -> TransactDataSample {
        TransactDataSample {
            samplemoment: self.samplemoment,
            lsn: self.lsn.clone(),
            wal_bytes: self.wal_bytes,
            num_transactions: self.num_transactions,
        }
    }
}
