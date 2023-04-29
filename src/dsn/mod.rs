use std::borrow::Borrow;
use crate::generic;
use std::collections::HashMap;
use users::{get_current_uid, get_user_by_uid};
use openssl::ssl::{SslConnector, SslFiletype, SslMethod};
use postgres_openssl::MakeTlsConnector;
use postgres::{Client, NoTls};

#[derive(Debug, Clone)]
pub struct Dsn {
    kv: HashMap<String, String>,
    ssl_mode: String,
}

fn shell_expand(path: &str) -> String {
    shellexpand::tilde(path).to_string()
}

impl Dsn {
    pub fn from_string(from: &str) -> Dsn {
        let mut dsn = Dsn::new();
        let split = from.split(" ");
        for s in split {
            if let Some((key, value)) = s.split_once("=") {
                dsn.set_value(key, value)
            }
        }
        dsn
    }
    pub fn copy(&self) -> Dsn {
        let mut kv: HashMap<String, String> = HashMap::new();
        for (k, v) in self.kv.borrow() {
            kv.insert(k.to_string(), v.to_string());
        }
        Dsn { kv, ssl_mode: self.ssl_mode.to_string() }
    }
    pub fn cleanse(&self) -> Dsn {
        let mut kv: HashMap<String, String> = HashMap::new();
        kv.extend(self.clone().kv);
        kv.remove("sslmode");
        kv.remove("sslcert");
        kv.remove("sslkey");
        kv.remove("sslrootcert");
        kv.remove("sslcrl");
        let ssl_mode = "disable".to_string();
        Dsn { kv, ssl_mode }
    }
    pub fn new() -> Dsn {
        let mut kv: HashMap<String, String> = HashMap::new();

        let mut user = generic::get_env_str("", "PGUSER", "").to_string();
        if user.is_empty() {
            user = match get_user_by_uid(get_current_uid()).unwrap().name().to_str() {
                Some(osuser) => osuser.to_string(),
                None => "".to_string(),
            };
        }
        kv.insert("user".to_string(), user.to_string());
        kv.insert(
            "dbname".to_string(),
            generic::get_env_str("", "PGDATABASE", user.as_str()),
        );
        kv.insert(
            "host".to_string(),
            generic::get_env_str("", "PGHOST", "/tmp"),
        );
        let ssl_mode = generic::get_env_str("", "PGSSLMODE", "prefer");
        kv.insert(
            "sslmode".to_string(),
            ssl_mode.to_string(),
        );
        kv.insert(
            "sslcert".to_string(),
            generic::get_env_str("", "PGSSLCERT", "~/.postgresql/postgresql.crt"),
        );
        kv.insert(
            "sslkey".to_string(),
            generic::get_env_str("", "PGSSLKEY", "~/.postgresql/postgresql.key"),
        );
        kv.insert(
            "sslrootcert".to_string(),
            generic::get_env_str("", "PGSSLROOTCERT", "~/.postgresql/root.crt"),
        );
        kv.insert(
            "sslcrl".to_string(),
            generic::get_env_str("", "PGSSLCRL", "~/.postgresql/root.crl"),
        );
        Dsn { kv, ssl_mode }
    }
    pub fn to_string(&self) -> String {
        let mut vec = Vec::new();
        for (k, v) in self.clone().kv {
            vec.push(format!("{0}={1}", k, v))
        }
        vec.sort();
        vec.join(" ")
    }
    fn set_value(&mut self, key: &str, value: &str) {
        self.kv.insert(key.to_string(), value.to_string());
        if key.eq("sslmode") {
            self.ssl_mode = value.to_string()
        }
    }
    fn get_value(&self, key: &str, default: &str) -> String {
        match self.kv.get_key_value(key) {
            Some(kv) => {
                let (k, v) = kv;
                if key.eq(k) {
                    return v.to_string();
                }
            }
            None => return default.to_string()
        }
        default.to_string()
    }
    pub fn use_tls(&self) -> bool {
        self.ssl_mode.ne("disable")
    }
    pub fn verify_hostname(&self) -> bool {
        self.ssl_mode.eq("verify-full")
    }
    pub fn client(self) -> Client {
        let copy = self.cleanse().to_string();
        let conn_string = copy.as_str();
        if ! self.copy().use_tls() {
            println!("not using tls");
            let client = postgres::Client::connect(conn_string, NoTls).unwrap();
            // The source_connection object performs the actual communication
            // with the database, so spawn it off to run on its own.
            return client
        }
        let mut builder = match SslConnector::builder(SslMethod::tls()) {
            Ok(value) => value,
            Err(error) => panic!("connector error: {}", error),
        };
        let cert_file = shell_expand(self.get_value("sslcert", "").as_str());
        if !cert_file.is_empty() {
            if let Err(error) = builder.set_certificate_chain_file(cert_file) {
                eprintln!("set_certificate_file: {}", error);
            }
            let private_key = shell_expand(
                self.get_value("sslkey",
                               "~/.postgresql/postgresql.key").as_str());
            if let Err(error) = builder.set_private_key_file(
                private_key, SslFiletype::PEM) {
                eprintln!("set_client_key_file: {}", error);
            }
            let root_cert = shell_expand(
                self.get_value("sslrootcert",
                               "~/.postgresql/root.crt").as_str());
            if let Err(error) = builder.set_ca_file(root_cert) {
                eprintln!("set_ca_file: {}", error);
            }
        }

        let mut connector = MakeTlsConnector::new(builder.build());
        connector.set_callback(move |config, _| {
            config.set_verify_hostname(self.verify_hostname());
            Ok(())
        });
        let client = postgres::Client::connect(
            conn_string, connector).unwrap();
        return client
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        // Lets record the envvars we set
        let mut envvars = HashMap::new();
        envvars.insert("PGHOST", "here");
        envvars.insert("PGDATABASE", "there");
        envvars.insert("PGUSER", "me");
        envvars.insert("PGSSLMODE", "disable");
        envvars.insert("PGSSLCERT", "~/cert");
        envvars.insert("PGSSLKEY", "~/key");
        envvars.insert("PGSSLROOTCERT", "~/root");
        envvars.insert("PGSSLCRL", "~/crl");
        // and set them
        for (key, value) in envvars.iter() {
            std::env::set_var(key, value);
        }

        // Lets test with these set
        let mut d = Dsn::new();
        assert_eq!(d.use_tls(), false);
        assert_eq!(d.verify_hostname(), false);
        d.set_value("sslmode", "verify-full");
        assert_eq!(d.use_tls(), true);
        assert_eq!(d.verify_hostname(), true);
        assert_eq!(d.to_string(),
        concat!("dbname=there ",
                "host=here ",
                "sslcert=~/cert ",
                "sslcrl=~/crl ",
                "sslkey=~/key ",
                "sslmode=verify-full ",
                "sslrootcert=~/root user=me"));
        // and unset them
        for (key, _) in envvars.iter() {
            std::env::remove_var(key);
        }
        // And test without them being set
        d = Dsn::new();
        assert_eq!(d.use_tls(), true);
        assert_eq!(d.cleanse().to_string(),
        concat!("dbname=sebman ",
                "host=/tmp ",
                "user=sebman"));
        assert_eq!(d.to_string(),
        concat!("dbname=sebman ",
                "host=/tmp ",
                "sslcert=~/.postgresql/postgresql.crt ",
                "sslcrl=~/.postgresql/root.crl ",
                "sslkey=~/.postgresql/postgresql.key ",
                "sslmode=prefer ",
                "sslrootcert=~/.postgresql/root.crt ",
                "user=sebman"));
        // And reset them to the value they had before runnignt his test
        for (key, value) in std::env::vars() {
            if envvars.contains_key(key.as_str()) {
                std::env::set_var(key, value);
            }
        }
    }

}

