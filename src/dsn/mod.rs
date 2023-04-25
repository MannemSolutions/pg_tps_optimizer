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
    // fn new() -> Dsn {
    //     Dsn{
    //         kv: HashMap::new(),
    //     }
    // }
    pub fn from_string(from: &str) -> Dsn {
        let mut kv: HashMap<String, String> = HashMap::new();
        let mut ssl_mode = "disable".to_string();
        let split = from.split(" ");
        for s in split {
            if let Some((k, v)) = s.split_once("=") {
                kv.insert(k.to_string(), v.to_string());
                if k.eq("sslmode") {
                    ssl_mode = v.to_string()
                }
            }
        }
        Dsn { kv, ssl_mode }
    }
    pub fn copy(&self) -> Dsn {
        let mut kv: HashMap<String, String> = HashMap::new();
        for (k, v) in self.kv.borrow() {
            kv.insert(k.to_string(), v.to_string());
        }
        Dsn { kv, ssl_mode: self.ssl_mode.to_string() }
    }
    pub fn cleanse(self) -> Dsn {
        let mut kv: HashMap<String, String> = HashMap::new();
        kv.extend(self.kv);
        kv.remove("sslmode");
        kv.remove("sslcert");
        kv.remove("sslkey");
        kv.remove("sslrootcert");
        kv.remove("sslcrl");
        let ssl_mode = "disable".to_string();
        Dsn { kv, ssl_mode }
    }
    pub fn from_defaults() -> Dsn {
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
    pub fn to_string(self) -> String {
        let mut vec = Vec::new();
        for (k, v) in self.kv {
            vec.push(format!("{0}={1}", k, v))
        }
        vec.join(" ")
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
    pub fn use_tls(self) -> bool {
        self.ssl_mode.eq("prefer")
    }
    pub fn verify_hostname(&self) -> bool {
        self.ssl_mode.eq("verify-full")
    }
    pub fn client(self) -> Client {
        let copy = self.copy().cleanse().to_string();
        let conn_string = copy.as_str();
        if ! self.copy().use_tls() {
            println!("not using tls");
            let client = postgres::Client::connect(
                conn_string, NoTls).unwrap();
            // The source_connection object performs the actual communication with the database,
            // so spawn it off to run on its own.
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
            let private_key = shell_expand(self.get_value("sslkey", "~/.postgresql/postgresql.key").as_str());
            if let Err(error) = builder.set_private_key_file(private_key, SslFiletype::PEM) {
                eprintln!("set_client_key_file: {}", error);
            }
            let root_cert = shell_expand(self.get_value("sslrootcert", "~/.postgresql/root.crt").as_str());
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
