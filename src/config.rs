pub use crate::message::*;
pub use crate::rdb::*;
pub use crate::stream::*;
use rand::{distributions::Alphanumeric, Rng};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use structopt::StructOpt;
#[derive(Debug, StructOpt)]
#[structopt(name = "example")]
pub struct Opt {
    #[structopt(long, parse(from_os_str), default_value = "/tmp/redis-files")]
    pub _dir: PathBuf,

    #[structopt(long, parse(from_os_str), default_value = "dump.rdb")]
    pub _dbfilename: PathBuf,

    #[structopt(long, default_value = "6379")]
    pub _port: u32,

    #[structopt(long, parse(from_str))]
    pub _replicaof: Option<String>,
}
pub struct ServerConfig {
    pub port: String,
    pub dir: String,
    pub dbfilename: String,
    pub is_master: bool,
    pub master_ip_port: Option<String>,
    pub master_id: String,
    pub master_repl_offset: RwLock<u64>,
    pub master_slave_channels: tokio::sync::Mutex<Vec<Arc<tokio::sync::Mutex<ReplicaHandle>>>>,
    pub _current_ack: RwLock<u64>,
}
impl Default for ServerConfig {
    fn default() -> Self {
        Self::new()
    }
}
impl ServerConfig {
    pub fn new() -> ServerConfig {
        // Parse and Set configuration from launch arguments
        let opt = Opt::from_args();

        let config = ServerConfig {
            port: opt._port.to_string(),
            dir: opt._dir.to_string_lossy().to_string(),
            dbfilename: opt._dbfilename.to_string_lossy().to_string(),
            is_master: opt._replicaof.is_none(),

            // NOTE: The format of the input has changed
            // TODO: implement handler for input with quotes or not
            master_ip_port: match opt._replicaof.clone() {
                Some(replicaof) => {
                    let sp: Vec<_> = replicaof.split(" ").collect();
                    Some(format!("{}:{}", sp.first().unwrap(), sp.get(1).unwrap()))
                }
                None => None,
            },
            master_id: rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(40)
                .map(char::from)
                .collect(),
            master_repl_offset: RwLock::new(0),
            master_slave_channels: tokio::sync::Mutex::new(vec![]),
            _current_ack: RwLock::new(0),
        };

        config
    }
}
