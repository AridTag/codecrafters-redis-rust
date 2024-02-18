mod client;
mod database;
mod persistence;
mod util;

use std::path::Path;
use std::str::FromStr;
use tokio::net::TcpListener;
use std::sync::Arc;
use tokio::sync::RwLock;
use once_cell::sync::Lazy;
use clap::{arg, Parser};

use crate::client::*;
use crate::database::db_load;

static CONFIG: Lazy<Arc<RwLock<Config>>> = Lazy::new(|| { Arc::new(RwLock::new(Config::default())) });

struct Config {
    dir: Option<String>,
    db_filename: Option<String>,
    port: u16,
    replica_of: Option<ReplicaOf>
}

#[allow(unused)]
struct ReplicaOf {
    host: String,
    port: u16,
}

impl Config {
    const fn default() -> Self {
        Self {
            dir: None,
            db_filename: None,
            port: 6379,
            replica_of: None,
        }
    }
}

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    dir: Option<String>,

    #[arg(long)]
    dbfilename: Option<String>,

    #[arg(long)]
    port: Option<u16>,

    #[clap(number_of_values = 2, name = "replicaof")]
    #[arg(long)]
    replica_of: Option<Vec<String>>,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    handle_arguments().await?;
    load_database().await?;
    let port = CONFIG.read().await.port;
    run_server(port).await?;

    Ok(())
}

async fn handle_arguments() -> Result<(), anyhow::Error> {

    let args = Args::parse();

    let mut config = CONFIG.write().await;
    if let Some(dir) = args.dir {
        config.dir = Some(dir);
    }

    if let Some(db_filename) = args.dbfilename {
        config.db_filename = Some(db_filename);
    }

    if let Some(port) = args.port {
        config.port = port;
    }

    if let Some(replica) = args.replica_of {
        config.replica_of = Some(ReplicaOf {
            host: replica[0].clone(),
            port: u16::from_str(replica[1].as_str())?,
        });
    }

    Ok(())
}

async fn load_database() -> Result<(), anyhow::Error> {
    let config = CONFIG.read().await;
    if config.dir.is_none() || config.db_filename.is_none() {
        return Ok(());
    }

    let path = Path::new(config.dir.as_ref().unwrap());
    let path = path.join(config.db_filename.as_ref().unwrap());
    db_load(path).await?;

    Ok(())
}

async fn run_server(port: u16) -> tokio::io::Result<()> {
    let bind_addr = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(bind_addr.clone()).await.unwrap();
    println!("Listening on {}", bind_addr);
    loop {
        let (stream, addr) = listener.accept().await?;
        println!("Accepted connection from {}", addr);

        tokio::spawn(async move {
            let mut client = RedisClientConnection::new(stream);
            match client.process().await {
                Ok(_) => {
                    println!("Client disconnected without error");
                }
                Err(e) => {
                    println!("Encountered error while processing client. {:?}", e);
                }
            }
        });
    }
}