mod client;
mod database;

use tokio::net::TcpListener;
use std::sync::Arc;
use tokio::sync::RwLock;
use once_cell::sync::Lazy;
use clap::{arg, Parser};

use crate::client::*;

struct Config {
    dir: Option<String>,
    db_filename: Option<String>,
}

impl Config {
    const fn default() -> Self {
        Self {
            dir: None,
            db_filename: None,
        }
    }
}

static CONFIG: Lazy<Arc<RwLock<Config>>> = Lazy::new(|| { Arc::new(RwLock::new(Config::default())) });

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    dir: Option<String>,

    #[arg(long)]
    dbfilename: Option<String>,
}

#[tokio::main]
async fn main() -> tokio::io::Result<()> {
    {
        let args = Args::parse();

        let mut config = CONFIG.write().await;
        if let Some(dir) = args.dir {
            config.dir = Some(dir);
        }

        if let Some(db_filename) = args.dbfilename {
            config.db_filename = Some(db_filename);
        }
    }

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

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