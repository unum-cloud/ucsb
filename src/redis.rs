//! Redis-protocol key-value benchmark across compatible servers.
//!
//! Requires Docker; the binary manages its own pinned server container.
//!
//! ## Build and run
//!
//! ```sh
//! cargo run --release --no-default-features --features redis-backend \
//!     --bin crud-eval-redis -- --records 100K --threads 4
//! ```

use clap::{Parser, ValueEnum};
use crudeval::{
    backend::{Backend, BackendCapabilities, BackendSession, DataModel, Durability, Key, RecordBatch, Result},
    docker::ContainerHandle,
    run, CommonArgs,
};
use redis::{Client, Connection};
use serde_json::{json, Value};
use std::{collections::BTreeMap, path::Path};

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Server {
    Redis,
    Valkey,
    Dragonfly,
    Garnet,
    Kvrocks,
}
#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    common: CommonArgs,
    #[arg(long, value_enum, default_value = "redis")]
    server: Server,
}
struct RedisBackend {
    container: ContainerHandle,
    client: Client,
    server: Server,
    durability: Durability,
    image: &'static str,
}
struct RedisSession {
    connection: Connection,
}

fn open(args: &CommonArgs, path: &Path, server: Server) -> Result<Box<dyn Backend>> {
    if args.reopen || args.drop_caches {
        return Err("Docker backends do not support --reopen or --drop-caches".into());
    }

    if args.data_model != DataModel::KeyValue {
        return Err("Redis supports only kv data model".into());
    }
    let image = match server {
        Server::Redis => "redis:8.10.2",
        Server::Valkey => "valkey/valkey:9.1.2",
        Server::Dragonfly => "ghcr.io/dragonflydb/dragonfly:v2.0.0",
        Server::Garnet => "ghcr.io/microsoft/garnet:2.1.8",
        Server::Kvrocks => "apache/kvrocks:2.17.0",
    };
    let append = if args.durability == Durability::None {
        "no"
    } else {
        "yes"
    };
    let sync = if args.durability == Durability::Flushed {
        "always"
    } else {
        "everysec"
    };
    let mut command = match server {
        Server::Redis | Server::Valkey => vec!["--save", "", "--appendonly", append, "--appendfsync", sync],
        Server::Dragonfly => {
            if args.durability != Durability::None {
                return Err("Dragonfly snapshot persistence does not implement buffered/flushed WAL durability; use --durability none".into());
            }
            vec!["--dir=/data", "--dbfilename=crudeval", "--snapshot_cron="]
        }
        Server::Garnet => vec![
            "--bind",
            "0.0.0.0",
            "--port",
            "6379",
            "--checkpointdir",
            "/data/checkpoint",
            "--logdir",
            "/data/log",
            "--storage-tier",
            "--recover",
            "--memory",
            "256m",
            "--index",
            "64m",
        ],
        Server::Kvrocks => vec![
            "--bind",
            "0.0.0.0",
            "--port",
            "6379",
            "--dir",
            "/data",
            "--rocksdb.write_options.disable_wal",
            if args.durability == Durability::None {
                "yes"
            } else {
                "no"
            },
            "--rocksdb.write_options.sync",
            if args.durability == Durability::Flushed {
                "yes"
            } else {
                "no"
            },
        ],
    };
    if matches!(server, Server::Garnet) && args.durability != Durability::None {
        command.extend([
            "--aof",
            "--aof-commit-freq",
            if args.durability == Durability::Flushed {
                "0"
            } else {
                "1000"
            },
        ]);
        if args.durability == Durability::Flushed {
            command.push("--aof-commit-wait");
        }
    }
    let container = ContainerHandle::start(image, 6379, path, "/data", &[], &command)?;
    let client = Client::open(format!("redis://127.0.0.1:{}/", container.port)).map_err(|e| e.to_string())?;
    container.ready(|| {
        let mut connection = client.get_connection().map_err(|e| e.to_string())?;
        redis::cmd("PING")
            .query::<String>(&mut connection)
            .map(|_| ())
            .map_err(|e| e.to_string())
    })?;
    Ok(Box::new(RedisBackend {
        container,
        client,
        server,
        durability: args.durability,
        image,
    }))
}
impl Backend for RedisBackend {
    fn metadata(&self) -> BTreeMap<String, Value> {
        BTreeMap::from([
            ("backend".into(), json!(format!("{:?}", self.server).to_lowercase())),
            ("durability".into(), json!(self.durability)),
            ("image".into(), json!(self.image)),
            (
                "flush".into(),
                json!(if matches!(self.server, Server::Kvrocks) {
                    "FLUSHMEMTABLE"
                } else {
                    "SAVE"
                }),
            ),
            (
                "storage".into(),
                json!("native binary keys; no auxiliary ordered index"),
            ),
        ])
    }
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            native_batch_read: true,
            native_batch_write: false,
            ..Default::default()
        }
    }
    fn session(&self) -> Result<Box<dyn BackendSession + '_>> {
        Ok(Box::new(RedisSession {
            connection: self.client.get_connection().map_err(|e| e.to_string())?,
        }))
    }
    fn flush(&self) -> Result<()> {
        if matches!(self.server, Server::Kvrocks) {
            let mut connection = self.client.get_connection().map_err(|e| e.to_string())?;
            return redis::cmd("FLUSHMEMTABLE")
                .query::<()>(&mut connection)
                .map_err(|e| e.to_string());
        }
        let mut connection = self.client.get_connection().map_err(|e| e.to_string())?;
        redis::cmd("SAVE")
            .query::<()>(&mut connection)
            .map_err(|e| e.to_string())
    }
    fn server_usage(&self) -> Result<Option<Value>> {
        self.container.stats().map(Some)
    }
    fn disk_bytes(&self) -> Result<u64> {
        self.container.disk_bytes()
    }
}
impl BackendSession for RedisSession {
    fn insert(&mut self, keys: &[Key], values: &RecordBatch) -> Result<usize> {
        let mut pipe = redis::pipe();
        for (i, key) in keys.iter().enumerate() {
            pipe.cmd("SET")
                .arg(key.as_bytes().as_slice())
                .arg(values.get(i).ok_or("Missing insert value")?)
                .arg("NX");
        }
        let results: Vec<Option<String>> = pipe.query(&mut self.connection).map_err(|e| e.to_string())?;
        Ok(results.iter().filter(|r| r.is_some()).count())
    }
    fn read(&mut self, keys: &[Key], output: &mut RecordBatch) -> Result<usize> {
        output.clear();
        if keys.is_empty() {
            return Ok(0);
        }
        let mut cmd = redis::cmd("MGET");
        for key in keys {
            cmd.arg(key.as_bytes().as_slice());
        }
        let values: Vec<Option<Vec<u8>>> = cmd.query(&mut self.connection).map_err(|e| e.to_string())?;
        let mut found = 0;
        for value in values {
            found += usize::from(value.is_some());
            output.push(value.as_deref());
        }
        Ok(found)
    }
    fn update(&mut self, keys: &[Key], values: &RecordBatch) -> Result<usize> {
        let mut pipe = redis::pipe();
        for (i, key) in keys.iter().enumerate() {
            pipe.cmd("SET")
                .arg(key.as_bytes().as_slice())
                .arg(values.get(i).ok_or("Missing update value")?)
                .arg("XX");
        }
        let results: Vec<Option<String>> = pipe.query(&mut self.connection).map_err(|e| e.to_string())?;
        Ok(results.iter().filter(|r| r.is_some()).count())
    }
    fn delete(&mut self, keys: &[Key]) -> Result<usize> {
        if keys.is_empty() {
            return Ok(0);
        }
        let mut cmd = redis::cmd("DEL");
        for key in keys {
            cmd.arg(key.as_bytes().as_slice());
        }
        cmd.query(&mut self.connection).map_err(|e| e.to_string())
    }
    fn range_read(&mut self, _: Key, _: usize, _: &mut Vec<Key>, _: &mut RecordBatch) -> Result<usize> {
        Err("Redis does not support ordered key ranges".into())
    }
}
fn main() {
    let cli = Cli::parse();
    if let Err(error) = run(cli.common, format!("{:?}", cli.server), |args, path| {
        open(args, path, cli.server)
    }) {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
