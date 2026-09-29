//! FalkorDB graph benchmark using Cypher over the Redis protocol.
//!
//! Requires Docker; the binary manages its own pinned FalkorDB container.
//!
//! ## Build and run
//!
//! ```sh
//! cargo run --release --no-default-features --features falkordb-backend \
//!     --bin crud-eval-falkordb -- --records 1K --threads 4 --data-model graph
//! ```

mod cypher;
use clap::Parser;
use crudeval::{
    backend::{Backend, BackendCapabilities, BackendSession, DataModel, Durability, Result},
    docker::ContainerHandle,
    run, CommonArgs,
};
use cypher::{CypherConnection, CypherSession};
use redis::{Client, Connection as RedisConnection, Value as RedisValue};
use serde_json::{json, Value};
use std::{collections::BTreeMap, path::Path};
#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    common: CommonArgs,
}
struct FalkorDbBackend {
    container: ContainerHandle,
    client: Client,
    durability: Durability,
}
struct FalkorDbConnection(RedisConnection);
impl CypherConnection for FalkorDbConnection {
    fn rows(&mut self, query: &str, columns: usize) -> Result<Vec<Vec<String>>> {
        let response: RedisValue = redis::cmd("GRAPH.QUERY")
            .arg("crudeval")
            .arg(query)
            .query(&mut self.0)
            .map_err(|e| e.to_string())?;
        let RedisValue::Array(parts) = response else {
            return Err("Invalid graph response".into());
        };
        if parts.len() == 1 {
            return Ok(Vec::new());
        }
        let Some(RedisValue::Array(rows)) = parts.get(1) else {
            return Err("Missing graph result rows".into());
        };
        rows.iter()
            .map(|row| {
                let RedisValue::Array(values) = row else {
                    return Err("Invalid graph row".into());
                };
                if values.len() != columns {
                    return Err("Wrong graph column count".into());
                }
                values
                    .iter()
                    .map(|v| redis::from_redis_value::<String>(v.clone()).map_err(|e| e.to_string()))
                    .collect()
            })
            .collect()
    }
}
fn open(args: &CommonArgs, path: &Path) -> Result<Box<dyn Backend>> {
    if args.reopen || args.drop_caches {
        return Err("Docker backends do not support --reopen or --drop-caches".into());
    }

    if args.data_model != DataModel::Graph {
        return Err("FalkorDB supports only graph data model".into());
    }
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
    let redis_args = format!("--save '' --appendonly {append} --appendfsync {sync}");
    let container = ContainerHandle::start(
        "falkordb/falkordb:6.0.0",
        6379,
        path,
        "/data",
        &[("REDIS_ARGS", &redis_args)],
        &[],
    )?;
    let client = Client::open(format!("redis://127.0.0.1:{}/", container.port)).map_err(|e| e.to_string())?;
    container.ready(|| {
        let mut c = FalkorDbConnection(client.get_connection().map_err(|e| e.to_string())?);
        c.rows("RETURN 'ready' AS c0", 1).map(|_| ())
    })?;
    let mut connection = FalkorDbConnection(client.get_connection().map_err(|e| e.to_string())?);
    if let Err(error) = connection.rows("CREATE INDEX FOR (v:Vertex) ON (v.id)", 0) {
        if !error.contains("'id' is already indexed") {
            return Err(error);
        }
    }
    Ok(Box::new(FalkorDbBackend {
        container,
        client,
        durability: args.durability,
    }))
}
impl Backend for FalkorDbBackend {
    fn metadata(&self) -> BTreeMap<String, Value> {
        BTreeMap::from([
            ("backend".into(), json!("falkordb")),
            ("image".into(), json!("falkordb/falkordb:6.0.0")),
            ("durability".into(), json!(self.durability)),
        ])
    }
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            ordered_ranges: true,
            ..Default::default()
        }
    }
    fn session(&self) -> Result<Box<dyn BackendSession + '_>> {
        Ok(Box::new(CypherSession(FalkorDbConnection(
            self.client.get_connection().map_err(|e| e.to_string())?,
        ))))
    }
    fn flush(&self) -> Result<()> {
        redis::cmd("SAVE")
            .query::<()>(&mut self.client.get_connection().map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())
    }
    fn server_usage(&self) -> Result<Option<Value>> {
        self.container.stats().map(Some)
    }
    fn disk_bytes(&self) -> Result<u64> {
        self.container.disk_bytes()
    }
}
fn main() {
    let cli = Cli::parse();
    if let Err(error) = run(cli.common, "falkordb/falkordb:6.0.0", open) {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
