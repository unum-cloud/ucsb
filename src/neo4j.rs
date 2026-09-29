//! Neo4j and Memgraph graph benchmark using Bolt and Cypher.
//!
//! Requires Docker; the binary manages its own pinned graph server.
//!
//! ## Build and run
//!
//! ```sh
//! cargo run --release --no-default-features --features neo4j-backend \
//!     --bin crud-eval-neo4j -- --records 1K --threads 4 --data-model graph --durability flushed
//! ```

mod cypher;
use clap::{Parser, ValueEnum};
use crudeval::{
    backend::{Backend, BackendCapabilities, BackendSession, DataModel, Durability, Result},
    docker::ContainerHandle,
    run, CommonArgs,
};
use cypher::{CypherConnection, CypherSession};
use neo4rs::{query, ConfigBuilder, Graph};
use serde_json::{json, Value};
use std::{collections::BTreeMap, path::Path, sync::Arc};
use tokio::runtime::Runtime;
#[derive(Clone, Copy, Debug, ValueEnum)]
enum Server {
    Neo4j,
    Memgraph,
}
#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    common: CommonArgs,
    #[arg(long, value_enum, default_value = "neo4j")]
    server: Server,
}
struct Neo4jBackend {
    container: ContainerHandle,
    runtime: Arc<Runtime>,
    graph: Graph,
    server: Server,
    image: &'static str,
}
struct Neo4jConnection {
    runtime: Arc<Runtime>,
    graph: Graph,
}
impl CypherConnection for Neo4jConnection {
    fn rows(&mut self, text: &str, columns: usize) -> Result<Vec<Vec<String>>> {
        self.runtime.block_on(async {
            for attempt in 0..8 {
                let result: Result<Vec<Vec<String>>> = async {
                    let mut stream = self.graph.execute(query(text)).await.map_err(|e| e.to_string())?;
                    let mut rows = Vec::new();
                    while let Some(row) = stream.next().await.map_err(|e| e.to_string())? {
                        let mut values = Vec::new();
                        for i in 0..columns {
                            values.push(row.get::<String>(&format!("c{i}")).map_err(|e| e.to_string())?);
                        }
                        rows.push(values);
                    }
                    Ok(rows)
                }
                .await;
                match result {
                    Err(ref error)
                        if attempt < 7
                            && (error.contains("TransientError")
                                || error.contains("SerializationError")
                                || error.contains("UniquenessConstraintViolation")
                                || error.contains("Unable to commit due to unique constraint violation")) =>
                    {
                        tokio::time::sleep(std::time::Duration::from_millis(5 << attempt)).await;
                    }
                    _ => return result,
                }
            }
            unreachable!()
        })
    }
}

fn open(args: &CommonArgs, path: &Path, server: Server) -> Result<Box<dyn Backend>> {
    if args.reopen || args.drop_caches {
        return Err("Docker backends do not support --reopen or --drop-caches".into());
    }

    if args.data_model != DataModel::Graph {
        return Err("Neo4j and Memgraph support only graph data model".into());
    }
    if args.durability != Durability::Flushed {
        return Err("Neo4j/Memgraph require --durability flushed".into());
    }
    let (image, storage, env) = match server {
        Server::Neo4j => (
            "neo4j:2026.09.0-community",
            "/data",
            vec![("NEO4J_AUTH", "neo4j/crudeval-password")],
        ),
        Server::Memgraph => ("memgraph/memgraph:3.13.1", "/var/lib/memgraph", vec![]),
    };
    let command = match server {
        Server::Neo4j => vec![],
        Server::Memgraph => vec![
            "--storage-wal-enabled=true",
            "--storage-snapshot-interval-sec=300",
            "--storage-wal-file-flush-every-n-tx=1",
        ],
    };
    let container = ContainerHandle::start(image, 7687, path, storage, &env, &command)?;
    let runtime = Arc::new(Runtime::new().map_err(|e| e.to_string())?);
    let uri = format!("127.0.0.1:{}", container.port);
    let (user, password) = match server {
        Server::Neo4j => ("neo4j", "crudeval-password"),
        Server::Memgraph => ("", ""),
    };
    let database = match server {
        Server::Neo4j => "neo4j",
        Server::Memgraph => "memgraph",
    };
    let config = ConfigBuilder::new()
        .uri(&uri)
        .user(user)
        .password(password)
        .db(database)
        .build()
        .map_err(|e| e.to_string())?;
    container.ready(|| {
        runtime.block_on(async {
            let graph = Graph::connect(config.clone()).await.map_err(|e| e.to_string())?;
            graph.run(query("RETURN 1")).await.map_err(|e| e.to_string())
        })
    })?;
    let graph = runtime.block_on(Graph::connect(config)).map_err(|e| e.to_string())?;
    let index = match server {
        Server::Neo4j => "CREATE CONSTRAINT vertex_id IF NOT EXISTS FOR (v:Vertex) REQUIRE v.id IS UNIQUE",
        Server::Memgraph => "CREATE INDEX ON :Vertex(id)",
    };
    runtime.block_on(graph.run(query(index))).map_err(|e| e.to_string())?;
    if matches!(server, Server::Memgraph) {
        runtime
            .block_on(graph.run(query("CREATE CONSTRAINT ON (v:Vertex) ASSERT v.id IS UNIQUE")))
            .map_err(|e| e.to_string())?;
    }
    Ok(Box::new(Neo4jBackend {
        container,
        runtime,
        graph,
        server,
        image,
    }))
}
impl Backend for Neo4jBackend {
    fn metadata(&self) -> BTreeMap<String, Value> {
        BTreeMap::from([
            ("backend".into(), json!(format!("{:?}", self.server).to_lowercase())),
            ("image".into(), json!(self.image)),
            ("durability".into(), json!("flushed")),
        ])
    }
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            ordered_ranges: true,
            ..Default::default()
        }
    }
    fn session(&self) -> Result<Box<dyn BackendSession + '_>> {
        Ok(Box::new(CypherSession(Neo4jConnection {
            runtime: self.runtime.clone(),
            graph: self.graph.clone(),
        })))
    }
    fn flush(&self) -> Result<()> {
        Ok(())
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
    if let Err(error) = run(cli.common, format!("{:?}", cli.server), |args, path| {
        open(args, path, cli.server)
    }) {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
