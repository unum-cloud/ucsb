//! MongoDB-protocol benchmark for key-value records and documents.
//!
//! Requires Docker; MongoDB and FerretDB use pinned server images.
//!
//! ## Build and run
//!
//! ```sh
//! cargo run --release --no-default-features --features mongodb-backend \
//!     --bin crud-eval-mongodb -- --records 100K --threads 4 --durability buffered
//! ```

use clap::{Parser, ValueEnum};
use crudeval::{
    backend::{Backend, BackendCapabilities, BackendSession, DataModel, Durability, Key, RecordBatch, Result},
    docker::{ContainerHandle, NetworkHandle},
    run, CommonArgs,
};
use mongodb::{
    bson::{self, doc, spec::BinarySubtype, Binary, Bson, Document},
    sync::{Client, Collection},
};
use serde_json::{json, Value};
use std::{collections::BTreeMap, path::Path};
#[derive(Clone, Copy, Debug, ValueEnum)]
enum Server {
    Mongodb,
    Ferretdb,
}
#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    common: CommonArgs,
    #[arg(long, value_enum, default_value = "mongodb")]
    server: Server,
}
struct MongoDbBackend {
    container: ContainerHandle,
    postgres: Option<ContainerHandle>,
    _network: Option<NetworkHandle>,
    server: Server,
    image: &'static str,
    client: Client,
    data_model: DataModel,
    durability: Durability,
}
struct MongoDbSession {
    collection: Collection<Document>,
    data_model: DataModel,
}
fn id(key: Key) -> Bson {
    Bson::Binary(Binary {
        subtype: BinarySubtype::Generic,
        bytes: key.as_bytes().to_vec(),
    })
}
fn open(args: &CommonArgs, path: &Path, server: Server) -> Result<Box<dyn Backend>> {
    if args.reopen || args.drop_caches {
        return Err("Docker backends do not support --reopen or --drop-caches".into());
    }

    if args.data_model == DataModel::Graph {
        return Err("MongoDB supports kv and docs modalities".into());
    }
    if args.data_model == DataModel::Documents && args.field != "/score" {
        return Err("Document updates currently require --field /score".into());
    }
    if args.durability == Durability::None && matches!(server, Server::Mongodb) {
        return Err("MongoDB cannot disable journaling; choose --durability buffered or flushed".into());
    }
    let image = match server {
        Server::Mongodb => "mongodb/mongodb-community-server:9.0.2-ubi9-slim",
        Server::Ferretdb => "ghcr.io/ferretdb/ferretdb:2.7.0",
    };
    let network = if matches!(server, Server::Ferretdb) {
        Some(NetworkHandle::create()?)
    } else {
        None
    };
    let postgres = if let Some(network) = &network {
        let fsync = if args.durability == Durability::None {
            "off"
        } else {
            "on"
        };
        let sync = if args.durability == Durability::Flushed {
            "on"
        } else {
            "off"
        };
        let postgres = ContainerHandle::start_with_network(
            "ghcr.io/ferretdb/postgres-documentdb:17-0.107.0-ferretdb-2.7.0",
            5432,
            &path.join("postgres"),
            "/var/lib/postgresql/data",
            &[
                ("POSTGRES_USER", "crudeval"),
                ("POSTGRES_PASSWORD", "crudeval"),
                ("POSTGRES_DB", "postgres"),
            ],
            &[
                "postgres",
                "-c",
                &format!("fsync={fsync}"),
                "-c",
                &format!("synchronous_commit={sync}"),
            ],
            Some(network),
        )?;
        postgres.ready(|| {
            postgres
                .exec(&["pg_isready", "-U", "crudeval", "-d", "postgres"])
                .map(|_| ())
        })?;
        Some(postgres)
    } else {
        None
    };
    let postgres_url = postgres
        .as_ref()
        .map(|pg| {
            pg.name()
                .map(|name| format!("postgres://crudeval:crudeval@{name}:5432/postgres"))
        })
        .transpose()?;
    let env = if let Some(url) = &postgres_url {
        vec![
            ("FERRETDB_POSTGRESQL_URL", url.as_str()),
            ("FERRETDB_TELEMETRY", "disabled"),
        ]
    } else {
        vec![]
    };
    let container = ContainerHandle::start_with_network(
        image,
        27017,
        &path.join("mongo"),
        if matches!(server, Server::Ferretdb) {
            "/tmp/crudeval"
        } else {
            "/data/db"
        },
        &env,
        &[],
        network.as_ref(),
    )?;
    let uri = match server {
        Server::Mongodb => format!(
            "mongodb://127.0.0.1:{}/?serverSelectionTimeoutMS=1000&journal={}",
            container.port,
            args.durability == Durability::Flushed
        ),
        Server::Ferretdb => format!(
            "mongodb://crudeval:crudeval@127.0.0.1:{}/?serverSelectionTimeoutMS=1000",
            container.port
        ),
    };
    let client = Client::with_uri_str(uri).map_err(|e| e.to_string())?;
    container.ready(|| {
        client
            .database("admin")
            .run_command(doc! {"ping":1})
            .run()
            .map(|_| ())
            .map_err(|e| e.to_string())
    })?;
    Ok(Box::new(MongoDbBackend {
        container,
        postgres,
        _network: network,
        server,
        image,
        client,
        data_model: args.data_model,
        durability: args.durability,
    }))
}
impl Backend for MongoDbBackend {
    fn metadata(&self) -> BTreeMap<String, Value> {
        BTreeMap::from([
            ("backend".into(), json!(format!("{:?}", self.server).to_lowercase())),
            ("image".into(), json!(self.image)),
            ("durability".into(), json!(self.durability)),
            ("journal_or_wal_enabled".into(), json!(true)),
            (
                "batch_writes".into(),
                json!({"insert":"native", "delete":"native", "update":"per document"}),
            ),
        ])
    }
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            ordered_ranges: true,
            native_batch_read: true,
            native_batch_write: false,
            ..Default::default()
        }
    }
    fn session(&self) -> Result<Box<dyn BackendSession + '_>> {
        Ok(Box::new(MongoDbSession {
            collection: self.client.database("crudeval").collection("records"),
            data_model: self.data_model,
        }))
    }
    fn flush(&self) -> Result<()> {
        if let Some(postgres) = &self.postgres {
            return postgres
                .exec(&["psql", "-U", "crudeval", "-d", "postgres", "-c", "CHECKPOINT"])
                .map(|_| ());
        }
        self.client
            .database("admin")
            .run_command(doc! {"fsync":1})
            .run()
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
    fn server_usage(&self) -> Result<Option<Value>> {
        self.container.stats().map(Some)
    }
    fn disk_bytes(&self) -> Result<u64> {
        match &self.postgres {
            Some(pg) => pg.disk_bytes(),
            None => self.container.disk_bytes(),
        }
    }
}
impl MongoDbSession {
    fn document(&self, key: Key, value: &[u8]) -> Result<Document> {
        if self.data_model == DataModel::KeyValue {
            Ok(doc! {"_id":id(key),"value":Binary{subtype:BinarySubtype::Generic,bytes:value.to_vec()}})
        } else {
            let value: Value = serde_json::from_slice(value).map_err(|e| e.to_string())?;
            let mut doc = bson::to_document(&value).map_err(|e| e.to_string())?;
            doc.insert("_id", id(key));
            Ok(doc)
        }
    }
    fn value(&self, mut doc: Document) -> Result<Vec<u8>> {
        if self.data_model == DataModel::KeyValue {
            return doc.get_binary_generic("value").cloned().map_err(|e| e.to_string());
        }
        let key =
            Key::from_slice(doc.get_binary_generic("_id").map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;
        doc.insert("_id", key.to_string());
        serde_json::to_vec(&doc).map_err(|e| e.to_string())
    }
}
impl BackendSession for MongoDbSession {
    fn insert(&mut self, keys: &[Key], values: &RecordBatch) -> Result<usize> {
        if keys.is_empty() {
            return Ok(0);
        }
        let docs = keys
            .iter()
            .enumerate()
            .map(|(i, k)| self.document(*k, values.get(i).ok_or("Missing value")?))
            .collect::<Result<Vec<_>>>()?;
        self.collection
            .insert_many(docs)
            .run()
            .map(|r| r.inserted_ids.len())
            .map_err(|e| e.to_string())
    }
    fn read(&mut self, keys: &[Key], out: &mut RecordBatch) -> Result<usize> {
        out.clear();
        let ids = keys.iter().map(|k| id(*k)).collect::<Vec<_>>();
        let cursor = self
            .collection
            .find(doc! {"_id":{"$in":ids}})
            .run()
            .map_err(|e| e.to_string())?;
        let mut values = BTreeMap::new();
        for doc in cursor {
            let doc = doc.map_err(|e| e.to_string())?;
            let key = Key::from_slice(doc.get_binary_generic("_id").map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
            values.insert(key, self.value(doc)?);
        }
        for key in keys {
            out.push(values.get(key).map(Vec::as_slice));
        }
        Ok(values.len())
    }
    fn update(&mut self, keys: &[Key], values: &RecordBatch) -> Result<usize> {
        let mut count = 0;
        for (i, key) in keys.iter().enumerate() {
            let doc = self.document(*key, values.get(i).ok_or("Missing value")?)?;
            let field = if self.data_model == DataModel::Documents {
                "score"
            } else {
                "value"
            };
            let value = doc.get(field).ok_or("Missing update field")?.clone();
            let mut fields = Document::new();
            fields.insert(field, value);
            count += self
                .collection
                .update_one(doc! {"_id":id(*key)}, doc! {"$set":fields})
                .run()
                .map_err(|e| e.to_string())?
                .matched_count as usize;
        }
        Ok(count)
    }
    fn delete(&mut self, keys: &[Key]) -> Result<usize> {
        let ids = keys.iter().map(|k| id(*k)).collect::<Vec<_>>();
        self.collection
            .delete_many(doc! {"_id":{"$in":ids}})
            .run()
            .map(|r| r.deleted_count as usize)
            .map_err(|e| e.to_string())
    }
    fn range_read(&mut self, start: Key, limit: usize, keys: &mut Vec<Key>, out: &mut RecordBatch) -> Result<usize> {
        keys.clear();
        out.clear();
        if limit == 0 {
            return Ok(0);
        }
        let cursor = self
            .collection
            .find(doc! {"_id":{"$gte":id(start)}})
            .sort(doc! {"_id":1})
            .limit(limit as i64)
            .run()
            .map_err(|e| e.to_string())?;
        for doc in cursor {
            let doc = doc.map_err(|e| e.to_string())?;
            keys.push(
                Key::from_slice(doc.get_binary_generic("_id").map_err(|e| e.to_string())?)
                    .map_err(|e| e.to_string())?,
            );
            out.push(Some(&self.value(doc)?));
        }
        Ok(keys.len())
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
