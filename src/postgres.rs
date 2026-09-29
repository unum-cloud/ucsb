//! PostgreSQL benchmark for key-value records, JSONB documents, and graphs.
//!
//! Requires Docker; the binary manages its own pinned PostgreSQL container.
//!
//! ## Build and run
//!
//! ```sh
//! cargo run --release --no-default-features --features postgres-backend \
//!     --bin crud-eval-postgres -- --records 100K --threads 4
//! ```

use clap::Parser;
use crudeval::{
    backend::{Backend, BackendCapabilities, BackendSession, DataModel, Durability, Key, RecordBatch, Result},
    docker::ContainerHandle,
    run, CommonArgs,
};
use postgres::{Client, NoTls};
use serde_json::{json, Value};
use std::{collections::BTreeMap, path::Path};
#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    common: CommonArgs,
}
struct PostgresBackend {
    container: ContainerHandle,
    url: String,
    data_model: DataModel,
    durability: Durability,
}
struct PostgresSession {
    client: Client,
    data_model: DataModel,
    transaction: bool,
}
fn open(args: &CommonArgs, path: &Path) -> Result<Box<dyn Backend>> {
    if args.reopen || args.drop_caches {
        return Err("Docker backends do not support --reopen or --drop-caches".into());
    }

    if args.data_model == DataModel::Documents && args.field != "/score" {
        return Err("Document updates currently require --field /score".into());
    }
    let sync = if args.durability == Durability::Flushed {
        "on"
    } else {
        "off"
    };
    let fsync = if args.durability == Durability::None {
        "off"
    } else {
        "on"
    };
    let container = ContainerHandle::start(
        "postgres:18.6",
        5432,
        path,
        "/var/lib/postgresql",
        &[("POSTGRES_PASSWORD", "crudeval"), ("POSTGRES_DB", "crudeval")],
        &[
            "postgres",
            "-c",
            &format!("fsync={fsync}"),
            "-c",
            &format!("synchronous_commit={sync}"),
        ],
    )?;
    let url = format!(
        "host=127.0.0.1 port={} user=postgres password=crudeval dbname=crudeval connect_timeout=2",
        container.port
    );
    container.ready(|| Client::connect(&url, NoTls).map(|_| ()).map_err(|e| format!("{e:?}")))?;
    let mut client = Client::connect(&url, NoTls).map_err(|e| format!("{e:?}"))?;
    let schema=match args.data_model { DataModel::KeyValue=>"CREATE TABLE IF NOT EXISTS records (id bytea PRIMARY KEY, value bytea NOT NULL)",DataModel::Documents=>"CREATE TABLE IF NOT EXISTS records (id bytea PRIMARY KEY, value jsonb NOT NULL)",DataModel::Graph=>"CREATE TABLE IF NOT EXISTS records (id bytea PRIMARY KEY, version bigint NOT NULL); CREATE TABLE IF NOT EXISTS edges (source bytea NOT NULL REFERENCES records(id) ON DELETE CASCADE, slot integer NOT NULL, target bytea NOT NULL, PRIMARY KEY(source,slot)); CREATE INDEX IF NOT EXISTS edges_target ON edges(target)"};
    client.batch_execute(schema).map_err(|e| format!("{e:?}"))?;
    Ok(Box::new(PostgresBackend {
        container,
        url,
        data_model: args.data_model,
        durability: args.durability,
    }))
}
impl Backend for PostgresBackend {
    fn metadata(&self) -> BTreeMap<String, Value> {
        BTreeMap::from([
            ("backend".into(), json!("postgres")),
            ("image".into(), json!("postgres:18.6")),
            ("durability".into(), json!(self.durability)),
            ("wal_enabled".into(), json!(true)),
            ("fsync".into(), json!(self.durability != Durability::None)),
            (
                "synchronous_commit".into(),
                json!(self.durability == Durability::Flushed),
            ),
        ])
    }
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            ordered_ranges: true,
            transactions: true,
            ..Default::default()
        }
    }
    fn session(&self) -> Result<Box<dyn BackendSession + '_>> {
        Ok(Box::new(PostgresSession {
            client: Client::connect(&self.url, NoTls).map_err(|e| format!("{e:?}"))?,
            data_model: self.data_model,
            transaction: false,
        }))
    }
    fn flush(&self) -> Result<()> {
        Client::connect(&self.url, NoTls)
            .map_err(|e| format!("{e:?}"))?
            .batch_execute("CHECKPOINT")
            .map_err(|e| format!("{e:?}"))
    }
    fn server_usage(&self) -> Result<Option<Value>> {
        self.container.stats().map(Some)
    }
    fn disk_bytes(&self) -> Result<u64> {
        self.container.disk_bytes()
    }
}
impl PostgresSession {
    fn graph_value(key: Key, version: i64, targets: Vec<Vec<u8>>, slots: Vec<i32>) -> Result<Vec<u8>> {
        let neighbors = targets
            .iter()
            .map(|b| Key::from_slice(b).map(|k| k.to_string()).map_err(|e| format!("{e:?}")))
            .collect::<Result<Vec<_>>>()?;
        serde_json::to_vec(&json!({"_id":key.to_string(),"version":version,"neighbors":neighbors,"slots":slots}))
            .map_err(|e| format!("{e:?}"))
    }
    fn write(&mut self, keys: &[Key], values: &RecordBatch, insert: bool) -> Result<usize> {
        let local = self.data_model == DataModel::Graph && !self.transaction;
        if local {
            self.begin()?;
        }
        let result = self.write_inner(keys, values, insert);
        if local {
            match &result {
                Ok(_) => self.commit()?,
                Err(_) => self.rollback()?,
            }
        }
        result
    }
    fn write_inner(&mut self, keys: &[Key], values: &RecordBatch, insert: bool) -> Result<usize> {
        let mut count = 0;
        for (i, key) in keys.iter().enumerate() {
            let bytes = values.get(i).ok_or("Missing value")?;
            let id = key.as_bytes().as_slice();
            count += match self.data_model {
                DataModel::KeyValue => self
                    .client
                    .execute(
                        if insert {
                            "INSERT INTO records VALUES($1,$2) ON CONFLICT DO NOTHING"
                        } else {
                            "UPDATE records SET value=$2 WHERE id=$1"
                        },
                        &[&id, &bytes],
                    )
                    .map_err(|e| format!("{e:?}"))? as usize,
                DataModel::Documents => {
                    let value: Value = serde_json::from_slice(bytes).map_err(|e| format!("{e:?}"))?;
                    self.client
                        .execute(
                            if insert {
                                "INSERT INTO records VALUES($1,$2) ON CONFLICT DO NOTHING"
                            } else {
                                "UPDATE records SET value=jsonb_set(value,'{score}',$2::jsonb->'score') WHERE id=$1"
                            },
                            &[&id, &value],
                        )
                        .map_err(|e| format!("{e:?}"))? as usize
                }
                DataModel::Graph => {
                    let value: Value = serde_json::from_slice(bytes).map_err(|e| format!("{e:?}"))?;
                    let version = value["version"].as_i64().ok_or("Invalid graph version")?;
                    let changed = self
                        .client
                        .execute(
                            if insert {
                                "INSERT INTO records VALUES($1,$2) ON CONFLICT DO NOTHING"
                            } else {
                                "UPDATE records SET version=$2 WHERE id=$1"
                            },
                            &[&id, &version],
                        )
                        .map_err(|e| format!("{e:?}"))?;
                    if changed > 0 {
                        if !insert {
                            self.client
                                .execute("DELETE FROM edges WHERE source=$1 AND slot=0", &[&id])
                                .map_err(|e| format!("{e:?}"))?;
                        }
                        let slots = value["slots"].as_array().ok_or("Missing graph slots")?;
                        for (index, target) in value["neighbors"]
                            .as_array()
                            .ok_or("Invalid neighbors")?
                            .iter()
                            .enumerate()
                        {
                            let slot = slots.get(index).and_then(Value::as_i64).ok_or("Invalid graph slot")?;
                            if !insert && slot != 0 {
                                continue;
                            }
                            let target = Key::parse_str(target.as_str().ok_or("Invalid target")?)
                                .map_err(|e| format!("{e:?}"))?;
                            self.client.execute("INSERT INTO edges VALUES($1,$2,$3) ON CONFLICT(source,slot) DO UPDATE SET target=excluded.target",&[&id,&(slot as i32),&target.as_bytes().as_slice()]).map_err(|e|e.to_string())?;
                        }
                    }
                    changed as usize
                }
            };
        }
        Ok(count)
    }
}
impl BackendSession for PostgresSession {
    fn insert(&mut self, k: &[Key], v: &RecordBatch) -> Result<usize> {
        self.write(k, v, true)
    }
    fn update(&mut self, k: &[Key], v: &RecordBatch) -> Result<usize> {
        self.write(k, v, false)
    }
    fn read(&mut self, keys: &[Key], out: &mut RecordBatch) -> Result<usize> {
        out.clear();
        let mut count = 0;
        for key in keys {
            let row = self
                .client
                .query_opt(
                    if self.data_model == DataModel::Graph {
                        "SELECT version,ARRAY(SELECT target FROM edges WHERE source=r.id ORDER BY slot),ARRAY(SELECT slot FROM edges WHERE source=r.id ORDER BY slot) FROM records r WHERE id=$1"
                    } else {
                        "SELECT value FROM records WHERE id=$1"
                    },
                    &[&key.as_bytes().as_slice()],
                )
                .map_err(|e| format!("{e:?}"))?;
            let value = match row {
                None => None,
                Some(row) => Some(match self.data_model {
                    DataModel::KeyValue => row.get::<_, Vec<u8>>(0),
                    DataModel::Documents => {
                        serde_json::to_vec(&row.get::<_, Value>(0)).map_err(|e| format!("{e:?}"))?
                    }
                    DataModel::Graph => Self::graph_value(*key, row.get(0), row.get(1), row.get(2))?,
                }),
            };
            count += usize::from(value.is_some());
            out.push(value.as_deref());
        }
        Ok(count)
    }
    fn delete(&mut self, keys: &[Key]) -> Result<usize> {
        let mut count = 0;
        for key in keys {
            count += self
                .client
                .execute(
                    if self.data_model == DataModel::Graph {
                        "WITH removed AS (DELETE FROM edges WHERE target=$1) DELETE FROM records WHERE id=$1"
                    } else {
                        "DELETE FROM records WHERE id=$1"
                    },
                    &[&key.as_bytes().as_slice()],
                )
                .map_err(|e| format!("{e:?}"))? as usize;
        }
        Ok(count)
    }
    fn range_read(&mut self, start: Key, limit: usize, keys: &mut Vec<Key>, out: &mut RecordBatch) -> Result<usize> {
        keys.clear();
        out.clear();
        let rows = self
            .client
            .query(
                if self.data_model == DataModel::Graph {
                    "SELECT id,version,ARRAY(SELECT target FROM edges WHERE source=r.id ORDER BY slot),ARRAY(SELECT slot FROM edges WHERE source=r.id ORDER BY slot) FROM records r WHERE id >= $1 ORDER BY id LIMIT $2"
                } else {
                    "SELECT id,value FROM records WHERE id >= $1 ORDER BY id LIMIT $2"
                },
                &[&start.as_bytes().as_slice(), &(limit as i64)],
            )
            .map_err(|e| format!("{e:?}"))?;
        for row in rows {
            let id: Vec<u8> = row.get(0);
            let key = Key::from_slice(&id).map_err(|e| format!("{e:?}"))?;
            let value = match self.data_model {
                DataModel::KeyValue => row.get::<_, Vec<u8>>(1),
                DataModel::Documents => serde_json::to_vec(&row.get::<_, Value>(1)).map_err(|e| format!("{e:?}"))?,
                DataModel::Graph => Self::graph_value(key, row.get(1), row.get(2), row.get(3))?,
            };
            keys.push(key);
            out.push(Some(&value));
        }
        Ok(keys.len())
    }
    fn expand_neighbors(&mut self, start: Key, limit: usize, keys: &mut Vec<Key>) -> Result<usize> {
        keys.clear();
        let rows=self.client.query("SELECT DISTINCT b.target FROM edges a JOIN edges b ON a.target=b.source WHERE a.source=$1 AND b.target<>$1 ORDER BY b.target LIMIT $2",&[&start.as_bytes().as_slice(),&(limit as i64)]).map_err(|e|e.to_string())?;
        for row in rows {
            let id: Vec<u8> = row.get(0);
            keys.push(Key::from_slice(&id).map_err(|e| format!("{e:?}"))?);
        }
        Ok(keys.len())
    }
    fn begin(&mut self) -> Result<()> {
        self.client.batch_execute("BEGIN").map_err(|e| format!("{e:?}"))?;
        self.transaction = true;
        Ok(())
    }
    fn commit(&mut self) -> Result<()> {
        self.client.batch_execute("COMMIT").map_err(|e| format!("{e:?}"))?;
        self.transaction = false;
        Ok(())
    }
    fn rollback(&mut self) -> Result<()> {
        self.client.batch_execute("ROLLBACK").map_err(|e| format!("{e:?}"))?;
        self.transaction = false;
        Ok(())
    }
}
fn main() {
    let cli = Cli::parse();
    if let Err(error) = run(cli.common, "postgres:18.6", open) {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
