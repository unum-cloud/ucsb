//! Shared vertex, edge, and traversal operations for Cypher-compatible servers.

use crudeval::backend::{BackendSession, Key, RecordBatch, Result};
use serde_json::{json, Value};

pub trait CypherConnection {
    fn rows(&mut self, query: &str, columns: usize) -> Result<Vec<Vec<String>>>;
}
pub struct CypherSession<Q: CypherConnection>(pub Q);
impl<Q: CypherConnection> CypherSession<Q> {
    fn write(&mut self, keys: &[Key], values: &RecordBatch, insert: bool) -> Result<usize> {
        let mut count = 0;
        for (i, key) in keys.iter().enumerate() {
            let value: Value =
                serde_json::from_slice(values.get(i).ok_or("Missing graph value")?).map_err(|e| e.to_string())?;
            let version = value["version"].as_u64().ok_or("Missing graph version")?;
            let neighbors = value["neighbors"]
                .as_array()
                .ok_or("Missing graph neighbors")?
                .iter()
                .map(|v| Key::parse_str(v.as_str().ok_or("Invalid neighbor")?).map_err(|e| e.to_string()))
                .collect::<Result<Vec<_>>>()?;
            let slots = value["slots"]
                .as_array()
                .ok_or("Missing graph slots")?
                .iter()
                .map(|v| v.as_u64().ok_or_else(|| "Invalid slot".to_string()))
                .collect::<Result<Vec<_>>>()?;
            if slots.len() != neighbors.len() {
                return Err("Mismatched graph slots".into());
            }
            let mut query = if insert {
                format!("MERGE (v:Vertex {{id:'{key}'}}) WITH v WHERE v.active IS NULL SET v.active=true, v.version={version}")
            } else {
                format!("MATCH (v:Vertex {{id:'{key}',active:true}}) SET v.version={version}")
            };
            if !insert {
                query.push_str(" WITH v OPTIONAL MATCH (v)-[old:Edge {slot:0}]->() DELETE old WITH DISTINCT v");
            }
            for (slot, target) in slots.iter().zip(&neighbors) {
                if !insert && *slot != 0 {
                    continue;
                }
                query.push_str(&format!(
                    " MERGE (t{slot}:Vertex {{id:'{target}'}}) MERGE (v)-[e{slot}:Edge {{slot:{slot}}}]->(t{slot})"
                ));
                if *slot == 0 {
                    query.push_str(&format!(" SET e0.version={version}"));
                }
            }
            query.push_str(" RETURN toString(count(v)) AS c0");
            let rows = self.0.rows(&query, 1)?;
            count += rows
                .first()
                .and_then(|r| r.first())
                .ok_or("Missing write count")?
                .parse::<usize>()
                .map_err(|e| e.to_string())?;
        }
        Ok(count)
    }
    fn get(&mut self, key: Key) -> Result<Option<Vec<u8>>> {
        let rows=self.0.rows(&format!("MATCH (v:Vertex {{id:'{key}',active:true}}) OPTIONAL MATCH (v)-[e:Edge]->(t:Vertex) RETURN toString(coalesce(e.version,v.version)) AS c0, coalesce(t.id,'') AS c1, coalesce(toString(e.slot),'') AS c2 ORDER BY e.slot"),3)?;
        if rows.is_empty() {
            return Ok(None);
        }
        let version = rows[0][0].parse::<u64>().map_err(|e| e.to_string())?;
        let slots = rows
            .iter()
            .filter(|r| !r[1].is_empty())
            .map(|r| r[2].parse::<u64>().map_err(|e| e.to_string()))
            .collect::<Result<Vec<_>>>()?;
        let neighbors = rows
            .iter()
            .filter_map(|r| if r[1].is_empty() { None } else { Some(r[1].clone()) })
            .collect::<Vec<_>>();
        serde_json::to_vec(&json!({"_id":key.to_string(),"version":version,"neighbors":neighbors,"slots":slots}))
            .map(Some)
            .map_err(|e| e.to_string())
    }
}
impl<Q: CypherConnection> BackendSession for CypherSession<Q> {
    fn insert(&mut self, k: &[Key], v: &RecordBatch) -> Result<usize> {
        self.write(k, v, true)
    }
    fn update(&mut self, k: &[Key], v: &RecordBatch) -> Result<usize> {
        self.write(k, v, false)
    }
    fn read(&mut self, keys: &[Key], out: &mut RecordBatch) -> Result<usize> {
        out.clear();
        let mut found = 0;
        for key in keys {
            let value = self.get(*key)?;
            found += usize::from(value.is_some());
            out.push(value.as_deref());
        }
        Ok(found)
    }
    fn delete(&mut self, keys: &[Key]) -> Result<usize> {
        let mut count = 0;
        for key in keys {
            let rows=self.0.rows(&format!("MATCH (v:Vertex {{id:'{key}',active:true}}) WITH v, 1 AS n DETACH DELETE v RETURN toString(n) AS c0"),1)?;
            count += rows.len();
        }
        Ok(count)
    }
    fn range_read(&mut self, start: Key, limit: usize, keys: &mut Vec<Key>, out: &mut RecordBatch) -> Result<usize> {
        keys.clear();
        out.clear();
        let rows=self.0.rows(&format!("MATCH (v:Vertex {{active:true}}) WHERE v.id >= '{start}' RETURN v.id AS c0 ORDER BY v.id LIMIT {limit}"),1)?;
        for row in rows {
            let key = Key::parse_str(&row[0]).map_err(|e| e.to_string())?;
            let value = self.get(key)?;
            keys.push(key);
            out.push(value.as_deref());
        }
        Ok(keys.len())
    }
    fn expand_neighbors(&mut self, start: Key, limit: usize, keys: &mut Vec<Key>) -> Result<usize> {
        keys.clear();
        let rows=self.0.rows(&format!("MATCH (v:Vertex {{id:'{start}',active:true}})-[:Edge]->(:Vertex)-[:Edge]->(t:Vertex) WHERE t.id <> '{start}' RETURN DISTINCT t.id AS c0 ORDER BY t.id LIMIT {limit}"),1)?;
        for row in rows {
            keys.push(Key::parse_str(&row[0]).map_err(|e| e.to_string())?);
        }
        Ok(keys.len())
    }
}
