//! Parameterized graph batches shared by Bolt and Redis graph transports.

use std::alloc::System;

use crudeval::backend::{
    BackendSession, GraphEdge, GraphInput, GraphOutput, GraphPatch, GraphSession, Key, KeysOutput, Result,
    TransactionSession, VertexRef,
};

pub struct EdgeParameter {
    pub slot: u32,
    pub target: Key,
}
pub struct VertexParameter {
    pub id: Key,
    pub version: u64,
    pub edges: Vec<EdgeParameter, System>,
}
pub struct PatchParameter {
    pub id: Key,
    pub version: u64,
    pub neighbor: Option<Key>,
}
pub struct Parameters {
    pub keys: Vec<Key, System>,
    pub vertices: Vec<VertexParameter, System>,
    pub patches: Vec<PatchParameter, System>,
    pub start: Key,
    pub limit: i64,
}
impl Default for Parameters {
    fn default() -> Self {
        Self {
            keys: Vec::new_in(System),
            vertices: Vec::new_in(System),
            patches: Vec::new_in(System),
            start: Key::nil(),
            limit: 0,
        }
    }
}
#[derive(Clone, Copy)]
pub enum Shape {
    Count,
    Vertices,
    Keys,
}
pub struct GraphRow {
    pub index: i64,
    pub key: Option<Key>,
    pub version: Option<u64>,
    pub target: Option<Key>,
    pub slot: Option<u32>,
}
pub struct CypherOutput<'a, 'b, 'c> {
    pub shape: Shape,
    pub count: usize,
    pub keys: Option<&'a mut KeysOutput<'b>>,
    pub vertices: Option<&'a mut GraphOutput<'c>>,
    edges: Vec<GraphEdge, System>,
    index: Option<i64>,
    version: Option<u64>,
    key: Option<Key>,
}
impl<'a, 'b, 'c> CypherOutput<'a, 'b, 'c> {
    pub fn new(shape: Shape, keys: Option<&'a mut KeysOutput<'b>>, vertices: Option<&'a mut GraphOutput<'c>>) -> Self {
        Self {
            shape,
            count: 0,
            keys,
            vertices,
            edges: Vec::new_in(System),
            index: None,
            version: None,
            key: None,
        }
    }
    pub fn clear(&mut self) {
        self.count = 0;
        self.edges.clear();
        self.index = None;
        self.version = None;
        self.key = None;
        if let Some(keys) = &mut self.keys {
            keys.clear();
        }
        if let Some(vertices) = &mut self.vertices {
            vertices.clear();
        }
    }
    fn finish_vertex(&mut self) -> Result<()> {
        if self.index.is_none() {
            return Ok(());
        }
        if let Some(output) = &mut self.vertices {
            output.push(self.version.map(|version| VertexRef {
                version,
                edges: &self.edges,
            }))?;
        }
        if let Some(key) = self.key {
            if let Some(keys) = &mut self.keys {
                keys.push(key)?;
            }
            self.count += 1;
        }
        Ok(())
    }
    pub fn row(&mut self, row: GraphRow) -> Result<()> {
        if self.index != Some(row.index) {
            self.finish_vertex()?;
            self.edges.clear();
            self.index = Some(row.index);
            self.version = row.version;
            self.key = row.key;
        }
        if let (Some(target), Some(slot)) = (row.target, row.slot) {
            self.edges.push(GraphEdge { target, slot });
            if slot == 0 {
                self.version = row.version;
            }
        }
        Ok(())
    }
    pub fn key(&mut self, key: Key) -> Result<()> {
        if let Some(keys) = &mut self.keys {
            keys.push(key)?;
        }
        self.count += 1;
        Ok(())
    }
    pub fn finish(&mut self) -> Result<usize> {
        self.finish_vertex()?;
        self.index = None;
        Ok(self.count)
    }
}
pub trait CypherConnection {
    fn execute(&mut self, query: &str, params: &Parameters, output: &mut CypherOutput<'_, '_, '_>) -> Result<usize>;
}
pub struct CypherSession<Q: CypherConnection> {
    pub connection: Q,
    parameters: Parameters,
    edges: Vec<GraphEdge, System>,
}
impl<Q: CypherConnection> CypherSession<Q> {
    pub fn new(connection: Q) -> Self {
        Self {
            connection,
            parameters: Parameters::default(),
            edges: Vec::new_in(System),
        }
    }
    fn execute(&mut self, query: &str, mut output: CypherOutput<'_, '_, '_>) -> Result<usize> {
        std::mem::swap(&mut self.edges, &mut output.edges);
        let result = self.connection.execute(query, &self.parameters, &mut output);
        std::mem::swap(&mut self.edges, &mut output.edges);
        result
    }
}
const INSERT:&str="UNWIND $vertices AS row MERGE (v:Vertex {id:row.id}) WITH v,row WHERE v.active IS NULL SET v.active=true,v.version=row.version FOREACH (edge IN row.edges | MERGE (t:Vertex {id:edge.target}) MERGE (v)-[e:Edge {slot:edge.slot}]->(t) SET e.version=row.version) RETURN count(v) AS count";
const UPDATE:&str="UNWIND $patches AS row MATCH (v:Vertex {id:row.id,active:true}) SET v.version=row.version WITH v,row OPTIONAL MATCH (v)-[old:Edge {slot:0}]->() DELETE old WITH DISTINCT v,row FOREACH (target IN CASE WHEN row.neighbor IS NULL THEN [] ELSE [row.neighbor] END | MERGE (t:Vertex {id:target}) CREATE (v)-[e:Edge {slot:0}]->(t) SET e.version=row.version) RETURN count(v) AS count";
const READ:&str="UNWIND range(0,size($keys)-1) AS n OPTIONAL MATCH (v:Vertex {id:$keys[n],active:true}) OPTIONAL MATCH (v)-[e:Edge]->(t:Vertex) RETURN n AS n,v.id AS id,coalesce(CASE WHEN e.slot=0 THEN e.version ELSE null END,v.version) AS version,t.id AS target,e.slot AS slot ORDER BY n,slot";
const RANGE:&str="MATCH (v:Vertex {active:true}) WHERE v.id >= $start WITH v ORDER BY v.id LIMIT $limit WITH collect(v) AS vs UNWIND range(0,size(vs)-1) AS n WITH n,vs[n] AS v OPTIONAL MATCH (v)-[e:Edge]->(t:Vertex) RETURN n AS n,v.id AS id,coalesce(CASE WHEN e.slot=0 THEN e.version ELSE null END,v.version) AS version,t.id AS target,e.slot AS slot ORDER BY n,slot";
impl<Q: CypherConnection> TransactionSession for CypherSession<Q> {}
impl<Q: CypherConnection> BackendSession for CypherSession<Q> {
    fn graph(&mut self) -> Option<&mut dyn GraphSession> {
        Some(self)
    }
}
impl<Q: CypherConnection> GraphSession for CypherSession<Q> {
    fn insert(&mut self, keys: &[Key], values: &GraphInput<'_>) -> Result<usize> {
        self.parameters.vertices.resize_with(keys.len(), || VertexParameter {
            id: Key::nil(),
            version: 0,
            edges: Vec::new_in(System),
        });
        for (i, key) in keys.iter().enumerate() {
            let value = values.get(i).ok_or("Missing vertex")?;
            let row = &mut self.parameters.vertices[i];
            row.id = *key;
            row.version = value.version;
            row.edges.clear();
            row.edges.extend(value.edges.iter().map(|e| EdgeParameter {
                slot: e.slot,
                target: e.target,
            }));
        }
        self.execute(INSERT, CypherOutput::new(Shape::Count, None, None))
    }
    fn update(&mut self, keys: &[Key], patches: &[GraphPatch]) -> Result<usize> {
        self.parameters.patches.clear();
        self.parameters
            .patches
            .extend(keys.iter().zip(patches).map(|(key, p)| PatchParameter {
                id: *key,
                version: p.version,
                neighbor: p.neighbor,
            }));
        self.execute(UPDATE, CypherOutput::new(Shape::Count, None, None))
    }
    fn read(&mut self, keys: &[Key], out: &mut GraphOutput<'_>) -> Result<usize> {
        out.clear();
        if keys.is_empty() {
            return Ok(0);
        }
        self.parameters.keys.clear();
        self.parameters.keys.extend_from_slice(keys);
        self.execute(READ, CypherOutput::new(Shape::Vertices, None, Some(out)))
    }
    fn delete(&mut self, keys: &[Key]) -> Result<usize> {
        self.parameters.keys.clear();
        self.parameters.keys.extend_from_slice(keys);
        self.execute("UNWIND $keys AS key MATCH (v:Vertex {id:key,active:true}) WITH DISTINCT v DETACH DELETE v RETURN count(v) AS count",CypherOutput::new(Shape::Count,None,None))
    }
    fn range_read(
        &mut self,
        start: Key,
        limit: usize,
        keys: &mut KeysOutput<'_>,
        out: &mut GraphOutput<'_>,
    ) -> Result<usize> {
        keys.clear();
        out.clear();
        if limit == 0 {
            return Ok(0);
        }
        self.parameters.start = start;
        self.parameters.limit = limit as i64;
        self.execute(RANGE, CypherOutput::new(Shape::Vertices, Some(keys), Some(out)))
    }
    fn expand_neighbors(&mut self, start: Key, limit: usize, keys: &mut KeysOutput<'_>) -> Result<usize> {
        self.parameters.start = start;
        self.parameters.limit = limit as i64;
        self.execute("MATCH (v:Vertex {id:$start,active:true})-[:Edge]->(:Vertex)-[:Edge]->(t:Vertex) WHERE t.id <> $start RETURN DISTINCT t.id AS id ORDER BY id LIMIT $limit",CypherOutput::new(Shape::Keys,Some(keys),None))
    }
}
