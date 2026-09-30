//! Owned Docker containers and networks with protocol readiness and cleanup.

use std::{
    alloc::System,
    ffi::OsStr,
    path::Path,
    process::Command,
    thread,
    time::{Duration, Instant},
};

use crate::backend::Result;

pub struct ContainerHandle {
    id: String,
    pub port: u16,
    storage: String,
}

fn docker<I, S>(args: I) -> Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut args = args.into_iter();
    let first = args.next().ok_or("missing Docker command")?;
    let output = Command::new("docker")
        .arg(first.as_ref())
        .args(args)
        .output()
        .map_err(|e| format!("Docker: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "docker {}: {}",
            first.as_ref().to_string_lossy(),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

impl ContainerHandle {
    pub fn start(
        image: &str,
        port: u16,
        path: &Path,
        storage: &str,
        env: &[(&str, &str)],
        command: &[&str],
    ) -> Result<Self> {
        Self::start_with_network(image, port, path, storage, env, command, None)
    }

    pub fn start_with_network(
        image: &str,
        port: u16,
        path: &Path,
        storage: &str,
        env: &[(&str, &str)],
        command: &[&str],
        network: Option<&NetworkHandle>,
    ) -> Result<Self> {
        std::fs::create_dir_all(path).map_err(|e| e.to_string())?;
        let path = path.canonicalize().map_err(|e| e.to_string())?;
        let publish = format!("127.0.0.1::{port}");
        let mount = format!("type=bind,src={},dst={storage}", path.display());
        let mut args = Vec::with_capacity_in(6 + env.len() * 2 + command.len(), System);
        args.extend([
            "create".to_owned(),
            "--publish".into(),
            publish,
            "--mount".into(),
            mount,
        ]);
        if let Some(network) = network {
            args.extend(["--network".into(), network.id.clone()]);
        }
        for (key, value) in env {
            args.extend(["--env".into(), format!("{key}={value}")]);
        }
        args.push(image.into());
        args.extend(command.iter().map(|s| (*s).into()));
        // Create first so every failure after ownership is established has a cleanup guard.
        let id = docker(&args)?;
        let mut container = Self {
            id,
            port: 0,
            storage: storage.into(),
        };
        docker(["start", &container.id])?;
        let mut published_port = 0;
        container.ready(|| {
            let published = docker(["port", &container.id, &format!("{port}/tcp")])?;
            published_port = published
                .rsplit(':')
                .next()
                .ok_or("Docker did not publish a port")?
                .parse()
                .map_err(|e| format!("Docker port: {e}"))?;
            Ok(())
        })?;
        container.port = published_port;
        Ok(container)
    }

    pub fn name(&self) -> Result<String> {
        docker(["inspect", "--format", "{{.Name}}", &self.id]).map(|name| name.trim_start_matches('/').to_owned())
    }
    pub fn exec(&self, args: &[&str]) -> Result<String> {
        docker(["exec", self.id.as_str()].into_iter().chain(args.iter().copied()))
    }

    pub fn ready(&self, mut probe: impl FnMut() -> Result<()>) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            match probe() {
                Ok(()) => return Ok(()),
                Err(error) => {
                    let running = docker(["inspect", "--format", "{{.State.Running}}", &self.id])?;
                    if running != "true" || Instant::now() >= deadline {
                        let state = docker(["inspect", "--format", "{{json .State}}", &self.id]).unwrap_or_default();
                        let logs = docker(["logs", "--tail", "20", &self.id]).unwrap_or_default();
                        return Err(format!("Server did not become ready: {error}\n{state}\n{logs}"));
                    }
                    thread::sleep(Duration::from_millis(100));
                }
            }
        }
    }

    pub fn stats(&self) -> Result<serde_json::Value> {
        let output = docker(["stats", "--no-stream", "--format", "{{json .}}", &self.id])?;
        let mut value: serde_json::Value = serde_json::from_str(&output).map_err(|e| e.to_string())?;
        value["scope"] = serde_json::json!("container");
        Ok(value)
    }

    pub fn disk_bytes(&self) -> Result<u64> {
        let output = docker(["exec", &self.id, "du", "-sk", &self.storage])?;
        output
            .split_whitespace()
            .next()
            .ok_or("Empty disk usage")?
            .parse::<u64>()
            .map(|n| n * 1024)
            .map_err(|e| e.to_string())
    }
}

impl Drop for ContainerHandle {
    fn drop(&mut self) {
        if let Err(error) = docker(["rm", "--force", "--volumes", &self.id]) {
            eprintln!("ContainerHandle cleanup: {error}");
        }
    }
}

pub struct NetworkHandle {
    id: String,
}
impl NetworkHandle {
    pub fn create() -> Result<Self> {
        let name = format!(
            "crudeval-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| e.to_string())?
                .as_nanos()
        );
        docker(["network", "create", &name]).map(|id| Self { id })
    }
}
impl Drop for NetworkHandle {
    fn drop(&mut self) {
        if let Err(error) = docker(["network", "rm", &self.id]) {
            eprintln!("NetworkHandle cleanup: {error}");
        }
    }
}
