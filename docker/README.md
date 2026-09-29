# Docker configurations

These Compose files provide standalone equivalents of the pinned server images used by the Rust adapters.
The benchmark binaries manage their own containers directly and do not consume these files or attach to manually started servers.
Use the files for inspecting an engine's startup configuration or running it independently.

| File           | Default service | Optional profiles                          |
| :------------- | :-------------- | :----------------------------------------- |
| `redis.yml`    | Redis           | `valkey`, `dragonfly`, `garnet`, `kvrocks` |
| `mongodb.yml`  | MongoDB         | `ferretdb` with PostgreSQL/DocumentDB      |
| `postgres.yml` | PostgreSQL      | —                                          |
| `neo4j.yml`    | Neo4j           | `memgraph`                                 |
| `falkordb.yml` | FalkorDB        | —                                          |

```sh
docker compose -f docker/redis.yml up -d redis
docker compose -f docker/redis.yml port redis 6379
docker compose -f docker/redis.yml down
```

Ports bind to localhost with dynamically assigned host ports.
Named volumes survive `down`; pass `--volumes` only when their data is no longer needed.
The sample configurations prioritize benchmark operation over durable write acknowledgment; the Rust binaries expose explicit durability policies and record their effective settings.
