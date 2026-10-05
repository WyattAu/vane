# Dogfood deployment — vane in front of real services

This is not a test stub: it exercises vane against real services
(Redis, a static file server) with production-shaped configs, so
every feature is exercised the way a real deployment uses it.

## What's exercised

| Feature | How |
|---|---|
| TLS termination + h3 | the edge listener serves TLS/h2/h3 on one port |
| HTTP/1.1 relay | `/` → the file server |
| TCP L4 splice | `:6379` → Redis (raw TCP passthrough) |
| Config hot-reload | edit `vane.toml`, the route applies live |
| Health checks | active probes on the file server |
| Access logs | JSON access log per transaction |
| Rate limiting | `[rate_limit]` with a generous default |

## Running

```sh
docker compose -f dogfood/compose.yml up -d
# http://localhost:8080 → the file server
# redis-cli -p 6379     → Redis via TCP splice

# Hot-reload: edit dogfood/vane.toml, save — routes apply live.
```

## Stopping

```sh
docker compose -f dogfood/compose.yml down
```
