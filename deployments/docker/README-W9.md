# W9: Docker Integration & Kubo+go-ds-s3

## W9 Phase 1: Smoke Test ✓

**Status:** COMPLETE

Files:
- `docker-compose.yml` — ozd + Kubo (3 tmpfs disks, R=2)
- `Dockerfile.ozd` — multi-stage Rust build (2.6MB runtime)
- `ozd-docker.toml` — Docker config (64MB segments)
- `smoke-local.toml` — localhost config
- `kubo-init.sh` — Kubo init (Phase 2: go-ds-s3 config injection)

Smoke test script: `scripts/kubo_smoke.sh`
- Tests ozd S3 API directly via curl
- 8 assertions: healthz, PUT, GET, HEAD, LIST, DELETE, 404, metrics

Run Phase 1:
```bash
# Build and test (no Docker compose, bare ozd)
./target/release/ozd --config deployments/docker/smoke-local.toml &
sleep 2
bash scripts/kubo_smoke.sh
# ✓ ALL PASSED
```

---

## W9 Phase 2: Kubo + go-ds-s3 Integration

**Status:** IN PROGRESS

### Architecture

```
IPFS Kubo (w/ go-ds-s3 plugin)
  ├─ /blocks → S3 API → ozd:9100 (blockstore)
  └─ /       → leveldb → /data/ipfs/datastore (metadata, pins)

ozd (S3-compatible)
  ├─ Stores blocks with R=2 mirror via HRW placement
  ├─ 3 tmpfs disks (dev) or 60 HDD+NVMe (prod)
  └─ Exposes /healthz, /metrics, /admin/*
```

### Files Created/Updated

1. **Dockerfile.kubo** — Build Kubo from source with go-ds-s3 module
   - Uses Go 1.22 builder
   - Clones Kubo v0.32.1 and go-ds-s3 source
   - Adds go-ds-s3 as a module dependency (replace directive)
   - Builds `ipfs` binary with embedded s3ds datastore
   - Runtime: Debian slim + jq (for config JSON manipulation)

2. **kubo-init.sh** — Configuration injection script
   - Initializes IPFS_PATH with default profile
   - Uses jq to inject Datastore.Spec with go-ds-s3 mount
   - Mounts /blocks → ozd:9100 (S3 API)
   - Mounts / → leveldb (metadata/pins)
   - Sets S3 endpoint, bucket, credentials (must match ozd auth)

3. **docker-compose.yml** — Updated for Phase 2
   - ozd: adds healthcheck (curl /healthz)
   - kubo: now builds from Dockerfile.kubo (not standard image)
   - kubo: depends_on ozd with health condition (waits for ozd ready)
   - kubo: volumes for persistent /data/ipfs across restarts
   - Both services have container_name for easy reference

4. **Dockerfile.ozd** — Minor: added curl for healthcheck

### Build & Run (Phase 2)

```bash
# Build both ozd and Kubo from scratch
docker compose -f deployments/docker/docker-compose.yml up --build

# In another terminal, test:
./scripts/kubo_smoke.sh http://localhost:9100

# Or use Kubo directly:
export KUBO_API=/ip4/127.0.0.1/tcp/5001
ipfs --api=$KUBO_API version
ipfs --api=$KUBO_API add <file>  # Stores in ozd via S3
```

### Configuration

ozd (docker-compose):
```toml
listen = "0.0.0.0:9100"
auth.access_key = "minioadmin"
auth.secret_key = "minioadmin"
replicas = 2
write_quorum = 2
```

Kubo (injected by kubo-init.sh):
```json
{
  "Datastore": {
    "Spec": {
      "type": "mount",
      "mounts": [
        {
          "mountpoint": "/blocks",
          "child": {
            "type": "s3ds",
            "regionEndpoint": "http://ozd:9100",
            "accessKey": "minioadmin",
            "secretKey": "minioadmin",
            "bucket": "kubo"
          }
        },
        {
          "mountpoint": "/",
          "child": {
            "type": "levelds",
            "path": "datastore"
          }
        }
      ]
    }
  }
}
```

### Testing Strategy

**Phase 1 (✓ done):** Direct ozd S3 API testing via curl
```bash
curl -X PUT -d "body" http://localhost:9100/kubo/blocks/CIQ...
curl http://localhost:9100/kubo/blocks/CIQ...
# Tests S3 API independently of Kubo
```

**Phase 2 (in progress):** Kubo ↔ ozd integration
```bash
docker compose up --build
ipfs add <file>  # Kubo writes blocks to ozd via go-ds-s3
ipfs cat <hash>  # Kubo reads blocks from ozd via S3
```

### Known Issues / TODOs

1. **go-ds-s3 module build:** May need specific version compatibility
   - If build fails: check Kubo version vs go-ds-s3 latest
   - Fallback: use pre-built Kubo image with plugin

2. **Config injection:** kubo-init.sh uses jq (must be in image)
   - If jq missing: fallback to sed (crude but functional)

3. **Credentials:** hardcoded minioadmin/minioadmin in Phase 2
   - Should use env vars: `$OZD_ACCESS_KEY`, `$OZD_SECRET_KEY`

4. **Metrics:** ozd /metrics exposed for monitoring
   - Kubo blockstore metrics visible via /admin/usage

### Next Steps (Post-W9)

- [ ] End-to-end test: ipfs add → retrieve from ozd
- [ ] Verify replication (R=2 mirror across tmpfs disks)
- [ ] Load test: batch add/get/delete via Kubo
- [ ] Credentials as env vars (not hardcoded)
- [ ] Production Kubo+go-ds-s3 image with optimized layers
- [ ] Benchmark: compare vs standard flatfs
