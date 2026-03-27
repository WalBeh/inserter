# CrateDB Record Generator — Elixir Implementation

High-performance CrateDB record generator using the BEAM VM with preemptive scheduling across all CPU cores.

## Why Elixir?

The BEAM VM solves the concurrency problems of both Python and Rust for this workload:
- **No GIL**: Each worker is a lightweight BEAM process, preemptively scheduled across all CPU cores
- **No spawn_blocking needed**: CPU work in one process doesn't block I/O in another
- **Millions of processes**: Can run thousands of concurrent workers with minimal overhead
- **Built-in fault tolerance**: OTP supervisors restart crashed workers automatically

## Prerequisites

### macOS
```bash
brew install elixir
```

### Ubuntu / Debian
```bash
# Add Erlang Solutions repo
wget https://packages.erlang-solutions.com/erlang-solutions_2.0_all.deb
sudo dpkg -i erlang-solutions_2.0_all.deb
sudo apt update
sudo apt install -y elixir erlang-dev

# Or use asdf (recommended)
asdf plugin add erlang
asdf plugin add elixir
asdf install erlang 27.0
asdf install elixir 1.17.0-otp-27
```

### Amazon Linux / RHEL
```bash
# Using asdf (easiest)
git clone https://github.com/asdf-vm/asdf.git ~/.asdf --branch v0.14.0
echo '. "$HOME/.asdf/asdf.sh"' >> ~/.bashrc && source ~/.bashrc
asdf plugin add erlang
asdf plugin add elixir
asdf install erlang 27.0
asdf install elixir 1.17.0-otp-27
asdf global erlang 27.0
asdf global elixir 1.17.0-otp-27
```

### Kubernetes / Docker
```bash
# Run a pod with Elixir pre-installed
kubectl run elixir --image elixir:slim -- sleep infinity
kubectl exec -it elixir -- bash

# Inside the pod:
apt update && apt install -y git
git clone https://github.com/WalBeh/inserter.git && cd inserter/elixir
echo 'CRATE_CONNECTION_STRING=http://crate@your-cratedb-host:4200' > .env
mix local.hex --force && mix local.rebar --force
mix deps.get && mix compile

# When done:
# kubectl delete pod elixir
```

### Verify installation
```bash
elixir --version
mix --version
```

## Build & Run

```bash
cd elixir

# Set up connection
echo 'CRATE_CONNECTION_STRING=http://crate@localhost:4200' > .env

# Install dependencies
mix deps.get

# Run
mix run -e "CrateWrite.main()" -- --table-name test --duration 1 --threads 64 --batch-size 1000 --batch-interval 0

# Or build an escript (standalone binary)
mix escript.build
./crate_write --table-name test --duration 1 --threads 64 --batch-size 1000 --batch-interval 0
```

## CLI Options

Same flags as Python and Rust:

```
--table-name TEXT         Table to create/insert into (required)
--duration INTEGER        Minutes to run (required)
--connection-string TEXT  CrateDB URL (overrides .env)
--batch-size INTEGER      Records per bulk insert (default: 100)
--batch-interval INTEGER  Milliseconds between batches (default: 100, 0 = none)
--threads INTEGER         Number of worker processes (default: 1)
--objects INTEGER         Extra low-cardinality TEXT columns (default: 0)
--shards INTEGER          Table shards (default: 4)
--replicas INTEGER        Table replicas (default: 1)
--benchmark               Minimal output, JSONL result to stdout
--no-compression          Disable gzip (faster on localhost)
--log-level STRING        info, warning, error (default: info)
```

## Benchmark Mode

```bash
./crate_write --benchmark --no-compression --table-name bench --duration 2 \
  --threads 64 --batch-size 1000 --batch-interval 0 --shards 32 --replicas 0 >> results.json
```

Outputs JSONL with `"client": "elixir-http"` — same schema as Python and Rust.

## Architecture

```
Application Supervisor
├── Finch (HTTP connection pool)
├── Monitor (ETS counters + GenServer for samples)
└── WorkerSupervisor (DynamicSupervisor)
    ├── Worker 0 (GenServer: generate → serialize → POST → loop)
    ├── Worker 1
    └── Worker N
```

Each worker is a BEAM process (~2KB memory). The BEAM scheduler distributes them across all CPU cores with preemptive scheduling — no process can monopolize a core.

### Monitor Design

Mirrors the Rust AtomicU64 + RwLock pattern:
- **Hot path** (every batch): `ets:update_counter` — lock-free, no GenServer call
- **Cold path** (every 10s): `GenServer.call` for rate samples and percentiles

### Future: Pipeline Architecture

The worker currently does generate → serialize → send in one process. To split into a pipeline:

```
GeneratorPool (GenStage Producers) → Buffer → IngesterPool (GenStage Consumers)
```

The `Generator.generate_batch` call inside `Worker.handle_info(:run_batch, ...)` is the seam point.

## Dependencies

| Package | Purpose |
|---------|---------|
| finch | HTTP client with connection pooling |
| jason | JSON encoding/decoding |
| elixir_uuid | UUID v4 generation |
| dotenvy | .env file loading |

## License

MIT
