# Time-Series Database Benchmark for IoT Energy Data

A comprehensive benchmark comparing 7 open-source time-series databases for high-frequency IoT energy monitoring workloads.

## Key Results

| Database | Compression | Peak Memory | Ingestion | Monthly Cost* |
|----------|-------------|-------------|-----------|---------------|
| **ClickHouse** | **11.8x** | 583 MB | 158k rows/s | **$70** |
| **VictoriaMetrics** | 9.7x | **443 MB** | 152k rows/s | **$72** |
| Elasticsearch TSDB | 6.2x | 1700 MB | 43k rows/s | $161 |
| InfluxDB2 | 2.0x | 446 MB | 46k rows/s | $262 |
| InfluxDB3 Core | 1.3x | 850 MB | 10k rows/s | $414 |
| TimescaleDB | 0.6x | 464 MB | 19k rows/s | $833 |
| QuestDB | 0.4x | 684 MB | 221k rows/s | $1,154 |

*Projected for 10,000 IoT devices @ 1-second intervals

**Winner: ClickHouse** - Best compression (11.8x) with excellent ingestion speed.
**Runner-up: VictoriaMetrics** - Lowest memory usage (443 MB) with great compression (9.7x).

> See [BENCHMARK_REPORT.md](BENCHMARK_REPORT.md) for the full analysis.

## Use Case

Evaluating TSDBs for a distributed energy resource (DER) monitoring platform:
- **Scale:** 10,000+ devices (solar inverters, batteries, smart meters, EV chargers)
- **Frequency:** 1-second data intervals
- **Throughput:** 36M datapoints/hour
- **Queries:** Real-time dashboards, mobile APIs, analytics/AI

## Quick Start

```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/tsdb-benchmark.git
cd tsdb-benchmark

# Start all databases
docker compose up -d

# Wait for databases to be ready (~60 seconds)
sleep 60

# Run the storage + resource benchmark
cd benchmark
uv run python storage_test.py
```

## Databases Tested

| Database | Version | Port | Web UI |
|----------|---------|------|--------|
| VictoriaMetrics | 1.106.1 | 8428 | http://localhost:8428/vmui |
| QuestDB | 8.2.1 | 9000 | http://localhost:9000 |
| ClickHouse | 24.8 | 8123 | http://localhost:8123/play |
| TimescaleDB | latest-pg16 | 5432 | - |
| Elasticsearch | 8.17.0 | 9200 | - |
| InfluxDB2 | 2.7 | 8086 | http://localhost:8086 |
| InfluxDB3 Core | latest | 8181 | - |
| Grafana | 11.4.0 | 3000 | http://localhost:3000 |

## Test Methodology

### Hardware
- MacBook Air M3 (Apple Silicon)
- Docker Desktop for Mac

### Dataset
- 3,000,000 rows per database
- Realistic IoT energy data format
- 10,000 simulated DERs (distributed energy resources)

### Metrics Measured
- **Storage:** Actual disk usage after flush
- **Compression:** Ratio vs raw JSON size
- **Ingestion:** Rows per second
- **CPU:** Average and peak usage during ingestion
- **Memory:** Average and peak RAM consumption

## Data Model

```
WALLET (authentication)
  └── SITE (physical location)
        └── DEVICE (gateway)
              └── DER (distributed energy resource)
                    ├── PV Inverter: power, MPPT voltage/current
                    ├── Battery: SoC, voltage, current, temperature
                    ├── Smart Meter: 3-phase power (L1/L2/L3)
                    └── EV Charger: power, vehicle SoC, session energy
```

## Project Structure

```
.
├── docker-compose.yml          # All 7 databases + Grafana
├── BENCHMARK_REPORT.md         # Full benchmark results and analysis
├── TSDB_ANALYSIS.md            # Initial research and recommendations
├── benchmark/
│   ├── storage_test.py         # Storage + resource benchmark
│   ├── run_benchmarks.py       # Query performance benchmark
│   └── data_generator.py       # Realistic IoT data generator
└── results/                    # Benchmark results (JSON)
```

## Requirements

- Docker + Docker Compose
- Python 3.12+ (or [uv](https://github.com/astral-sh/uv))
- ~10GB RAM recommended (Elasticsearch alone needs 2GB+)

## Key Findings

1. **ClickHouse has the best compression** (11.8x) - only 17 bytes per datapoint
2. **VictoriaMetrics uses the least memory** (443 MB peak) - ideal for constrained environments
3. **Elasticsearch TSDB uses 4x more memory** (1.7 GB) than alternatives - significantly impacts cloud costs
4. **QuestDB is fastest for ingestion** (221k rows/s) but has poor storage efficiency (0.4x)
5. **InfluxDB3 Core is not production-ready** - very slow ingestion (10k rows/s)

## License

MIT

## Contributing

Issues and pull requests welcome. Please include benchmark reproduction steps.

## Acknowledgments

Built for evaluating TSDBs for [Sourceful Energy](https://sourceful.energy/)'s IoT platform.
