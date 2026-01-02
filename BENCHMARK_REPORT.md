# TSDB Benchmark Report for Sourceful Energy

**Date:** January 2026
**Target:** 10,000 DERs @ 1s interval = 36M datapoints/hour
**Test Duration:** 5 minutes (3,000,000 rows)
**Databases Tested:** 7 (VictoriaMetrics, QuestDB, ClickHouse, TimescaleDB, Elasticsearch TSDB, InfluxDB2, InfluxDB3 Core)

## Executive Summary

We tested 7 time-series databases against Sourceful Energy's use cases:
- **Mobile API** - Low-latency reads for mobile clients
- **Dashboards** - Aggregations for web dashboards
- **Analytics/AI** - Complex queries, anomaly detection
- **MQTT Ingestion** - High-throughput writes

### Quick Recommendation

| Use Case | Best Choice | Runner-up | Notes |
|----------|-------------|-----------|-------|
| **Total Cost** | ClickHouse ($70/mo) | VictoriaMetrics ($72/mo) | Best balance of storage + compute |
| **Storage** | ClickHouse (11.8x) | VictoriaMetrics (9.7x) | 17 bytes/row vs 21 bytes/row |
| **Memory** | VictoriaMetrics (443 MB) | InfluxDB2 (446 MB) | ES uses 1.7 GB (4x more!) |
| **Ingestion** | QuestDB (220k rows/s) | ClickHouse (158k rows/s) | QuestDB 22x faster than slowest |
| **Analytics/AI** | ClickHouse | QuestDB | OLAP-optimized |

### ⚠️ Key Finding: Elasticsearch Memory Cost

Elasticsearch uses **1.7 GB peak memory** during ingestion - nearly **4x more** than VictoriaMetrics/ClickHouse.
This significantly impacts cloud costs despite its good 6.2x compression.

---

## Detailed Results

### Ingestion Performance

| Database | Rows/sec | Time | Disk (3M rows) | Compression | Bytes/Row |
|----------|----------|------|----------------|-------------|-----------|
| **QuestDB** | 220,588 | 13.6s | 1328 MB | 0.4x | 464 |
| **ClickHouse** | 157,895 | 19.0s | 49 MB | **11.8x** ★ | 17 |
| **VictoriaMetrics** | 152,284 | 19.7s | 59 MB | 9.7x | 21 |
| **InfluxDB2** | 46,083 | 65.1s | 284 MB | 2.0x | 99 |
| **Elasticsearch TSDB** | 43,103 | 69.6s | 93 MB | 6.2x | 32 |
| **TimescaleDB** | 19,024 | 157.7s | 961 MB | 0.6x | 336 |
| **InfluxDB3 Core** | 9,977 | 300.7s | 442 MB | 1.3x | 154 |

**Analysis:**
- **QuestDB** fastest ingestion (221k rows/s) but worst storage efficiency
- **ClickHouse** best compression (11.8x = 92% savings, only 17 bytes/row!)
- **VictoriaMetrics** excellent compression (9.7x) with good ingestion
- **Elasticsearch TSDB** good compression (6.2x) but high memory cost
- **InfluxDB2** decent (2.0x compression, 46k rows/s)
- **InfluxDB3 Core** slowest (10k rows/s) - not production ready
- **TimescaleDB/QuestDB** expand data beyond raw size

### Use Case: Mobile API (300 queries)

Simulates mobile app home screens, DER history, site overviews.

| Database | Avg Latency | P95 Latency | Total Time |
|----------|-------------|-------------|------------|
| **TimescaleDB** | **0.7ms** | 1.5ms | 215ms |
| VictoriaMetrics | 1.2ms | 2.2ms | 363ms |
| QuestDB | 5.3ms | 11.1ms | 1582ms |
| ClickHouse | 7.9ms | 10.6ms | 2360ms |

**Winner: TimescaleDB** - PostgreSQL's indexing shines for point lookups.

### Use Case: Dashboard (200 queries)

Simulates dashboard widgets, hourly charts, fleet overviews.

| Database | Avg Latency | P95 Latency | Total Time |
|----------|-------------|-------------|------------|
| **VictoriaMetrics** | **0.7ms** | 0.9ms | 146ms |
| TimescaleDB | 1.1ms | 2.5ms | 223ms |
| ClickHouse | 4.6ms | 6.2ms | 925ms |
| QuestDB | 7.3ms | 11.4ms | 1469ms |

**Winner: VictoriaMetrics** - PromQL optimized for metrics aggregation.

### Use Case: Analytics/AI (80 queries)

Simulates anomaly detection, PV/consumption correlation, peak detection.

| Database | Avg Latency | P95 Latency | Total Time |
|----------|-------------|-------------|------------|
| **ClickHouse** | **6.0ms** | 13.9ms | 479ms |
| QuestDB | 13.2ms | 34.5ms | 1059ms |
| VictoriaMetrics | 21.9ms | 84.8ms | 1753ms |
| TimescaleDB | 44.2ms | 174.8ms | 3533ms |

**Winner: ClickHouse** - Built for OLAP, excellent for complex aggregations.

---

## Resource Usage (CPU & Memory)

Measured during 3M row ingestion using `docker stats`:

| Database | Avg CPU % | Peak CPU % | Avg Memory | Peak Memory |
|----------|-----------|------------|------------|-------------|
| **VictoriaMetrics** | 6.9% | 288.7% | 183 MB | **443 MB** ★ |
| **InfluxDB2** | 16.9% | 243.7% | 88 MB | 446 MB |
| **TimescaleDB** | 23.9% | 96.5% | 385 MB | 464 MB |
| **ClickHouse** | 12.3% | 197.6% | 453 MB | 583 MB |
| **QuestDB** | 10.5% | 497.4% | 523 MB | 684 MB |
| **InfluxDB3** | 5.5% | 79.1% | 332 MB | 850 MB |
| **Elasticsearch** | 17.9% | 388.8% | 1641 MB | **1700 MB** ⚠️ |

**Key Findings:**
- **Elasticsearch uses 4x more memory** than VictoriaMetrics (1.7 GB vs 443 MB)
- **VictoriaMetrics** most memory-efficient (443 MB peak)
- **QuestDB** has highest CPU spikes (497% peak) during batch ingestion
- **InfluxDB3** has low CPU but high memory (850 MB) for its slow ingestion

---

## Storage Efficiency (Measured)

Actual disk usage measured inside containers after flushing data to disk.

| Database | Disk Usage (3M rows) | Compression Ratio | Bytes per Row |
|----------|---------------------|-------------------|---------------|
| **ClickHouse** | **49 MB** | **11.8x** ★ | 17 |
| **VictoriaMetrics** | 59 MB | 9.7x | 21 |
| **Elasticsearch TSDB** | 93 MB | 6.2x | 32 |
| InfluxDB2 | 284 MB | 2.0x | 99 |
| InfluxDB3 Core | 442 MB | 1.3x | 154 |
| TimescaleDB | 961 MB | 0.6x (expanded) | 336 |
| QuestDB | 1,328 MB | 0.4x (expanded) | 464 |

**Key Findings:**
- **ClickHouse** best compression - only **17 bytes per datapoint**!
- **VictoriaMetrics** excellent at 21 bytes/row with lowest memory usage
- **Elasticsearch TSDB** good compression (6.2x) but 4x memory cost
- **InfluxDB2** decent compression (2.0x), reasonable memory
- **InfluxDB3 Core** still maturing - slow ingestion, moderate compression
- **QuestDB/TimescaleDB** expand data beyond raw size - expensive at scale

---

## Cloud Cost Estimation (10k DERs @ 1s, per month)

Based on measured storage + memory (3M rows extrapolated to 26B datapoints/month):

| Database | Storage/mo | Storage $ | Peak Memory | Compute $ | **Total/mo** |
|----------|------------|-----------|-------------|-----------|--------------|
| **ClickHouse** | 411 GB | $41 | 583 MB | $29 | **$70** ★ |
| **VictoriaMetrics** | 500 GB | $50 | 443 MB | $22 | **$72** |
| **Elasticsearch TSDB** | 783 GB | $78 | 1700 MB | $83 | **$161** |
| InfluxDB2 | 2,400 GB | $240 | 446 MB | $22 | $262 |
| InfluxDB3 Core | 3,726 GB | $373 | 850 MB | $42 | $414 |
| TimescaleDB | 8,107 GB | $811 | 464 MB | $23 | $833 |
| QuestDB | 11,205 GB | $1,121 | 684 MB | $33 | $1,154 |

*Compute cost estimated at ~$50/GB RAM/month for cloud instances*

**Analysis:**
- **ClickHouse & VictoriaMetrics** tie at ~$70/month - best value
- **Elasticsearch TSDB** memory usage (1.7 GB) **doubles its cost** vs ClickHouse
- **InfluxDB2** decent middle ground if already using InfluxDB
- **InfluxDB3 Core** not production ready (very slow ingestion)
- **QuestDB/TimescaleDB** 16x more expensive than ClickHouse!

---

## Architecture Recommendations

### Option 1: Single Database (Simplest)

**Choose QuestDB if:**
- Ingestion speed is critical
- You want SQL compatibility
- Storage cost is not the primary concern
- Team familiar with SQL

**Choose VictoriaMetrics if:** ⭐ RECOMMENDED
- **Storage cost is a concern** (7.5x compression = 87% savings)
- Already using Prometheus/Grafana
- Dashboard queries dominate
- Want lowest operational complexity
- Memory efficiency is important

**Choose ClickHouse if:**
- Analytics/AI is a priority
- Storage costs matter at scale
- Need good compression (3.7x)
- Want SQL compatibility

**Choose Elasticsearch TSDB if:**
- Already using Elastic stack
- Need full-text search alongside TSDB
- Team has Elasticsearch expertise
- ⚠️ Note: Uses 4x more memory than alternatives

**Choose InfluxDB2 if:**
- Already using InfluxDB ecosystem
- Need Flux query language
- Moderate compression requirements (2x)

**NOT recommended:**
- **InfluxDB3 Core** - Very slow (10k rows/s), not production-ready
- **QuestDB** - Fast ingestion but 16x storage cost of ClickHouse
- **TimescaleDB** - Poor compression without tuning, high storage cost

### Option 2: Hybrid Architecture (Recommended for Sourceful)

```
┌─────────────────────────────────────────────────────────────────┐
│                     SOURCEFUL ENERGY STACK                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   MQTT/Kafka ──────┬──────────────────────────────────┐         │
│                    │                                  │         │
│                    ▼                                  ▼         │
│   ┌─────────────────────────┐    ┌─────────────────────────┐   │
│   │    VictoriaMetrics      │    │      ClickHouse         │   │
│   │    (Hot Data)           │    │   (Historical Data)     │   │
│   │                         │    │                         │   │
│   │ • Last 24-48h           │    │ • 30+ days              │   │
│   │ • Mobile API            │    │ • Analytics/AI          │   │
│   │ • Dashboard widgets     │    │ • Reporting             │   │
│   │ • PromQL for Grafana    │    │ • Anomaly detection     │   │
│   └────────────┬────────────┘    └───────────────────────┘   │
│                │                                              │
│                │  Downsampling/ETL                            │
│                └──────────────────►                           │
│                                                                │
│   ┌─────────────────────────────────────────────────────────┐ │
│   │                     Grafana                              │ │
│   │   • Real-time dashboards (VictoriaMetrics)              │ │
│   │   • Historical analysis (ClickHouse)                    │ │
│   └─────────────────────────────────────────────────────────┘ │
│                                                                │
└─────────────────────────────────────────────────────────────────┘
```

**Benefits:**
- VictoriaMetrics for real-time (you already use it)
- ClickHouse for analytics (7.2x compression = ~85% storage savings)
- Clear separation of concerns
- Best-of-breed for each use case

### Option 3: PostgreSQL-Native Stack

If your team has strong PostgreSQL expertise:

```
TimescaleDB (single database)
├── Hot data: Hypertables with compression
├── Mobile API: DISTINCT ON queries (fastest)
├── Dashboard: time_bucket aggregations
└── Analytics: Continuous aggregates
```

**Pros:**
- Single database to manage
- Full SQL compatibility
- Strong ecosystem
- Good for mobile API latency

**Cons:**
- Slowest ingestion (6k rows/s vs 54k for QuestDB)
- Would need sharding at scale
- Higher memory requirements

---

## Scaling Projections

### 100,000 DERs @ 1s interval = 360M datapoints/hour

| Database | Ingestion | Storage/mo | Memory | Notes |
|----------|-----------|------------|--------|-------|
| **ClickHouse** | ✅ Cluster | **4.1 TB** | ~6 GB | Best overall value |
| **VictoriaMetrics** | ✅ Cluster | 5.0 TB | ~4 GB | Lowest memory, vmagent |
| **Elasticsearch** | ✅ Cluster | 7.6 TB | ~17 GB | High memory requirement |
| InfluxDB2 | ⚠️ Single node | 24 TB | ~4 GB | May need clustering |
| InfluxDB3 | ❌ Too slow | 37 TB | ~9 GB | Not production-ready |
| TimescaleDB | ⚠️ Multi-node | 81 TB | ~5 GB | Poor default compression |
| QuestDB | ⚠️ Storage | 112 TB | ~7 GB | Fast but very expensive |

---

## Benchmark Methodology

### Test Environment
- **Hardware:** MacBook Air M3 (Apple Silicon)
- **Runtime:** Docker Desktop for Mac
- **Test Duration:** 5 minutes per database
- **Dataset:** 3,000,000 rows (realistic IoT energy data)
- **Data Format:** PV inverters, batteries, smart meters, EV chargers
- **Databases:** 7 (VictoriaMetrics, QuestDB, ClickHouse, TimescaleDB, Elasticsearch TSDB, InfluxDB2, InfluxDB3 Core)
- **Monitoring:** `docker stats` for CPU + memory profiling

### Query Categories
1. **Mobile API (100 iterations × 3 queries)**
   - Latest values for specific DERs
   - 24h history for a DER
   - Site overview

2. **Dashboard (50 iterations × 4 queries)**
   - Current power for site
   - Hourly aggregation chart
   - Daily summary
   - Fleet overview

3. **Analytics (20 iterations × 4 queries)**
   - Anomaly detection (>2 stddev)
   - PV vs consumption correlation
   - Peak detection
   - Efficiency report by DER type

### Data Model
```
WALLET → SITE → DEVICE → DER
  │        │       │       │
  │        │       │       ├── PV (power, MPPT voltage/current)
  │        │       │       ├── Battery (SoC, voltage, current)
  │        │       │       ├── Meter (3-phase: L1/L2/L3)
  │        │       │       └── EV Charger (vehicle SoC, session)
  │        │       │
  │        │       └── device_id, gateway
  │        └── site_id, location
  └── wallet_id, auth
```

---

## Files

- `benchmark/run_benchmarks.py` - Main benchmark script
- `benchmark/storage_test.py` - Storage + resource efficiency test (disk, CPU, memory)
- `benchmark/data_generator.py` - Realistic Sourceful data generator
- `docker-compose.yml` - All 7 databases + Grafana
- `results/benchmark_results.json` - Raw results

## Running the Benchmark

```bash
# Start databases
./start-benchmark.sh up

# Run benchmark
./start-benchmark.sh test

# Run storage test (resets volumes, ingests 3M rows, measures disk)
cd benchmark && uv run python storage_test.py

# View results
cat results/benchmark_results.json
```
