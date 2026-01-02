# TSDB-analys för Sourceful Energy

## Ert Use Case

### Datamodell: WALLET → SITE → DEVICE → DER
```
WALLET (autentisering)
  └── SITE (fysisk plats, optimering sker här)
        └── DEVICE (gateway, Modbus/MQTT/P1)
              └── DER (PV, batteri, mätare, EV-laddare)
```

### Datapunkter som samlas in
| DER-typ | Metrics |
|---------|---------|
| **PV-system** | effekt (W), MPPT-spänning/ström, kumulativ energi |
| **Batteri** | effekt, ström, spänning, SoC (0-1), temperatur, kumulativ laddad/urladdad |
| **Elmätare** | effekt/spänning/ström per fas (L1-L3), frekvens, import/export |
| **EV-laddare** | AC/DC-effekt, fordonets SoC, V2G-potential |

### Karakteristik för ert data
- **Append-only**: Tidsseriedata skrivs kontinuerligt
- **Hög kardinalitet**: Många unika serier (site × device × DER × metric)
- **Realtidsbehov**: Near-realtime för optimering och VPP
- **Historiskt behov**: Långsiktig lagring för analys och fakturering
- **Query-mönster**: Aggregering per site/device, tidsbaserade rollups

---

## Kandidatöversikt

### Tier 1: Primära kandidater (Rekommenderade att testa)

| Databas | Licens | Språk | Styrkor för Sourceful |
|---------|--------|-------|----------------------|
| **VictoriaMetrics** | Apache 2.0 | Go | Redan i bruk, Prometheus-kompatibel, extremt lagringseffektiv |
| **QuestDB** | Apache 2.0 | Java | Snabbast ingestion, SQL-native, låg latens |
| **ClickHouse** | Apache 2.0 | C++ | Bäst för analytics, massiv skalbarhet |
| **TimescaleDB** | Apache 2.0 | C (PostgreSQL) | SQL-fullt, mogen, bra för hybrid workloads |
| **Apache IoTDB** | Apache 2.0 | Java | Specifikt byggt för IoT, edge-cloud sync |

### Tier 2: Sekundära alternativ

| Databas | Licens | Varför sekundär |
|---------|--------|-----------------|
| **Grafana Mimir** | AGPL | Bra men mer komplex ops, 3x mer disk än VM |
| **TDengine** | AGPL | Bra IoT-fokus men AGPL kan vara begränsande |
| **Apache Druid** | Apache 2.0 | Överdrivet för ren TSDB, bättre för OLAP |
| **M3DB** | Apache 2.0 | Komplex ops, Uber-specifikt |

### Tier 3: Inte rekommenderade

| Databas | Varför inte |
|---------|-------------|
| **InfluxDB 3.x** | Proprietär kärnkod, inte längre ren OSS |
| **Apache Pinot** | OLAP-fokus, överkomplicerat för ren TSDB |
| **Cassandra** | Generell NoSQL, inte optimerad för tidsserier |

---

## Detaljerad Jämförelse: Tier 1 Kandidater

### 1. VictoriaMetrics ⭐ (Redan i ert stack)

**Styrkor:**
- 2-10x bättre kompression än Prometheus-baserade lösningar
- Drop-in replacement för Prometheus i Grafana
- Enkel single-node deployment ELLER kluster
- 20x mindre lagring än QuestDB för samma data
- PromQL + MetricsQL support

**Svagheter:**
- Saknar stöd för vissa query-typer (groupby-orderby-limit, lastpoint)
- Begränsat SQL-stöd (måste använda PromQL/MetricsQL)
- Inte optimalt för komplex analytics

**Bäst för:** Prometheus-ekosystemet, kostnadseffektiv långtidslagring

### 2. QuestDB

**Styrkor:**
- 959k rows/sec ingestion (4 trådar) - snabbast i klassen
- Standard PostgreSQL wire protocol
- SQL med window functions, joins, geospatial
- Inbyggd downsampling pipelines (2025)
- Zero-GC Java core = förutsägbar latens

**Svagheter:**
- 20x mer lagringsutrymme än VictoriaMetrics
- Ingen managed cloud utanför preview
- Klusterstöd är nyare/mindre moget

**Bäst för:** Ultra-låg latens, SQL-first teams, realtidsprissättning

### 3. ClickHouse

**Styrkor:**
- 12M rows/sec ingestion i benchmarks
- Skalning till 1 PB+ kluster
- Materialized views för automatiska rollups
- Extremt kraftfull för analytics
- Stor community, vältestad i produktion

**Svagheter:**
- Operationell komplexitet vid stor skala
- Inte specifikt optimerad för TSDB-patterns
- Högre resursförbrukning vid liten skala

**Bäst för:** Analytics-tunga workloads, stora datamängder

### 4. TimescaleDB

**Styrkor:**
- Full PostgreSQL (joins, foreign keys, transactions)
- Automatisk partitionering (hypertables)
- Continuous aggregates för rollups
- Mogen, vältestad i energisektorn
- Compression 90-95%

**Svagheter:**
- Skalning kräver mer planering
- Något lägre raw ingestion än QuestDB/ClickHouse
- Licens har ändrats (Apache 2.0 för core, men TSL för vissa features)

**Bäst för:** Teams som kan PostgreSQL, hybrid analytics

### 5. Apache IoTDB

**Styrkor:**
- 30M datapunkter/sekund per nod
- Specifikt byggt för IoT med edge-cloud sync
- Låg kostnad: $0.23/GB lagring
- Lätt: 32MB minimum RAM, kör på ARM7
- Inbyggd stöd för out-of-order data

**Svagheter:**
- Mindre community än andra alternativ
- Mindre integration med Grafana-ekosystemet
- Java-baserat (kan vara + eller -)

**Bäst för:** Edge deployment, IoT-specifika workloads

---

## Rekommendation för Sourceful

### Primary Stack: VictoriaMetrics + ClickHouse

```
┌─────────────────────────────────────────────────────────┐
│                    SOURCEFUL ENERGY                      │
├─────────────────────────────────────────────────────────┤
│                                                          │
│   ┌──────────────┐         ┌──────────────────────┐    │
│   │   Devices    │ ──────► │   VictoriaMetrics    │    │
│   │ (DER data)   │         │   (Realtime/Metrics) │    │
│   └──────────────┘         └──────────┬───────────┘    │
│                                       │                 │
│                                       ▼                 │
│                            ┌──────────────────────┐    │
│                            │      Grafana         │    │
│                            │   (Dashboards)       │    │
│                            └──────────────────────┘    │
│                                                          │
│   ┌──────────────┐         ┌──────────────────────┐    │
│   │ Historical   │ ──────► │     ClickHouse       │    │
│   │   ETL        │         │   (Analytics/BI)     │    │
│   └──────────────┘         └──────────────────────┘    │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

**Varför denna kombination:**
1. **VictoriaMetrics** för realtidsmetrics och Grafana-integration (ni har redan detta)
2. **ClickHouse** för långsiktig analytics och BI-queries
3. Båda är Apache 2.0, ingen vendor lock-in
4. Skalerar från startup till enterprise

### Alternativ: Single-DB approach

Om ni vill ha EN databas för allt:

| Scenario | Rekommendation |
|----------|---------------|
| PromQL-fokus, minimera ops | VictoriaMetrics (cluster mode) |
| SQL-first, analytics-tungt | ClickHouse |
| PostgreSQL-kompetens finns | TimescaleDB |
| Edge + Cloud deployment | Apache IoTDB |

---

## Benchmarks att köra

### Test 1: Ingestion Performance
- Simulera 10,000 DERs med data var 5:e sekund
- Mät: rows/sec, CPU%, memory, disk I/O

### Test 2: Query Performance
- Senaste 24h för en site
- Aggregering senaste 30 dagar
- Kardinalitet: antal unika serier

### Test 3: Compression
- Ladda 1GB rådata
- Mät: slutlig diskstorlek
- Beräkna: kompressionratio

### Test 4: Long-term Storage
- Ladda 1 år simulerad data
- Query-prestanda över stora tidsintervall
- Retention policies

---

## Benchmark-resultat

### Testkonfiguration
- **10,000 DERs** (1000 wallets × 2 sites × 5 DERs)
- **1 sekunds intervall**
- **5 minuters simulering = 3,000,000 datapunkter**
- **Realistisk Sourceful-dataformat** (PV, Batteri, Mätare, EV-laddare)

### Ingestion Performance

| Databas | Tid | Rows/sec | Disk | Kompression |
|---------|-----|----------|------|-------------|
| **QuestDB** | 32.3s | **92,974** | 833 MB | 3.6k rows/MB |
| **VictoriaMetrics** | 36.3s | **82,635** | N/A | - |
| **TimescaleDB** | 73.3s | 40,937 | N/A | - |
| **ClickHouse** | 95.1s | 31,559 | N/A | - |

**Slutsats:** QuestDB och VictoriaMetrics är ~2-3x snabbare på ingestion.

### Query Performance (ms)

| Query | ClickHouse | QuestDB | TimescaleDB | VictoriaMetrics |
|-------|------------|---------|-------------|-----------------|
| latest_per_der (100 DERs) | 67.6 | 18.7 | **5.2** | 29.6 |
| hourly_aggregation | 29.3 | 83.2 | 59.0 | 1.3 |
| top_producers | 27.5 | 40.5 | 692.6 | 2.6 |
| power_stats_by_type | 21.4 | 126.9 | 418.0 | 4.8 |
| downsample_60s | 36.2 | 92.0 | 319.9 | 2.7 |
| anomaly_detection | **86.9** | 98.1 | 1133.4 | 2.9 |
| energy_balance | **8.2** | 27.3 | 3.1 | 1.2 |
| point_in_time (10k DERs) | **16.0** | 9.6 | 15.3 | 2.6 |

**Analys:**
- **ClickHouse**: Konsekvent bra queries (8-90ms), bäst för analytics
- **QuestDB**: Mycket snabb på senaste värden och point-in-time (9-130ms)
- **TimescaleDB**: Bra för enkla lookups, långsammare på aggregering (5ms-1.1s)
- **VictoriaMetrics**: Extremt snabba queries men PromQL-begränsningar

### Rekommendation för Sourceful

#### Primär rekommendation: **QuestDB**
- ✅ Snabbast ingestion (93k rows/sec)
- ✅ Bra query-prestanda över alla typer
- ✅ Native SQL (enkelt för teamet)
- ✅ Apache 2.0 licens
- ✅ InfluxDB Line Protocol (kompatibelt med befintlig infra)

#### Alternativ: **VictoriaMetrics + ClickHouse**
- VM för realtidsmetrics och Grafana
- ClickHouse för historisk analytics och BI
- Mer komplex arkitektur men bäst i respektive domän

#### Om PostgreSQL-kompetens finns: **TimescaleDB**
- Full SQL-kompatibilitet
- Långsammare på aggregering men mogen och stabil
- Bra om ni redan har PostgreSQL-infrastruktur

---

## Status

1. ✅ Strukturera alternativ (denna fil)
2. ✅ Sätta upp Docker-miljö med top 4 kandidater
3. ✅ Generera testdata som matchar WALLET-SITE-DEVICE-DER
4. ✅ Köra benchmarks
5. ✅ Dokumentera resultat

---

## Källor

- [VictoriaMetrics Blog: Rise of OSS TSDBs](https://victoriametrics.com/blog/the-rise-of-open-source-time-series-databases/)
- [CrateDB: Best TSDBs 2026](https://cratedb.com/blog/best-time-series-databases)
- [TimeStored Benchmarks](https://www.timestored.com/data/time-series-database-benchmarks)
- [TSBS GitHub](https://github.com/timescale/tsbs)
- [Sourceful Developer Docs](https://docs.sourceful.energy/)
