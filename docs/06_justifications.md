# 🎯 Justificativas Técnicas das Escolhas Arquiteturais

> **Documento para Avaliação Acadêmica - FIA/LABDATA**  
> **Respostas aos Questionamentos do Professor**

---

## 📋 Índice

1. [Resposta aos Comentários do Professor](#1-resposta-aos-comentários-do-professor)
2. [Justificativa: MinIO como Data Lake](#2-justificativa-minio-como-data-lake)
3. [Justificativa: Spark para Ingestão](#3-justificativa-spark-para-ingestão)
4. [Justificativa: Arquitetura Medallion](#4-justificativa-arquitetura-medallion)
5. [Justificativa: Tecnologias Open Source](#5-justificativa-tecnologias-open-source)
6. [Trade-offs e Decisões](#6-trade-offs-e-decisões)
7. [Comparação com Alternativas](#7-comparação-com-alternativas)

---

## 1. Resposta aos Comentários do Professor

### 1.1. "Preciso do desenho do fluxo de informações detalhado"

**✅ RESPONDIDO**

O fluxo completo está documentado em [`01_architecture.md`](./01_architecture.md) com os seguintes níveis de detalhamento:

1. **Diagrama de Alto Nível** (7 camadas)
2. **Fluxo de Dados por Etapa** (4 transformações principais)
3. **Especificação de cada Spark Job** (inputs, processing, outputs)
4. **Dependências entre DAGs** (grafo de execução)
5. **Schemas de dados** (entrada e saída de cada camada)

**Resumo Visual:**

```
┌────────────────────────────────────────────────────────────────────┐
│ FLUXO DETALHADO DE INFORMAÇÕES                                     │
├────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  [1] API SPTrans (JSON, 2 min) ─────────────────────┐              │
│                                                      │              │
│  [2] GTFS SPTrans (CSV, diário) ────────────────┐   │              │
│                                                  │   │              │
│                                                  ▼   ▼              │
│  [3] Airflow Scheduler ────> Trigger Spark Jobs                    │
│                                                  │                  │
│                                                  ▼                  │
│  [4] Spark Job: API Ingestion                                      │
│      Input: HTTP REST API                                          │
│      Process:                                                      │
│        • Autenticação (Bearer Token)                               │
│        • GET /Posicao                                              │
│        • Parse JSON (15k registros)                                │
│        • Schema validation (PySpark StructType)                    │
│        • Add metadata (ingestion_timestamp, pipeline_version)      │
│      Output: Parquet partitioned by date/hour                      │
│      Path: s3a://sptrans-bronze/api_positions/year=.../           │
│                                                  │                  │
│                                                  ▼                  │
│  [5] MinIO Storage (Bronze Layer)                                  │
│      Format: Apache Parquet (columnar)                             │
│      Compression: Snappy                                           │
│      Partitioning: Hive-style (year/month/day/hour)               │
│      Size: ~50GB/month                                             │
│                                                  │                  │
│                                                  ▼                  │
│  [6] Spark Job: Bronze → Silver                                    │
│      Input: Parquet files (last 1 hour)                            │
│      Process:                                                      │
│        • Data Quality Checks:                                      │
│          - Check nulls (vehicle_id, lat, lon, timestamp)          │
│          - Validate coordinates (-90<lat<90, -180<lon<180)        │
│          - Check future timestamps                                 │
│        • Deduplication:                                            │
│          - Window function: ROW_NUMBER() OVER (                    │
│              PARTITION BY vehicle_id, timestamp                    │
│              ORDER BY ingestion_timestamp DESC)                    │
│        • Enrichment:                                               │
│          - Join GTFS routes (by route_short_name)                 │
│          - Spatial join stops (ST_Distance < 50m)                 │
│          - Geocoding reverso: Nominatim API                        │
│            (lat, lon) → (street, neighborhood, district)          │
│        • Derived fields:                                           │
│          - speed_kmh = distance/time (Haversine formula)          │
│          - heading_degrees = ATAN2(Δlat, Δlon)                    │
│          - is_moving = speed > 5 km/h                              │
│      Output: Delta Lake table (ACID, versioned)                    │
│      Path: s3a://sptrans-silver/positions_cleaned/                │
│                                                  │                  │
│                                                  ▼                  │
│  [7] Delta Lake Storage (Silver Layer)                             │
│      Format: Delta Lake (Parquet + transaction log)                │
│      Features: MERGE, Time Travel, Schema Evolution                │
│      Size: ~30GB/month (40% reduction via DQ)                      │
│                                                  │                  │
│                                                  ▼                  │
│  [8] Spark Job: Silver → Gold                                      │
│      Input: Silver Delta Lake (last 24 hours)                      │
│      Process:                                                      │
│        • KPI Calculations:                                         │
│          a) Speed Analysis:                                        │
│             SELECT route_id, hour,                                 │
│                    AVG(speed_kmh) as avg_speed,                    │
│                    PERCENTILE(speed_kmh, 0.5) as median_speed,    │
│                    STDDEV(speed_kmh) as speed_stddev              │
│             GROUP BY route_id, hour                                │
│                                                                     │
│          b) Headway Calculation:                                   │
│             SELECT route_id, stop_id,                              │
│                    AVG(LEAD(timestamp) - timestamp) as headway    │
│             OVER (PARTITION BY route_id, stop_id)                 │
│                                                                     │
│          c) Punctuality:                                           │
│             SELECT trip_id,                                        │
│                    actual_arrival - scheduled_arrival as delay,   │
│                    CASE WHEN delay < 5 THEN 'on_time'            │
│                         ELSE 'delayed' END as status              │
│             FROM positions JOIN gtfs_stop_times                    │
│                                                                     │
│          d) Anomaly Detection:                                     │
│             z_score = (speed - AVG(speed)) / STDDEV(speed)        │
│             WHERE ABS(z_score) > 3                                 │
│      Output: Delta Lake aggregated tables                          │
│      Path: s3a://sptrans-gold/kpis_*/                             │
│                                                  │                  │
│                                                  ▼                  │
│  [9] Delta Lake Storage (Gold Layer)                               │
│      Tables: kpis_realtime, metrics_by_route, headway_analysis    │
│      Size: ~2GB/month (99% reduction via aggregation)             │
│                                                  │                  │
│                                                  ▼                  │
│  [10] Spark Job: Gold → PostgreSQL                                 │
│       Input: Gold Delta Lake                                       │
│       Process:                                                     │
│         • Read aggregated data                                     │
│         • Round values (2 decimal places)                          │
│         • Convert timestamps to America/Sao_Paulo timezone         │
│         • Filter last 7 days only                                  │
│       Output: PostgreSQL UPSERT                                    │
│         INSERT INTO serving.kpis_realtime                          │
│         ON CONFLICT (route_id, timestamp)                          │
│         DO UPDATE SET ...                                          │
│                                                  │                  │
│                                                  ▼                  │
│  [11] PostgreSQL (Serving Layer)                                   │
│       Tables: kpis_realtime, route_metrics, vehicle_status         │
│       Views: v_dashboard_summary, v_route_performance              │
│       Size: ~5GB (7 days rolling window)                           │
│                                                  │                  │
│                                              ┌───┴───┐              │
│                                              ▼       ▼              │
│  [12] Visualization Layer                                          │
│       • Apache Superset (BI dashboards)                            │
│       • Grafana (monitoring)                                       │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

### 1.2. "O projeto foi feito inicialmente usando PostgreSQL como lake quando é necessário um lake com maior capacidade de volumetria"

**✅ CORRIGIDO - JUSTIFICATIVA**

#### Problema Identificado

PostgreSQL **NÃO** é adequado como Data Lake para dados de alta volumetria porque:

1. **Custo de Storage**: PostgreSQL armazena dados em formato row-oriented (otimizado para OLTP, não analytics)
2. **Escalabilidade**: Requer particionamento manual e sharding complexo
3. **Performance**: Queries analíticos são lentos em tabelas com bilhões de registros
4. **Volumetria**: 5.4M registros/dia = ~200M registros/mês = ~500GB no PostgreSQL

#### Solução Implementada: MinIO como Data Lake

**Por que MinIO?**

| Critério | PostgreSQL | MinIO + Parquet | Vantagem |
|----------|-----------|-----------------|----------|
| **Storage Format** | Row-oriented | Columnar (Parquet) | ✅ 10x compressão |
| **Query Performance** | Seq scan = lento | Predicate pushdown | ✅ 50x mais rápido |
| **Custo Storage** | $0.10/GB/mês | $0.02/GB/mês | ✅ 5x mais barato |
| **Escalabilidade** | Vertical (1 server) | Horizontal (cluster) | ✅ Infinita |
| **Compatibilidade** | SQL only | S3 API (universal) | ✅ Spark, Presto, Athena |
| **Backup** | pg_dump (lento) | Object copy (rápido) | ✅ Snapshots instantâneos |

**Cálculo de Volumetria:**

```
Dados da API SPTrans:
• 15.000 veículos
• Atualização a cada 2 minutos
• 720 coletas/dia
• 15.000 × 720 = 10.8M registros/dia

Tamanho por registro (JSON):
• ~200 bytes/registro

Volume diário (PostgreSQL):
• 10.8M × 200 bytes = 2.16GB/dia RAW
• Com índices: ~4GB/dia
• 30 dias: 120GB
• 90 dias: 360GB

Volume diário (MinIO Parquet + Snappy):
• Parquet columnar: 50% de compressão
• Snappy: mais 50% de compressão
• Total: 75% de redução
• 2.16GB × 0.25 = 540MB/dia
• 30 dias: 16GB
• 90 dias: 49GB

ECONOMIA: 360GB → 49GB (86% de redução)
```

**PostgreSQL no Projeto Corrigido:**

✅ **Usado corretamente** como **Serving Layer** (não Data Lake):
- Armazena apenas dados agregados (Gold Layer)
- Volume: ~5GB (7 dias rolling window)
- Uso: Queries rápidas para dashboards
- Views materializadas para cache

#### Arquitetura Final Corrigida

```
┌──────────────────────────────────────────────────────────────────┐
│ ARQUITETURA CORRIGIDA                                            │
├──────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌─────────────┐                                                 │
│  │ API SPTrans │ (15k vehicles, 2 min frequency)                 │
│  └──────┬──────┘                                                 │
│         │                                                         │
│         ▼                                                         │
│  ┌─────────────────┐                                             │
│  │ Apache Spark    │ ← Ingestão distribuída (não Python puro)   │
│  │ Batch Job       │                                             │
│  └────────┬────────┘                                             │
│           │                                                       │
│           ▼                                                       │
│  ┌──────────────────────────────────────────────────────┐        │
│  │ MinIO (Data Lake) - S3 Compatible                