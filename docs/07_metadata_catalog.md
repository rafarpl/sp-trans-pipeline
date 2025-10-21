# 📚 Catálogo de Metadados - SPTrans Real-Time Pipeline

> **Documentação completa dos metadados, lineage e governança de dados**  
> **Versão**: 1.0 | **Data**: Outubro 2025

---

## 📋 Índice

1. [Visão Geral](#1-visão-geral)
2. [Data Lineage](#2-data-lineage)
3. [Catálogo de Datasets](#3-catálogo-de-datasets)
4. [Governança de Dados](#4-governança-de-dados)
5. [SLAs e Qualidade](#5-slas-e-qualidade)
6. [Documentação de Transformações](#6-documentação-de-transformações)

---

## 1. Visão Geral

### 1.1. Propósito do Catálogo

Este catálogo documenta todos os datasets, transformações e fluxos de dados do pipeline SPTrans, facilitando:
- **Descoberta de dados**: Encontrar datasets relevantes
- **Compreensão de lineage**: Rastrear origem e transformações
- **Governança**: Ownership, qualidade e compliance
- **Debugging**: Identificar problemas no pipeline

### 1.2. Estrutura de Metadados

```
┌─────────────────────────────────────────────────────────────┐
│ CAMADAS DE METADADOS                                        │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│ [1] Metadados Técnicos                                      │
│     • Schema (colunas, tipos, constraints)                  │
│     • Formato (Parquet, Delta Lake, PostgreSQL)            │
│     • Particionamento                                       │
│     • Localização física                                    │
│     • Tamanho e estatísticas                                │
│                                                              │
│ [2] Metadados de Negócio                                    │
│     • Descrição funcional                                   │
│     • Owner (responsável)                                   │
│     • Casos de uso                                          │
│     • Glossário de termos                                   │
│                                                              │
│ [3] Metadados Operacionais                                  │
│     • Frequência de atualização                             │
│     • SLAs (latência, disponibilidade)                      │
│     • Dependências upstream/downstream                      │
│     • Jobs que produzem/consomem                            │
│                                                              │
│ [4] Metadados de Qualidade                                  │
│     • Data Quality checks aplicados                         │
│     • Thresholds e alertas                                  │
│     • Histórico de qualidade                                │
│     • Incidentes documentados                               │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## 2. Data Lineage

### 2.1. Lineage Visual Completo

```
┌────────────────────────────────────────────────────────────────────────┐
│ DATA LINEAGE - FROM SOURCE TO CONSUMPTION                              │
├────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  [SOURCE 1]                    [SOURCE 2]                              │
│  API SPTrans                   GTFS Files                              │
│  /Posicao                      (5 arquivos CSV)                        │
│      │                              │                                  │
│      │ ingest_api_to_bronze.py      │ ingest_gtfs_to_bronze.py        │
│      ▼                              ▼                                  │
│  ┌──────────────────────────────────────────────────┐                 │
│  │ BRONZE LAYER                                     │                 │
│  │ s3a://sptrans-bronze/                            │                 │
│  │ ├── api_positions/          (Parquet)            │                 │
│  │ ├── gtfs_routes/            (Parquet)            │                 │
│  │ ├── gtfs_trips/             (Parquet)            │                 │
│  │ ├── gtfs_stops/             (Parquet)            │                 │
│  │ ├── gtfs_stop_times/        (Parquet)            │                 │
│  │ └── gtfs_shapes/            (Parquet)            │                 │
│  └───────────────────┬──────────────────────────────┘                 │
│                      │                                                 │
│                      │ bronze_to_silver.py                             │
│                      │ Transformations:                                │
│                      │ • Deduplication                                 │
│                      │ • Data Quality checks                           │
│                      │ • Join with GTFS                                │
│                      │ • Geocoding                                     │
│                      │ • Speed calculation                             │
│                      ▼                                                 │
│  ┌──────────────────────────────────────────────────┐                 │
│  │ SILVER LAYER                                     │                 │
│  │ s3a://sptrans-silver/          (Delta Lake)      │                 │
│  │ ├── positions_cleaned/                           │                 │
│  │ ├── trips_enriched/                              │                 │
│  │ ├── stops_geocoded/                              │                 │
│  │ └── routes_metadata/                             │                 │
│  └───────────────────┬──────────────────────────────┘                 │
│                      │                                                 │
│                      │ silver_to_gold.py                               │
│                      │ Aggregations:                                   │
│                      │ • KPIs calculation                              │
│                      │ • Headway analysis                              │
│                      │ • Speed metrics                                 │
│                      │ • Punctuality                                   │
│                      │ • Anomaly detection                             │
│                      ▼                                                 │
│  ┌──────────────────────────────────────────────────┐                 │
│  │ GOLD LAYER                                       │                 │
│  │ s3a://sptrans-gold/            (Delta Lake)      │                 │
│  │ ├── kpis_realtime/                               │                 │
│  │ ├── metrics_by_route/                            │                 │
│  │ ├── metrics_by_hour/                             │                 │
│  │ ├── headway_analysis/                            │                 │
│  │ ├── speed_analysis/                              │                 │
│  │ └── anomalies_detected/                          │                 │
│  └───────────────────┬──────────────────────────────┘                 │
│                      │                                                 │
│                      │ gold_to_postgres.py                             │
│                      │ Load Strategy: UPSERT                           │
│                      ▼                                                 │
│  ┌──────────────────────────────────────────────────┐                 │
│  │ SERVING LAYER                                    │                 │
│  │ PostgreSQL Database: sptrans                     │                 │
│  │ Schema: serving                                  │                 │
│  │ ├── kpis_realtime          (Table)               │                 │
│  │ ├── route_metrics          (Table)               │                 │
│  │ ├── vehicle_status         (Table)               │                 │
│  │ ├── v_dashboard_summary    (Mat. View)           │                 │
│  │ └── v_route_performance    (Mat. View)           │                 │
│  └───────────────────┬──────────────────────────────┘                 │
│                      │                                                 │
│         ┌────────────┴────────────┐                                   │
│         ▼                         ▼                                   │
│  [CONSUMPTION 1]          [CONSUMPTION 2]                             │
│  Apache Superset          Grafana                                     │
│  (BI Dashboards)          (Monitoring)                                │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2.2. Lineage por Campo (Exemplo: speed_kmh)

```
speed_kmh (Silver Layer)
    │
    ├─ SOURCE: Calculado a partir de
    │   │
    │   ├─ latitude (Bronze) ← API SPTrans: l[].vs[].py
    │   ├─ longitude (Bronze) ← API SPTrans: l[].vs[].px
    │   └─ timestamp (Bronze) ← API SPTrans: l[].vs[].ta
    │
    ├─ TRANSFORMATION: bronze_to_silver.py
    │   │
    │   └─ Logic:
    │       distance_m = haversine(lat1, lon1, lat2, lon2)
    │       time_diff_s = unix_timestamp(ts2) - unix_timestamp(ts1)
    │       speed_kmh = (distance_m / time_diff_s) * 3.6
    │
    └─ DOWNSTREAM: Usado em
        ├─ avg_speed_kmh (Gold: kpis_realtime)
        ├─ median_speed_kmh (Gold: kpis_realtime)
        └─ speed_stddev (Gold: kpis_realtime)
```

### 2.3. Matriz de Dependências

| Dataset | Produzido Por | Depende De | Consumido Por | Frequência |
|---------|---------------|------------|---------------|------------|
| `bronze/api_positions` | `dag_02_api_ingestion` | API SPTrans | `dag_03_bronze_to_silver` | 2 min |
| `bronze/gtfs_routes` | `dag_01_gtfs_ingestion` | GTFS Files | `dag_03_bronze_to_silver` | Diário |
| `silver/positions_cleaned` | `dag_03_bronze_to_silver` | bronze/* | `dag_04_silver_to_gold` | 10 min |
| `gold/kpis_realtime` | `dag_04_silver_to_gold` | silver/* | `dag_05_gold_to_serving` | 15 min |
| `serving.kpis_realtime` | `dag_05_gold_to_serving` | gold/* | Superset, Grafana | 15 min |

---

## 3. Catálogo de Datasets

### 3.1. Dataset: bronze/api_positions

**Metadados Técnicos:**
```yaml
name: api_positions
layer: bronze
format: parquet
compression: snappy
location: s3a://sptrans-bronze/api_positions/
partitioning:
  type: hive
  columns: [year, month, day, hour]
schema_version: "1.0"
created_at: "2025-10-01"
created_by: "engenharia-dados@fia.br"
```

**Metadados de Negócio:**
```yaml
description: |
  Posições GPS de todos os ônibus em circulação no sistema SPTrans,
  coletadas a cada 2 minutos via API Olho Vivo. Dados brutos sem
  transformação, mantidos para auditoria e reprocessamento.

owner: "Time Engenharia de Dados"
domain: "Transporte Público"
sensitivity: "Público"
use_cases:
  - "Análise de movimentação de frota"
  - "Cálculo de KPIs operacionais"
  - "Detecção de anomalias"
  - "Auditoria de dados"
```

**Metadados Operacionais:**
```yaml
update_frequency: "*/2 * * * *"  # A cada 2 minutos
sla:
  latency: "< 30 segundos"
  availability: "> 99.5%"
retention_policy: "90 dias"
backup_enabled: true
dependencies:
  upstream:
    - name: "API SPTrans"
      type: "external"
      endpoint: "https://api.olhovivo.sptrans.com.br"
  downstream:
    - name: "silver/positions_cleaned"
      type: "internal"
      job: "dag_03_bronze_to_silver"
```

**Metadados de Qualidade:**
```yaml
quality_checks:
  - name: "check_nulls"
    description: "Valida campos obrigatórios não são nulos"
    columns: [vehicle_id, latitude, longitude, timestamp]
    threshold: "< 1% nulls"
  
  - name: "check_coordinate_range"
    description: "Valida range de coordenadas"
    columns: [latitude, longitude]
    threshold: "-90 <= lat <= 90, -180 <= lon <= 180"
  
  - name: "check_duplicates"
    description: "Detecta registros duplicados"
    key: [vehicle_id, timestamp]
    threshold: "< 0.1% duplicates"

quality_score_history:
  - date: "2025-10-20"
    score: 98.5
  - date: "2025-10-19"
    score: 97.8
  - date: "2025-10-18"
    score: 99.1
```

**Estatísticas:**
```yaml
statistics:
  total_records: 972_000_000  # 90 dias
  size_compressed: 45 GB
  size_uncompressed: 180 GB
  avg_records_per_day: 10_800_000
  partitions_count: 2160  # 90 dias × 24 horas
```

---

### 3.2. Dataset: silver/positions_cleaned

**Metadados Técnicos:**
```yaml
name: positions_cleaned
layer: silver
format: delta
location: s3a://sptrans-silver/positions_cleaned/
partitioning:
  type: date
  column: date
delta_version: "3.0.0"
z_ordering: [vehicle_id, timestamp]
schema_version: "1.1"
```

**Metadados de Negócio:**
```yaml
description: |
  Posições GPS limpas, validadas e enriquecidas com informações de
  rotas (GTFS), paradas próximas e geocoding reverso. Inclui campos
  calculados como velocidade e direção do movimento.

owner: "Time Engenharia de Dados"
domain: "Transporte Público"
sensitivity: "Público"
pii_fields: []  # Sem dados pessoais
use_cases:
  - "Análises de performance de rotas"
  - "Cálculo de KPIs agregados"
  - "Machine Learning (previsão de atrasos)"
  - "Visualização em mapas"
```

**Metadados Operacionais:**
```yaml
update_frequency: "*/10 * * * *"  # A cada 10 minutos
sla:
  latency: "< 5 minutos"
  availability: "> 99.5%"
  data_quality: "> 95%"
retention_policy: "90 dias"
dependencies:
  upstream:
    - name: "bronze/api_positions"
      type: "internal"
      dependency: "required"
    - name: "bronze/gtfs_routes"
      type: "internal"
      dependency: "required"
    - name: "bronze/gtfs_stops"
      type: "internal"
      dependency: "required"
  downstream:
    - name: "gold/kpis_realtime"
      job: "dag_04_silver_to_gold"
    - name: "gold/metrics_by_route"
      job: "dag_04_silver_to_gold"
```

**Metadados de Qualidade:**
```yaml
quality_checks:
  - name: "check_dq_score"
    description: "Score geral de qualidade"
    threshold: "> 95%"
    action: "alert se < 90%"
  
  - name: "check_enrichment"
    description: "Valida enriquecimento com GTFS"
    columns: [route_short_name, nearest_stop_id]
    threshold: "> 90% preenchidos"
  
  - name: "check_speed_range"
    description: "Valida velocidade dentro do esperado"
    column: speed_kmh
    threshold: "0 <= speed <= 120"

transformations:
  - name: "deduplication"
    description: "Remove registros duplicados"
    logic: "ROW_NUMBER() OVER (PARTITION BY vehicle_id, timestamp)"
    
  - name: "gtfs_enrichment"
    description: "Join com dados GTFS"
    logic: "LEFT JOIN gtfs_routes ON route_code"
    
  - name: "speed_calculation"
    description: "Calcula velocidade"
    formula: "haversine(lat1,lon1,lat2,lon2) / time_diff * 3.6"
```

---

### 3.3. Dataset: gold/kpis_realtime

**Metadados Técnicos:**
```yaml
name: kpis_realtime
layer: gold
format: delta
location: s3a://sptrans-gold/kpis_realtime/
partitioning:
  type: timestamp
  column: timestamp_window
  granularity: day
aggregation_level: [route_id, 15min_window]
```

**Metadados de Negócio:**
```yaml
description: |
  KPIs operacionais calculados por rota a cada 15 minutos, incluindo
  velocidade média, headway, pontualidade e quantidade de veículos ativos.
  Dados pré-agregados otimizados para dashboards executivos.

owner: "Time Analytics"
domain: "KPIs Operacionais"
sensitivity: "Interno"
use_cases:
  - "Dashboard executivo"
  - "Monitoramento operacional em tempo real"
  - "Alertas de performance"
  - "Relatórios gerenciais"
```

**Metadados Operacionais:**
```yaml
update_frequency: "*/15 * * * *"  # A cada 15 minutos
sla:
  latency: "< 10 minutos"
  availability: "> 99.9%"
  accuracy: "> 98%"
retention_policy: "365 dias"
dependencies:
  upstream:
    - name: "silver/positions_cleaned"
      type: "internal"
      dependency: "required"
    - name: "silver/trips_enriched"
      type: "internal"
      dependency: "optional"
  downstream:
    - name: "serving.kpis_realtime"
      job: "dag_05_gold_to_serving"
```

**Metadados de Qualidade:**
```yaml
quality_checks:
  - name: "check_completeness"
    description: "Todas as rotas presentes"
    threshold: "> 95% das rotas ativas"
  
  - name: "check_kpi_ranges"
    description: "KPIs dentro do range esperado"
    thresholds:
      avg_speed_kmh: "5 <= x <= 60"
      avg_headway_minutes: "2 <= x <= 60"
      punctuality_rate: "0 <= x <= 1"

aggregations:
  - name: "avg_speed"
    function: "AVG(speed_kmh)"
    window: "15 minutes"
    
  - name: "median_speed"
    function: "PERCENTILE(speed_kmh, 0.5)"
    window: "15 minutes"
    
  - name: "avg_headway"
    function: "AVG(LEAD(timestamp) - timestamp)"
    partition: "route_id, stop_id"
```

---

## 4. Governança de Dados

### 4.1. Ownership e Responsabilidades

| Dataset / Domain | Owner | Responsabilidades |
|------------------|-------|-------------------|
| **Bronze Layer** | Time Engenharia de Dados | Ingestão, versionamento, auditoria |
| **Silver Layer** | Time Engenharia de Dados | Qualidade, transformações, lineage |
| **Gold Layer** | Time Analytics | KPIs, agregações, documentação |
| **Serving Layer** | Time Analytics | Performance queries, dashboards |
| **API SPTrans** | Time Integração | Autenticação, SLA, incidentes |
| **GTFS Data** | Time Integração | Download diário, validação |

### 4.2. Política de Acesso

**Níveis de Acesso:**

```yaml
roles:
  - name: "data_engineer"
    permissions:
      - read: [bronze, silver, gold, serving]
      - write: [bronze, silver, gold]
      - admin: [bronze]
  
  - name: "data_analyst"
    permissions:
      - read: [silver, gold, serving]
      - write: []
  
  - name: "dashboard_user"
    permissions:
      - read: [serving]
      - write: []
  
  - name: "admin"
    permissions:
      - read: [bronze, silver, gold, serving, metadata]
      - write: [bronze, silver, gold, serving, metadata]
      - admin: [all]
```

### 4.3. Política de Retenção

| Layer | Retenção | Backup | Motivo |
|-------|----------|--------|--------|
| Bronze | 90 dias | 30 dias | Auditoria e reprocessamento |
| Silver | 90 dias | 30 dias | Análises históricas |
| Gold | 365 dias | 90 dias | KPIs e compliance |
| Serving | 7 dias | Não | Cache, dados descartáveis |

**Lifecycle Policies:**

```yaml
# MinIO Lifecycle (Bronze)
lifecycle:
  - id: "expire_bronze_90d"
    prefix: "bronze/"
    expiration_days: 90
    
  - id: "transition_to_glacier"
    prefix: "bronze/"
    transition_days: 30
    storage_class: "GLACIER"

# PostgreSQL Partitions (Serving)
maintenance:
  - job: "drop_old_partitions"
    schedule: "daily 2:00 AM"
    retention: "7 days"
    command: |
      DROP TABLE IF EXISTS serving.kpis_realtime_{date-8d};
```

### 4.4. Compliance e Regulação

**LGPD (Lei Geral de Proteção de Dados):**
```yaml
compliance:
  lgpd:
    pii_fields: []  # Nenhum dado pessoal identificável
    anonymization: "not_required"
    data_subject_rights: "not_applicable"
    
  data_classification:
    bronze: "Public"
    silver: "Public"
    gold: "Internal"
    serving: "Internal"
```

---

## 5. SLAs e Qualidade

### 5.1. SLAs por Layer

**Bronze Layer:**
```yaml
sla:
  ingestion_latency:
    target: "< 30 segundos"
    p95: "25 segundos"
    p99: "40 segundos"
  
  availability:
    target: "> 99.5%"
    actual: "99.7%"
  
  data_loss:
    target: "0%"
    actual: "0.01%"  # 1 falha em 10k execuções
```

**Silver Layer:**
```yaml
sla:
  processing_latency:
    target: "< 5 minutos"
    p95: "4 minutos"
    p99: "7 minutos"
  
  data_quality:
    target: "> 95%"
    actual: "98.2%"
  
  completeness:
    target: "> 98%"
    actual: "99.1%"
```

**Gold Layer:**
```yaml
sla:
  aggregation_latency:
    target: "< 10 minutos"
    p95: "8 minutos"
    p99: "12 minutos"
  
  accuracy:
    target: "> 99%"
    actual: "99.5%"
  
  freshness:
    target: "< 15 minutos"
    actual: "12 minutos"
```

### 5.2. Data Quality Score

**Fórmula de Cálculo:**

```python
def calculate_dq_score(df):
    checks = {
        'completeness': check_completeness(df),    # 30%
        'validity': check_validity(df),            # 25%
        'consistency': check_consistency(df),      # 20%
        'accuracy': check_accuracy(df),            # 15%
        'timeliness': check_timeliness(df)         # 10%
    }
    
    weights = {
        'completeness': 0.30,
        'validity': 0.25,
        'consistency': 0.20,
        'accuracy': 0.15,
        'timeliness': 0.10
    }
    
    score = sum(checks[k] * weights[k] for k in checks)
    return score * 100  # 0-100 scale
```

**Thresholds:**
- **Excelente**: DQ Score ≥ 95%
- **Bom**: 90% ≤ DQ Score < 95%
- **Aceitável**: 85% ≤ DQ Score < 90%
- **Crítico**: DQ Score < 85% (bloqueia pipeline)

---

## 6. Documentação de Transformações

### 6.1. Transformação: bronze_to_silver

**Metadata:**
```yaml
name: "bronze_to_silver_positions"
version: "1.2.0"
author: "engenharia-dados@fia.br"
last_updated: "2025-10-15"
code_location: "src/processing/jobs/bronze_to_silver.py"
```

**Input:**
- bronze/api_positions (Parquet)
- bronze/gtfs_routes (Parquet)
- bronze/gtfs_stops (Parquet)

**Output:**
- silver/positions_cleaned (Delta Lake)

**Transformations Applied:**

1. **Deduplication**
```sql
SELECT *, ROW_NUMBER() OVER (
    PARTITION BY vehicle_id, timestamp 
    ORDER BY ingestion_timestamp DESC
) as rn
WHERE rn = 1
```

2. **Validation**
```python
valid_df = df.filter(
    (col("latitude").between(-90, 90)) &
    (col("longitude").between(-180, 180)) &
    (col("timestamp") <= current_timestamp()) &
    (col("vehicle_id").isNotNull())
)
```

3. **Enrichment**
```sql
SELECT 
    pos.*,
    routes.route_short_name,
    routes.route_long_name,
    stops.stop_id as nearest_stop_id,
    stops.stop_name as nearest_stop_name,
    ST_Distance(
        ST_Point(pos.longitude, pos.latitude),
        ST_Point(stops.stop_lon, stops.stop_lat)
    ) as distance_to_stop_meters
FROM positions pos
LEFT JOIN gtfs_routes routes ON pos.route_code = routes.route_short_name
LEFT JOIN gtfs_stops stops ON ST_Distance(...) < 50
```

4. **Derived Fields**
```python
df_with_speed = df.withColumn(
    "speed_kmh",
    (haversine_udf(
        lag("latitude").over(window),
        lag("longitude").over(window),
        col("latitude"),
        col("longitude")
    ) / (unix_timestamp("timestamp") - unix_timestamp(lag("timestamp").over(window)))) * 3.6
)
```

**Quality Checks:**
- Null check em campos críticos
- Range validation para coordenadas
- Duplicate detection
- Referential integrity com GTFS
- DQ Score calculation

**Performance:**
- Input size: ~90MB (1 hora de dados)
- Output size: ~60MB (após DQ)
- Processing time: ~3 minutos (Spark 3 executors)
- Records processed: ~450,000
- Records rejected: ~5,000 (1.1%)

---

### 6.2. Transformação: silver_to_gold

**Metadata:**
```yaml
name: "silver_to_gold_kpis"
version: "1.0.0"
author: "analytics@fia.br"
last_updated: "2025-10-10"
code_location: "src/processing/jobs/silver_to_gold.py"
```

**Input:**
- silver/positions_cleaned (Delta Lake)
- silver/trips_enriched (Delta Lake)

**Output:**
- gold/kpis_realtime (Delta Lake)
- gold/metrics_by_route (Delta Lake)
- gold/headway_analysis (Delta Lake)

**Aggregations:**

```sql
-- KPIs por rota e janela de 15 minutos
SELECT
    route_id,
    route_short_name,
    window(timestamp, '15 minutes').start as timestamp_window,
    COUNT(DISTINCT vehicle_id) as vehicles_active,
    AVG(speed_kmh) as avg_speed_kmh,
    PERCENTILE(speed_kmh, 0.5) as median_speed_kmh,
    STDDEV(speed_kmh) as speed_stddev,
    AVG(LEAD(timestamp) OVER w - timestamp) / 60 as avg_headway_minutes,
    SUM(CASE WHEN ABS(actual_arrival - scheduled_arrival) < 300 THEN 1 ELSE 0 END) 
        / COUNT(*) as punctuality_rate
FROM silver.positions_cleaned
WINDOW w AS (PARTITION BY route_id, nearest_stop_id ORDER BY timestamp)
GROUP BY route_id, route_short_name, timestamp_window
```

**Performance:**
- Input size: ~2GB (24 horas)
- Output size: ~50MB (agregado)
- Processing time: ~8 minutos
- Aggregation reduction: 97.5%

---

**Documento mantido por**: Time de Engenharia de Dados  
**Última atualização**: Outubro 2025  
**Versão**: 1.0