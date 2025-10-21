# 📖 Dicionário de Dados - SPTrans Real-Time Pipeline

> **Catálogo completo de todas as estruturas de dados do sistema**  
> **Versão**: 1.0 | **Data**: Outubro 2025

---

## 📋 Índice

1. [Fontes de Dados](#1-fontes-de-dados)
2. [Bronze Layer (Raw Data)](#2-bronze-layer-raw-data)
3. [Silver Layer (Cleaned Data)](#3-silver-layer-cleaned-data)
4. [Gold Layer (Aggregated Data)](#4-gold-layer-aggregated-data)
5. [Serving Layer (PostgreSQL)](#5-serving-layer-postgresql)
6. [Metadados e Controle](#6-metadados-e-controle)

---

## 1. Fontes de Dados

### 1.1. API SPTrans - Endpoint /Posicao

**URL:** `https://api.olhovivo.sptrans.com.br/v2.1/Posicao`  
**Método:** GET  
**Autenticação:** Bearer Token  
**Frequência:** A cada 2 minutos

**Schema JSON Response:**

```json
{
  "hr": "2025-10-20 14:30:00",  // Horário da resposta
  "l": [                         // Array de linhas
    {
      "c": "8000",               // Código da linha (string)
      "cl": 1234,                // Código da linha (numérico)
      "sl": 1,                   // Sentido (1 ou 2)
      "lt0": "Terminal A",       // Letreiro destino 0
      "lt1": "Terminal B",       // Letreiro destino 1
      "qv": 45,                  // Quantidade de veículos
      "vs": [                    // Array de veículos
        {
          "p": 12345,            // Prefixo do veículo
          "a": true,             // Acessível (boolean)
          "ta": "2025-10-20 14:29:45",  // Timestamp atualização
          "py": -23.550520,      // Latitude
          "px": -46.633308       // Longitude
        }
      ]
    }
  ]
}
```

**Campos Detalhados:**

| Campo | Tipo | Obrigatório | Descrição | Exemplo |
|-------|------|-------------|-----------|---------|
| `hr` | string | Sim | Horário da resposta (formato: YYYY-MM-DD HH:MM:SS) | "2025-10-20 14:30:00" |
| `l` | array | Sim | Array de linhas com veículos ativos | `[{...}, {...}]` |
| `l[].c` | string | Sim | Código da linha (letreiro) | "8000-10" |
| `l[].cl` | integer | Sim | Código numérico da linha | 1234 |
| `l[].sl` | integer | Sim | Sentido: 1=Principal→Secundário, 2=Secundário→Principal | 1 |
| `l[].lt0` | string | Sim | Letreiro destino sentido 0 | "Terminal A" |
| `l[].lt1` | string | Sim | Letreiro destino sentido 1 | "Terminal B" |
| `l[].qv` | integer | Sim | Quantidade de veículos localizados nesta linha | 45 |
| `l[].vs` | array | Sim | Array de veículos da linha | `[{...}]` |
| `l[].vs[].p` | integer | Sim | Prefixo do veículo (identificador único) | 12345 |
| `l[].vs[].a` | boolean | Sim | Veículo acessível (adaptado para PCD) | true |
| `l[].vs[].ta` | string | Sim | Timestamp da última atualização do GPS | "2025-10-20 14:29:45" |
| `l[].vs[].py` | float | Sim | Latitude (WGS84) | -23.550520 |
| `l[].vs[].px` | float | Sim | Longitude (WGS84) | -46.633308 |

**Regras de Validação:**
- Latitude: -90 ≤ py ≤ 90
- Longitude: -180 ≤ px ≤ 180
- Timestamp: Não pode ser futuro
- Prefixo: Deve ser único por timestamp

---

### 1.2. GTFS SPTrans (Static Data)

**URL:** https://www.sptrans.com.br/desenvolvedores/  
**Formato:** ZIP com arquivos TXT (CSV)  
**Atualização:** Diária (3:00 AM)

#### 1.2.1. routes.txt

**Descrição:** Informações sobre rotas/linhas de ônibus

| Campo | Tipo | Obrigatório | Descrição | Exemplo |
|-------|------|-------------|-----------|---------|
| `route_id` | string | Sim | Identificador único da rota | "1234" |
| `agency_id` | string | Sim | ID da agência operadora | "1" |
| `route_short_name` | string | Sim | Nome curto da rota (letreiro) | "8000-10" |
| `route_long_name` | string | Sim | Nome completo da rota | "Terminal A - Terminal B" |
| `route_type` | integer | Sim | Tipo de transporte (3 = ônibus) | 3 |
| `route_color` | string | Não | Cor da linha em hex | "FF0000" |
| `route_text_color` | string | Não | Cor do texto em hex | "FFFFFF" |

**Exemplo:**
```csv
route_id,agency_id,route_short_name,route_long_name,route_type
1234,1,8000-10,Terminal A - Terminal B,3
```

#### 1.2.2. trips.txt

**Descrição:** Viagens programadas (instância de uma rota em horário específico)

| Campo | Tipo | Obrigatório | Descrição | Exemplo |
|-------|------|-------------|-----------|---------|
| `trip_id` | string | Sim | Identificador único da viagem | "trip_001" |
| `route_id` | string | Sim | ID da rota (FK para routes.txt) | "1234" |
| `service_id` | string | Sim | ID do calendário (dia útil/fim de semana) | "WD" |
| `trip_headsign` | string | Não | Destino mostrado no ônibus | "Terminal B" |
| `direction_id` | integer | Não | Direção: 0 ou 1 | 1 |
| `shape_id` | string | Não | ID do trajeto (FK para shapes.txt) | "shape_8000_1" |
| `wheelchair_accessible` | integer | Não | 1=acessível, 0=não acessível | 1 |

**Exemplo:**
```csv
trip_id,route_id,service_id,trip_headsign,direction_id,shape_id
trip_001,1234,WD,Terminal B,1,shape_8000_1
```

#### 1.2.3. stops.txt

**Descrição:** Pontos de parada de ônibus

| Campo | Tipo | Obrigatório | Descrição | Exemplo |
|-------|------|-------------|-----------|---------|
| `stop_id` | string | Sim | Identificador único da parada | "12345" |
| `stop_code` | string | Não | Código da parada (visível ao público) | "P123" |
| `stop_name` | string | Sim | Nome da parada | "Av. Paulista, 1000" |
| `stop_desc` | string | Não | Descrição adicional | "Em frente ao MASP" |
| `stop_lat` | float | Sim | Latitude da parada (WGS84) | -23.561684 |
| `stop_lon` | float | Sim | Longitude da parada (WGS84) | -46.656423 |
| `zone_id` | string | Não | ID da zona tarifária | "Z1" |
| `stop_url` | string | Não | URL com informações da parada | "https://..." |
| `wheelchair_boarding` | integer | Não | 1=acessível, 0=não | 1 |

**Exemplo:**
```csv
stop_id,stop_name,stop_lat,stop_lon
12345,Av. Paulista - 1000,-23.561684,-46.656423
```

#### 1.2.4. stop_times.txt

**Descrição:** Horários de chegada/partida em cada parada

| Campo | Tipo | Obrigatório | Descrição | Exemplo |
|-------|------|-------------|-----------|---------|
| `trip_id` | string | Sim | ID da viagem (FK para trips.txt) | "trip_001" |
| `arrival_time` | string | Sim | Horário de chegada (HH:MM:SS) | "06:00:00" |
| `departure_time` | string | Sim | Horário de partida (HH:MM:SS) | "06:00:00" |
| `stop_id` | string | Sim | ID da parada (FK para stops.txt) | "12345" |
| `stop_sequence` | integer | Sim | Ordem da parada na viagem | 1 |
| `stop_headsign` | string | Não | Destino mostrado nesta parada | "Terminal B" |
| `pickup_type` | integer | Não | 0=regular, 1=sem embarque | 0 |
| `drop_off_type` | integer | Não | 0=regular, 1=sem desembarque | 0 |

**Exemplo:**
```csv
trip_id,arrival_time,departure_time,stop_id,stop_sequence
trip_001,06:00:00,06:00:00,12345,1
```

#### 1.2.5. shapes.txt

**Descrição:** Trajeto geográfico das rotas (pontos do caminho)

| Campo | Tipo | Obrigatório | Descrição | Exemplo |
|-------|------|-------------|-----------|---------|
| `shape_id` | string | Sim | Identificador do trajeto | "shape_8000_1" |
| `shape_pt_lat` | float | Sim | Latitude do ponto | -23.561684 |
| `shape_pt_lon` | float | Sim | Longitude do ponto | -46.656423 |
| `shape_pt_sequence` | integer | Sim | Ordem do ponto no trajeto | 1 |
| `shape_dist_traveled` | float | Não | Distância percorrida até este ponto (metros) | 150.5 |

**Exemplo:**
```csv
shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence
shape_8000_1,-23.561684,-46.656423,1
```

---

## 2. Bronze Layer (Raw Data)

### 2.1. api_positions (Parquet)

**Localização:** `s3a://sptrans-bronze/api_positions/`  
**Particionamento:** Hive-style por `year/month/day/hour`  
**Formato:** Apache Parquet (Snappy compression)

**Schema PySpark:**

```python
from pyspark.sql.types import *

bronze_schema = StructType([
    StructField("route_code", StringType(), nullable=False),
    StructField("route_id", IntegerType(), nullable=False),
    StructField("direction", IntegerType(), nullable=False),
    StructField("destination_0", StringType(), nullable=True),
    StructField("destination_1", StringType(), nullable=True),
    StructField("vehicle_id", IntegerType(), nullable=False),
    StructField("accessible", BooleanType(), nullable=False),
    StructField("latitude", DoubleType(), nullable=False),
    StructField("longitude", DoubleType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
    # Metadados
    StructField("ingestion_timestamp", TimestampType(), nullable=False),
    StructField("pipeline_version", StringType(), nullable=False),
    StructField("source_system", StringType(), nullable=False)
])
```

**Colunas:**

| Coluna | Tipo | Nullable | Descrição | Fonte API |
|--------|------|----------|-----------|-----------|
| `route_code` | string | Não | Código da linha (letreiro) | `l[].c` |
| `route_id` | int | Não | ID numérico da rota | `l[].cl` |
| `direction` | int | Não | Sentido da viagem (1 ou 2) | `l[].sl` |
| `destination_0` | string | Sim | Destino sentido 0 | `l[].lt0` |
| `destination_1` | string | Sim | Destino sentido 1 | `l[].lt1` |
| `vehicle_id` | int | Não | Prefixo do veículo | `l[].vs[].p` |
| `accessible` | boolean | Não | Veículo acessível | `l[].vs[].a` |
| `latitude` | double | Não | Latitude WGS84 | `l[].vs[].py` |
| `longitude` | double | Não | Longitude WGS84 | `l[].vs[].px` |
| `timestamp` | timestamp | Não | Timestamp do GPS | `l[].vs[].ta` |
| `ingestion_timestamp` | timestamp | Não | Quando foi ingerido no pipeline | `current_timestamp()` |
| `pipeline_version` | string | Não | Versão do pipeline | "1.0" |
| `source_system` | string | Não | Sistema de origem | "sptrans_api" |

**Particionamento:**
```
s3a://sptrans-bronze/api_positions/
  └── year=2025/
      └── month=10/
          └── day=20/
              └── hour=14/
                  ├── part-00000.parquet
                  ├── part-00001.parquet
                  └── part-00002.parquet
```

**Tamanho Estimado:**
- Registros por hora: 450,000 (15k veículos × 30 coletas)
- Tamanho por hora (comprimido): ~90 MB
- Tamanho por dia: ~2.16 GB
- Tamanho por mês: ~65 GB

---

### 2.2. gtfs_* (Parquet)

**Localização:** `s3a://sptrans-bronze/gtfs_<table>/`  
**Atualização:** Diária (substitui arquivo anterior)  
**Formato:** Apache Parquet

Tabelas:
- `gtfs_routes/`
- `gtfs_trips/`
- `gtfs_stops/`
- `gtfs_stop_times/`
- `gtfs_shapes/`

Schemas seguem especificação GTFS (ver seção 1.2)

---

## 3. Silver Layer (Cleaned Data)

### 3.1. positions_cleaned (Delta Lake)

**Localização:** `s3a://sptrans-silver/positions_cleaned/`  
**Formato:** Delta Lake (Parquet + Transaction Log)  
**Particionamento:** Por `date` (YYYY-MM-DD)

**Schema:**

```python
silver_schema = StructType([
    # Campos originais (validados)
    StructField("vehicle_id", IntegerType(), nullable=False),
    StructField("route_id", IntegerType(), nullable=False),
    StructField("route_code", StringType(), nullable=False),
    StructField("direction", IntegerType(), nullable=False),
    StructField("latitude", DoubleType(), nullable=False),
    StructField("longitude", DoubleType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("accessible", BooleanType(), nullable=False),
    
    # Enriquecimento com GTFS
    StructField("route_short_name", StringType(), nullable=True),
    StructField("route_long_name", StringType(), nullable=True),
    StructField("agency_id", StringType(), nullable=True),
    StructField("nearest_stop_id", StringType(), nullable=True),
    StructField("nearest_stop_name", StringType(), nullable=True),
    StructField("distance_to_stop_meters", DoubleType(), nullable=True),
    
    # Geocoding reverso
    StructField("street_name", StringType(), nullable=True),
    StructField("neighborhood", StringType(), nullable=True),
    StructField("district", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    
    # Campos calculados
    StructField("speed_kmh", DoubleType(), nullable=True),
    StructField("heading_degrees", DoubleType(), nullable=True),
    StructField("is_moving", BooleanType(), nullable=True),
    
    # Data Quality
    StructField("dq_score", DoubleType(), nullable=False),
    StructField("dq_flags", ArrayType(StringType()), nullable=True),
    
    # Metadados
    StructField("processed_timestamp", TimestampType(), nullable=False),
    StructField("date", DateType(), nullable=False)
])
```

**Colunas Detalhadas:**

| Coluna | Tipo | Descrição | Lógica de Cálculo |
|--------|------|-----------|-------------------|
| `speed_kmh` | double | Velocidade em km/h | Haversine(lat1,lon1,lat2,lon2) / time_diff |
| `heading_degrees` | double | Direção do movimento (0-360°) | ATAN2(Δlat, Δlon) |
| `is_moving` | boolean | Veículo em movimento | speed_kmh > 5 |
| `distance_to_stop_meters` | double | Distância até parada mais próxima | ST_Distance(vehicle_point, stop_point) |
| `dq_score` | double | Score de qualidade (0-100) | % de checks aprovados |
| `dq_flags` | array | Lista de problemas detectados | ["null_street", "low_speed"] |

**Transformações Aplicadas:**

1. **Deduplicação:**
```sql
SELECT *, ROW_NUMBER() OVER (
    PARTITION BY vehicle_id, timestamp 
    ORDER BY ingestion_timestamp DESC
) as rn
WHERE rn = 1
```

2. **Validação de Coordenadas:**
```sql
WHERE latitude BETWEEN -90 AND 90
  AND longitude BETWEEN -180 AND -180
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL
```

3. **Join com GTFS Routes:**
```sql
LEFT JOIN gtfs_routes 
ON silver.route_code = gtfs.route_short_name
```

4. **Spatial Join com Stops:**
```sql
-- Encontra parada mais próxima (raio de 50m)
SELECT 
    vehicle.*,
    stop.stop_id as nearest_stop_id,
    ST_Distance(
        ST_Point(vehicle.longitude, vehicle.latitude),
        ST_Point(stop.stop_lon, stop.stop_lat)
    ) as distance_meters
FROM positions vehicle
LEFT JOIN gtfs_stops stop
WHERE distance_meters < 50
```

5. **Cálculo de Velocidade:**
```sql
SELECT
    *,
    (HAVERSINE(
        lag(latitude) OVER w,
        lag(longitude) OVER w,
        latitude,
        longitude
    ) / (unix_timestamp(timestamp) - unix_timestamp(lag(timestamp) OVER w))) * 3.6 as speed_kmh
WINDOW w AS (PARTITION BY vehicle_id ORDER BY timestamp)
```

**Delta Lake Operations:**

```python
# MERGE para upsert
deltaTable.alias("target").merge(
    updates.alias("source"),
    "target.vehicle_id = source.vehicle_id AND target.timestamp = source.timestamp"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

---

### 3.2. trips_enriched (Delta Lake)

**Localização:** `s3a://sptrans-silver/trips_enriched/`

**Descrição:** Viagens detectadas a partir de posições sequenciais do mesmo veículo

**Schema:**

```python
trips_schema = StructType([
    StructField("trip_detected_id", StringType(), nullable=False),
    StructField("vehicle_id", IntegerType(), nullable=False),
    StructField("route_id", IntegerType(), nullable=False),
    StructField("trip_start_time", TimestampType(), nullable=False),
    StructField("trip_end_time", TimestampType(), nullable=True),
    StructField("origin_stop_id", StringType(), nullable=True),
    StructField("destination_stop_id", StringType(), nullable=True),
    StructField("total_distance_km", DoubleType(), nullable=True),
    StructField("total_duration_minutes", DoubleType(), nullable=True),
    StructField("avg_speed_kmh", DoubleType(), nullable=True),
    StructField("stops_visited", ArrayType(StringType()), nullable=True),
    StructField("status", StringType(), nullable=False)  // 'in_progress', 'completed'
])
```

**Lógica de Detecção de Viagem:**

```sql
-- Detecta início de viagem (veículo parado em terminal por >5 min e depois se move)
SELECT 
    vehicle_id,
    MIN(timestamp) as trip_start_time,
    FIRST(nearest_stop_id) as origin_stop_id
FROM positions_cleaned
WHERE is_moving = false 
  AND distance_to_stop_meters < 20  -- Parado no terminal
GROUP BY vehicle_id, 
    CASE WHEN is_moving THEN 1 ELSE 0 END  -- Sessão de movimento
```

---

## 4. Gold Layer (Aggregated Data)

### 4.1. kpis_realtime (Delta Lake)

**Localização:** `s3a://sptrans-gold/kpis_realtime/`  
**Granularidade:** Por rota, a cada 15 minutos

**Schema:**

```python
kpis_schema = StructType([
    StructField("route_id", IntegerType(), nullable=False),
    StructField("route_short_name", StringType(), nullable=False),
    StructField("timestamp_window", TimestampType(), nullable=False),
    StructField("vehicles_active", IntegerType(), nullable=False),
    StructField("avg_speed_kmh", DoubleType(), nullable=True),
    StructField("median_speed_kmh", DoubleType(), nullable=True),
    StructField("speed_stddev", DoubleType(), nullable=True),
    StructField("avg_headway_minutes", DoubleType(), nullable=True),
    StructField("punctuality_rate", DoubleType(), nullable=True),
    StructField("total_trips_completed", IntegerType(), nullable=True),
    StructField("total_distance_km", DoubleType(), nullable=True),
    StructField("anomalies_detected", IntegerType(), nullable=True)
])
```

**Cálculos:**

```sql
SELECT
    route_id,
    route_short_name,
    window(timestamp, '15 minutes').start as timestamp_window,
    COUNT(DISTINCT vehicle_id) as vehicles_active,
    AVG(speed_kmh) as avg_speed_kmh,
    PERCENTILE(speed_kmh, 0.5) as median_speed_kmh,
    STDDEV(speed_kmh) as speed_stddev,
    -- Headway: tempo médio entre ônibus consecutivos
    AVG(
        LEAD(timestamp) OVER (PARTITION BY route_id, nearest_stop_id ORDER BY timestamp) 
        - timestamp
    ) / 60 as avg_headway_minutes,
    -- Pontualidade: % de chegadas dentro de 5 min do programado
    SUM(CASE WHEN ABS(actual_arrival - scheduled_arrival) < INTERVAL 5 MINUTES THEN 1 ELSE 0 END) 
    / COUNT(*) as punctuality_rate
FROM positions_cleaned
GROUP BY route_id, route_short_name, timestamp_window
```

---

### 4.2. metrics_by_route (Delta Lake)

**Localização:** `s3a://sptrans-gold/metrics_by_route/`  
**Granularidade:** Por rota, agregação diária

**Schema:**

```python
metrics_route_schema = StructType([
    StructField("date", DateType(), nullable=False),
    StructField("route_id", IntegerType(), nullable=False),
    StructField("route_short_name", StringType(), nullable=False),
    StructField("total_vehicles", IntegerType(), nullable=False),
    StructField("total_trips", IntegerType(), nullable=False),
    StructField("total_distance_km", DoubleType(), nullable=False),
    StructField("avg_trip_duration_min", DoubleType(), nullable=True),
    StructField("avg_speed_kmh", DoubleType(), nullable=True),
    StructField("peak_vehicles_count", IntegerType(), nullable=True),
    StructField("peak_hour", IntegerType(), nullable=True),
    StructField("punctuality_rate", DoubleType(), nullable=True),
    StructField("reliability_score", DoubleType(), nullable=True)
])
```

---

### 4.3. headway_analysis (Delta Lake)

**Localização:** `s3a://sptrans-gold/headway_analysis/`

**Schema:**

```python
headway_schema = StructType([
    StructField("route_id", IntegerType(), nullable=False),
    StructField("stop_id", StringType(), nullable=False),
    StructField("hour_of_day", IntegerType(), nullable=False),
    StructField("avg_headway_minutes", DoubleType(), nullable=False),
    StructField("scheduled_headway_minutes", DoubleType(), nullable=True),
    StructField("headway_variance", DoubleType(), nullable=False),
    StructField("regularity_score", DoubleType(), nullable=False)  // 0-100
])
```

**Cálculo Regularity Score:**
```python
# Score baseado na consistência do headway
regularity_score = 100 * (1 - (headway_variance / avg_headway))
```

---

## 5. Serving Layer (PostgreSQL)

### 5.1. serving.kpis_realtime

**Tabela:** `sptrans.serving.kpis_realtime`  
**Tipo:** Table (com índices)  
**Retenção:** 7 dias (rolling window)

**DDL:**

```sql
CREATE TABLE serving.kpis_realtime (
    route_id INTEGER NOT NULL,
    route_short_name VARCHAR(50) NOT NULL,
    timestamp_window TIMESTAMP NOT NULL,
    vehicles_active INTEGER NOT NULL,
    avg_speed_kmh DECIMAL(5,2),
    median_speed_kmh DECIMAL(5,2),
    speed_stddev DECIMAL(5,2),
    avg_headway_minutes DECIMAL(5,2),
    punctuality_rate DECIMAL(5,4),
    total_trips_completed INTEGER,
    total_distance_km DECIMAL(10,2),
    anomalies_detected INTEGER,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (route_id, timestamp_window)
) PARTITION BY RANGE (timestamp_window);

-- Partições por dia
CREATE TABLE serving.kpis_realtime_2025_10_20 
PARTITION OF serving.kpis_realtime
FOR VALUES FROM ('2025-10-20') TO ('2025-10-21');

-- Índices
CREATE INDEX idx_kpis_timestamp ON serving.kpis_realtime(timestamp_window DESC);
CREATE INDEX idx_kpis_route ON serving.kpis_realtime(route_short_name);
```

---

### 5.2. serving.v_dashboard_summary

**Tipo:** Materialized View  
**Refresh:** A cada 15 minutos

**DDL:**

```sql
CREATE MATERIALIZED VIEW serving.v_dashboard_summary AS
SELECT
    COUNT(DISTINCT route_id) as total_routes_active,
    SUM(vehicles_active) as total_vehicles_active,
    AVG(avg_speed_kmh) as system_avg_speed,
    AVG(punctuality_rate) as system_punctuality,
    MAX(timestamp_window) as last_update
FROM serving.kpis_realtime
WHERE timestamp_window >= NOW() - INTERVAL '1 hour';

CREATE UNIQUE INDEX ON serving.v_dashboard_summary (last_update);
```

---

## 6. Metadados e Controle

### 6.1. metadata.pipeline_execution_log

**Descrição:** Log de execução de cada job do pipeline

```sql
CREATE TABLE metadata.pipeline_execution_log (
    execution_id SERIAL PRIMARY KEY,
    dag_id VARCHAR(100) NOT NULL,
    task_id VARCHAR(100) NOT NULL,
    execution_date TIMESTAMP NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(20) NOT NULL,  -- 'running', 'success', 'failed'
    records_processed BIGINT,
    records_failed BIGINT,
    error_message TEXT,
    spark_app_id VARCHAR(200)
);
```

---

### 6.2. metadata.data_quality_metrics

**Descrição:** Métricas de qualidade de dados por execução

```sql
CREATE TABLE metadata.data_quality_metrics (
    metric_id SERIAL PRIMARY KEY,
    execution_id INTEGER REFERENCES metadata.pipeline_execution_log(execution_id),
    layer VARCHAR(20) NOT NULL,  -- 'bronze', 'silver', 'gold'
    table_name VARCHAR(100) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value DECIMAL(10,4) NOT NULL,
    threshold_value DECIMAL(10,4),
    passed BOOLEAN NOT NULL,
    measured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Métricas Coletadas:**
- `null_rate`: % de valores nulos em campos críticos
- `duplicate_rate`: % de registros duplicados
- `outlier_rate`: % de valores fora do range esperado
- `completeness`: % de campos preenchidos
- `timeliness`: Latência entre evento e ingestão

---

**Documento mantido por**: Time de Engenharia de Dados  
**Última atualização**: Outubro 2025  
**Versão**: 1.0