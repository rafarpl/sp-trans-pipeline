"""
Constantes do projeto.
Valores fixos utilizados em todo o pipeline.
"""
from enum import Enum


# ============================================================================
# PROJECT INFO
# ============================================================================
PROJECT_NAME = "sptrans-realtime-pipeline"
PROJECT_VERSION = "1.0.0"
PROJECT_DESCRIPTION = "Pipeline de dados em tempo real para monitoramento de ônibus SPTrans"


# ============================================================================
# DATA LAKE PATHS
# ============================================================================
class DataLakePath:
    """Caminhos no Data Lake (MinIO)."""
    
    # Bronze Layer (Raw Data)
    BRONZE_API_POSITIONS = "s3a://sptrans-bronze/api_positions/"
    BRONZE_GTFS_ROUTES = "s3a://sptrans-bronze/gtfs_routes/"
    BRONZE_GTFS_TRIPS = "s3a://sptrans-bronze/gtfs_trips/"
    BRONZE_GTFS_STOPS = "s3a://sptrans-bronze/gtfs_stops/"
    BRONZE_GTFS_STOP_TIMES = "s3a://sptrans-bronze/gtfs_stop_times/"
    BRONZE_GTFS_SHAPES = "s3a://sptrans-bronze/gtfs_shapes/"
    
    # Silver Layer (Cleaned Data)
    SILVER_POSITIONS_CLEANED = "s3a://sptrans-silver/positions_cleaned/"
    SILVER_TRIPS_ENRICHED = "s3a://sptrans-silver/trips_enriched/"
    SILVER_STOPS_GEOCODED = "s3a://sptrans-silver/stops_geocoded/"
    SILVER_ROUTES_METADATA = "s3a://sptrans-silver/routes_metadata/"
    
    # Gold Layer (Aggregated Data)
    GOLD_KPIS_REALTIME = "s3a://sptrans-gold/kpis_realtime/"
    GOLD_METRICS_BY_ROUTE = "s3a://sptrans-gold/metrics_by_route/"
    GOLD_METRICS_BY_HOUR = "s3a://sptrans-gold/metrics_by_hour/"
    GOLD_HEADWAY_ANALYSIS = "s3a://sptrans-gold/headway_analysis/"
    GOLD_SPEED_ANALYSIS = "s3a://sptrans-gold/speed_analysis/"
    GOLD_ANOMALIES_DETECTED = "s3a://sptrans-gold/anomalies_detected/"


# ============================================================================
# SPTRANS API
# ============================================================================
class SPTransEndpoint:
    """Endpoints da API SPTrans."""
    
    AUTENTICAR = "/Login/Autenticar"
    POSICAO = "/Posicao"
    POSICAO_LINHA = "/Posicao/Linha"
    LINHAS = "/Linha/Buscar"
    PARADAS = "/Parada/Buscar"
    PREVISAO_CHEGADA = "/Previsao/Linha"
    PREVISAO_PARADA = "/Previsao/Parada"


# ============================================================================
# COLUMNS
# ============================================================================
class BronzeColumns:
    """Colunas da camada Bronze."""
    
    # API Positions
    ROUTE_CODE = "route_code"
    ROUTE_ID = "route_id"
    DIRECTION = "direction"
    DESTINATION_0 = "destination_0"
    DESTINATION_1 = "destination_1"
    VEHICLE_ID = "vehicle_id"
    ACCESSIBLE = "accessible"
    LATITUDE = "latitude"
    LONGITUDE = "longitude"
    TIMESTAMP = "timestamp"
    INGESTION_TIMESTAMP = "ingestion_timestamp"
    PIPELINE_VERSION = "pipeline_version"
    SOURCE_SYSTEM = "source_system"


class SilverColumns:
    """Colunas da camada Silver."""
    
    # Campos base (herdados do Bronze)
    VEHICLE_ID = "vehicle_id"
    ROUTE_ID = "route_id"
    ROUTE_CODE = "route_code"
    DIRECTION = "direction"
    LATITUDE = "latitude"
    LONGITUDE = "longitude"
    TIMESTAMP = "timestamp"
    ACCESSIBLE = "accessible"
    
    # Enriquecimento GTFS
    ROUTE_SHORT_NAME = "route_short_name"
    ROUTE_LONG_NAME = "route_long_name"
    AGENCY_ID = "agency_id"
    NEAREST_STOP_ID = "nearest_stop_id"
    NEAREST_STOP_NAME = "nearest_stop_name"
    DISTANCE_TO_STOP_METERS = "distance_to_stop_meters"
    
    # Geocoding
    STREET_NAME = "street_name"
    NEIGHBORHOOD = "neighborhood"
    DISTRICT = "district"
    CITY = "city"
    
    # Campos calculados
    SPEED_KMH = "speed_kmh"
    HEADING_DEGREES = "heading_degrees"
    IS_MOVING = "is_moving"
    
    # Data Quality
    DQ_SCORE = "dq_score"
    DQ_FLAGS = "dq_flags"
    
    # Metadados
    PROCESSED_TIMESTAMP = "processed_timestamp"
    DATE = "date"


class GoldColumns:
    """Colunas da camada Gold."""
    
    # KPIs Realtime
    ROUTE_ID = "route_id"
    ROUTE_SHORT_NAME = "route_short_name"
    TIMESTAMP_WINDOW = "timestamp_window"
    VEHICLES_ACTIVE = "vehicles_active"
    AVG_SPEED_KMH = "avg_speed_kmh"
    MEDIAN_SPEED_KMH = "median_speed_kmh"
    SPEED_STDDEV = "speed_stddev"
    AVG_HEADWAY_MINUTES = "avg_headway_minutes"
    PUNCTUALITY_RATE = "punctuality_rate"
    TOTAL_TRIPS_COMPLETED = "total_trips_completed"
    TOTAL_DISTANCE_KM = "total_distance_km"
    ANOMALIES_DETECTED = "anomalies_detected"


# ============================================================================
# DATA QUALITY
# ============================================================================
class DQCheckType(Enum):
    """Tipos de verificação de qualidade de dados."""
    
    NULL_CHECK = "null_check"
    RANGE_CHECK = "range_check"
    DUPLICATE_CHECK = "duplicate_check"
    REFERENTIAL_INTEGRITY = "referential_integrity"
    FRESHNESS_CHECK = "freshness_check"
    SCHEMA_CHECK = "schema_check"
    COMPLETENESS_CHECK = "completeness_check"


class DQThreshold:
    """Thresholds de qualidade de dados."""
    
    # Score geral
    SCORE_EXCELLENT = 95.0
    SCORE_GOOD = 90.0
    SCORE_ACCEPTABLE = 85.0
    SCORE_CRITICAL = 85.0  # Abaixo disso, bloqueia pipeline
    
    # Por tipo de check
    NULL_RATE_MAX = 0.01  # Máximo 1% de nulos
    DUPLICATE_RATE_MAX = 0.001  # Máximo 0.1% de duplicatas
    COMPLETENESS_MIN = 0.98  # Mínimo 98% de completude


# ============================================================================
# GEOCODING
# ============================================================================
class GeocodingProvider(Enum):
    """Provedores de geocoding."""
    
    NOMINATIM = "nominatim"
    GOOGLE_MAPS = "google"
    MAPBOX = "mapbox"


# ============================================================================
# COORDINATE VALIDATION
# ============================================================================
class CoordinateRange:
    """Ranges válidos para coordenadas."""
    
    # Latitude
    LAT_MIN = -90.0
    LAT_MAX = 90.0
    
    # Longitude
    LON_MIN = -180.0
    LON_MAX = 180.0
    
    # São Paulo (para validação específica)
    SAO_PAULO_LAT_MIN = -24.0
    SAO_PAULO_LAT_MAX = -23.0
    SAO_PAULO_LON_MIN = -47.0
    SAO_PAULO_LON_MAX = -46.0


# ============================================================================
# SPEED VALIDATION
# ============================================================================
class SpeedRange:
    """Ranges válidos para velocidade."""
    
    MIN_KMH = 0.0
    MAX_KMH = 120.0  # Velocidade máxima realista para ônibus
    
    # Limites para anomalias
    VERY_SLOW = 5.0  # Abaixo disso, considerado parado
    VERY_FAST = 80.0  # Acima disso, suspeito
    
    # Velocidade média esperada
    EXPECTED_AVG_KMH = 15.0
    EXPECTED_STDDEV_KMH = 10.0


# ============================================================================
# TIME WINDOWS
# ============================================================================
class TimeWindow:
    """Janelas de tempo para agregações."""
    
    REALTIME = "2 minutes"
    SHORT = "15 minutes"
    MEDIUM = "1 hour"
    LONG = "1 day"
    
    # Em minutos (para cálculos)
    REALTIME_MINUTES = 2
    SHORT_MINUTES = 15
    MEDIUM_MINUTES = 60
    LONG_MINUTES = 1440


# ============================================================================
# SPARK CONFIGS
# ============================================================================
class SparkDefaults:
    """Configurações padrão do Spark."""
    
    # Particionamento
    DEFAULT_PARALLELISM = 12
    SQL_SHUFFLE_PARTITIONS = 8
    
    # Write modes
    WRITE_MODE_APPEND = "append"
    WRITE_MODE_OVERWRITE = "overwrite"
    WRITE_MODE_ERROR = "error"
    WRITE_MODE_IGNORE = "ignore"
    
    # Formatos
    FORMAT_PARQUET = "parquet"
    FORMAT_DELTA = "delta"
    FORMAT_CSV = "csv"
    FORMAT_JSON = "json"
    
    # Compression
    COMPRESSION_SNAPPY = "snappy"
    COMPRESSION_GZIP = "gzip"
    COMPRESSION_NONE = "none"


# ============================================================================
# POSTGRES TABLES
# ============================================================================
class PostgresTable:
    """Nomes de tabelas no PostgreSQL."""
    
    # Serving Schema
    KPIS_REALTIME = "serving.kpis_realtime"
    ROUTE_METRICS = "serving.route_metrics"
    VEHICLE_STATUS = "serving.vehicle_status"
    ALERTS_ACTIVE = "serving.alerts_active"
    HISTORICAL_AGGREGATES = "serving.historical_aggregates"
    
    # Metadata Schema
    PIPELINE_EXECUTION_LOG = "metadata.pipeline_execution_log"
    DATA_QUALITY_METRICS = "metadata.data_quality_metrics"


# ============================================================================
# REDIS KEYS
# ============================================================================
class RedisKey:
    """Padrões de chaves no Redis."""
    
    # Prefixos
    PREFIX_POSITION = "bus:pos:"
    PREFIX_KPI = "kpi:route:"
    PREFIX_ALERT = "alert:"
    PREFIX_CACHE = "cache:"
    
    # TTL (em segundos)
    TTL_POSITION = 120  # 2 minutos
    TTL_KPI = 900  # 15 minutos
    TTL_ALERT = 3600  # 1 hora
    TTL_CACHE = 300  # 5 minutos


# ============================================================================
# METRICS
# ============================================================================
class MetricName:
    """Nomes de métricas Prometheus."""
    
    # Pipeline
    PIPELINE_RECORDS_PROCESSED = "pipeline_records_processed_total"
    PIPELINE_ERRORS = "pipeline_errors_total"
    PIPELINE_DURATION = "pipeline_duration_seconds"
    
    # Data Quality
    DATA_QUALITY_SCORE = "data_quality_score"
    DATA_QUALITY_CHECKS_PASSED = "data_quality_checks_passed_total"
    DATA_QUALITY_CHECKS_FAILED = "data_quality_checks_failed_total"
    
    # API
    API_REQUEST_DURATION = "api_request_duration_seconds"
    API_REQUEST_ERRORS = "api_request_errors_total"
    
    # Spark
    SPARK_JOB_DURATION = "spark_job_duration_seconds"
    SPARK_EXECUTOR_MEMORY_USED = "spark_executor_memory_used_bytes"


# ============================================================================
# LOGGING
# ============================================================================
class LogLevel:
    """Níveis de log."""
    
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


# ============================================================================
# FEATURE FLAGS
# ============================================================================
class FeatureFlag:
    """Feature flags do projeto."""
    
    KAFKA_STREAMING = "kafka_streaming"
    ML_PREDICTIONS = "ml_predictions"
    REAL_TIME_ALERTS = "real_time_alerts"
    ADVANCED_ANALYTICS = "advanced_analytics"
    GEOCODING = "geocoding"


# ============================================================================
# ERROR CODES
# ============================================================================
class ErrorCode:
    """Códigos de erro customizados."""
    
    # API
    API_AUTHENTICATION_FAILED = "E001"
    API_TIMEOUT = "E002"
    API_RATE_LIMIT = "E003"
    API_INVALID_RESPONSE = "E004"
    
    # Data Quality
    DQ_SCORE_BELOW_THRESHOLD = "E101"
    DQ_NULL_VALUES = "E102"
    DQ_DUPLICATE_RECORDS = "E103"
    DQ_INVALID_COORDINATES = "E104"
    
    # Storage
    STORAGE_CONNECTION_FAILED = "E201"
    STORAGE_WRITE_FAILED = "E202"
    STORAGE_READ_FAILED = "E203"
    
    # Spark
    SPARK_JOB_FAILED = "E301"
    SPARK_OUT_OF_MEMORY = "E302"


# ============================================================================
# ALERT SEVERITY
# ============================================================================
class AlertSeverity(Enum):
    """Severidade de alertas."""
    
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


if __name__ == '__main__':
    # Teste de constantes
    print(f"Project: {PROJECT_NAME} v{PROJECT_VERSION}")
    print(f"Bronze Path: {DataLakePath.BRONZE_API_POSITIONS}")
    print(f"DQ Threshold: {DQThreshold.SCORE_CRITICAL}")
    print(f"Speed Range: {SpeedRange.MIN_KMH} - {SpeedRange.MAX_KMH} km/h")