"""
Constantes do Sistema SPTrans Pipeline.

Define constantes usadas em todo o pipeline, incluindo configurações
de API, estrutura de dados, timeouts e limites.
"""

from enum import Enum
from typing import Final

# =============================================================================
# API SPTrans Configuration
# =============================================================================

# Base URLs
SPTRANS_API_BASE_URL: Final[str] = "http://api.olhovivo.sptrans.com.br/v2.1"
SPTRANS_GTFS_URL: Final[str] = "https://www.sptrans.com.br/umbraco/surface/googletransit/downloadgtfs"

# API Endpoints
API_ENDPOINTS: Final[dict] = {
    "authenticate": "/Login/Autenticar",
    "positions": "/Posicao",
    "positions_by_line": "/Posicao/Linha",
    "lines": "/Linha/Buscar",
    "line_details": "/Linha/CarregarDetalhes",
    "stops": "/Parada/Buscar",
    "stop_details": "/Parada/CarregarDetalhes",
    "corridors": "/Corredor",
    "forecast": "/Previsao",
    "forecast_by_line": "/Previsao/Linha",
    "forecast_by_stop": "/Previsao/Parada",
}

# API Limits
API_RATE_LIMIT_PER_MINUTE: Final[int] = 1000
API_TIMEOUT_SECONDS: Final[int] = 30
API_MAX_RETRIES: Final[int] = 3
API_RETRY_BACKOFF_FACTOR: Final[float] = 2.0

# API Response Codes
API_SUCCESS_CODE: Final[int] = 200
API_AUTH_REQUIRED_CODE: Final[int] = 401
API_RATE_LIMIT_CODE: Final[int] = 429
API_SERVER_ERROR_CODE: Final[int] = 500

# =============================================================================
# Data Lake Structure (MinIO/S3)
# =============================================================================

# Buckets
BUCKET_BRONZE: Final[str] = "bronze"
BUCKET_SILVER: Final[str] = "silver"
BUCKET_GOLD: Final[str] = "gold"
BUCKET_GTFS: Final[str] = "gtfs"
BUCKET_BACKUPS: Final[str] = "backups"

# Prefixes (folders inside buckets)
PREFIX_API_RAW: Final[str] = "api/raw"
PREFIX_API_POSITIONS: Final[str] = "api/positions"
PREFIX_API_LINES: Final[str] = "api/lines"
PREFIX_API_STOPS: Final[str] = "api/stops"
PREFIX_GTFS_STATIC: Final[str] = "gtfs/static"

# Partition Columns
PARTITION_COLS_BRONZE: Final[list] = ["year", "month", "day", "hour"]
PARTITION_COLS_SILVER: Final[list] = ["year", "month", "day"]
PARTITION_COLS_GOLD: Final[list] = ["year", "month"]

# =============================================================================
# Database Configuration (PostgreSQL)
# =============================================================================

# Schemas
SCHEMA_SERVING: Final[str] = "serving"
SCHEMA_CONTROL: Final[str] = "control"
SCHEMA_MONITORING: Final[str] = "monitoring"

# Tables (Serving Layer)
TABLE_POSITIONS_REALTIME: Final[str] = "positions_realtime"
TABLE_LINES_METRICS: Final[str] = "lines_metrics"
TABLE_STOPS_METRICS: Final[str] = "stops_metrics"
TABLE_FLEET_STATUS: Final[str] = "fleet_status"
TABLE_CORRIDORS_PERFORMANCE: Final[str] = "corridors_performance"
TABLE_HOURLY_AGGREGATES: Final[str] = "hourly_aggregates"
TABLE_DAILY_AGGREGATES: Final[str] = "daily_aggregates"

# Materialized Views
MV_LINES_CURRENT: Final[str] = "mv_lines_current"
MV_STOPS_CURRENT: Final[str] = "mv_stops_current"
MV_FLEET_SUMMARY: Final[str] = "mv_fleet_summary"
MV_PERFORMANCE_DASHBOARD: Final[str] = "mv_performance_dashboard"

# Control Tables
TABLE_JOB_EXECUTIONS: Final[str] = "job_executions"
TABLE_DATA_QUALITY: Final[str] = "data_quality_checks"
TABLE_PIPELINE_METRICS: Final[str] = "pipeline_metrics"

# =============================================================================
# Spark Configuration
# =============================================================================

# Application Names
SPARK_APP_PREFIX: Final[str] = "SPTrans"
SPARK_APP_BRONZE: Final[str] = f"{SPARK_APP_PREFIX}_Bronze"
SPARK_APP_SILVER: Final[str] = f"{SPARK_APP_PREFIX}_Silver"
SPARK_APP_GOLD: Final[str] = f"{SPARK_APP_PREFIX}_Gold"

# Delta Lake
DELTA_MERGE_SCHEMA: Final[bool] = True
DELTA_OPTIMIZE_ENABLED: Final[bool] = True
DELTA_VACUUM_RETENTION_HOURS: Final[int] = 168  # 7 days

# Spark Tuning
SPARK_SHUFFLE_PARTITIONS: Final[int] = 200
SPARK_DEFAULT_PARALLELISM: Final[int] = 100
SPARK_DRIVER_MEMORY: Final[str] = "4g"
SPARK_EXECUTOR_MEMORY: Final[str] = "4g"
SPARK_EXECUTOR_CORES: Final[int] = 2

# =============================================================================
# Data Quality Configuration
# =============================================================================

# Thresholds
DQ_MIN_SUCCESS_RATE: Final[float] = 0.95  # 95%
DQ_MAX_NULL_RATE: Final[float] = 0.05  # 5%
DQ_MAX_DUPLICATE_RATE: Final[float] = 0.01  # 1%
DQ_MIN_FRESHNESS_MINUTES: Final[int] = 10

# Validation Rules
DQ_REQUIRED_FIELDS_POSITION: Final[list] = [
    "vehicle_id",
    "line_id",
    "latitude",
    "longitude",
    "timestamp",
]

DQ_LATITUDE_RANGE: Final[tuple] = (-23.9, -23.3)  # São Paulo bounds
DQ_LONGITUDE_RANGE: Final[tuple] = (-46.9, -46.3)

# =============================================================================
# Pipeline Scheduling
# =============================================================================

# Airflow DAG IDs
DAG_API_INGESTION: Final[str] = "sptrans_api_ingestion"
DAG_GTFS_INGESTION: Final[str] = "sptrans_gtfs_ingestion"
DAG_BRONZE_TO_SILVER: Final[str] = "sptrans_bronze_to_silver"
DAG_SILVER_TO_GOLD: Final[str] = "sptrans_silver_to_gold"
DAG_GOLD_TO_SERVING: Final[str] = "sptrans_gold_to_serving"
DAG_DATA_QUALITY: Final[str] = "sptrans_data_quality"
DAG_MAINTENANCE: Final[str] = "sptrans_maintenance"

# Schedule Intervals (cron format)
SCHEDULE_API_INGESTION: Final[str] = "*/3 * * * *"  # Every 3 minutes
SCHEDULE_GTFS_INGESTION: Final[str] = "0 2 * * *"  # Daily at 2 AM
SCHEDULE_BRONZE_TO_SILVER: Final[str] = "*/5 * * * *"  # Every 5 minutes
SCHEDULE_SILVER_TO_GOLD: Final[str] = "*/15 * * * *"  # Every 15 minutes
SCHEDULE_GOLD_TO_SERVING: Final[str] = "*/10 * * * *"  # Every 10 minutes
SCHEDULE_DATA_QUALITY: Final[str] = "0 */1 * * *"  # Every hour
SCHEDULE_MAINTENANCE: Final[str] = "0 3 * * 0"  # Weekly on Sunday at 3 AM

# =============================================================================
# Business Metrics & KPIs
# =============================================================================

# Fleet Metrics
EXPECTED_FLEET_SIZE: Final[int] = 15000
MIN_OPERATIONAL_FLEET: Final[int] = 12000  # 80% of expected

# Service Level Metrics
TARGET_HEADWAY_MINUTES: Final[dict] = {
    "peak": 10,  # Horário de pico
    "off_peak": 20,  # Fora de pico
    "night": 30,  # Noturno
}

# Speed Thresholds (km/h)
SPEED_MIN_OPERATIONAL: Final[float] = 5.0
SPEED_MAX_REASONABLE: Final[float] = 80.0
SPEED_AVERAGE_TARGET: Final[float] = 18.0

# Congestion Levels
CONGESTION_THRESHOLDS: Final[dict] = {
    "low": 15.0,  # > 15 km/h
    "medium": 10.0,  # 10-15 km/h
    "high": 5.0,  # 5-10 km/h
    "critical": 5.0,  # < 5 km/h
}

# =============================================================================
# Monitoring & Alerting
# =============================================================================

# Prometheus Metrics
METRIC_PREFIX: Final[str] = "sptrans_pipeline"
METRIC_API_REQUESTS: Final[str] = f"{METRIC_PREFIX}_api_requests_total"
METRIC_API_ERRORS: Final[str] = f"{METRIC_PREFIX}_api_errors_total"
METRIC_RECORDS_PROCESSED: Final[str] = f"{METRIC_PREFIX}_records_processed_total"
METRIC_PIPELINE_DURATION: Final[str] = f"{METRIC_PREFIX}_duration_seconds"
METRIC_DATA_QUALITY_SCORE: Final[str] = f"{METRIC_PREFIX}_data_quality_score"

# Alert Thresholds
ALERT_API_ERROR_RATE: Final[float] = 0.10  # 10%
ALERT_PIPELINE_DELAY_MINUTES: Final[int] = 15
ALERT_DQ_SCORE_MIN: Final[float] = 0.90  # 90%

# =============================================================================
# File Formats & Compression
# =============================================================================

# Formats
FORMAT_BRONZE: Final[str] = "parquet"
FORMAT_SILVER: Final[str] = "delta"
FORMAT_GOLD: Final[str] = "delta"

# Compression
COMPRESSION_BRONZE: Final[str] = "snappy"
COMPRESSION_SILVER: Final[str] = "snappy"
COMPRESSION_GOLD: Final[str] = "snappy"

# =============================================================================
# Geocoding Configuration
# =============================================================================

# Nominatim (OpenStreetMap)
GEOCODING_BASE_URL: Final[str] = "https://nominatim.openstreetmap.org"
GEOCODING_USER_AGENT: Final[str] = "SPTrans-Pipeline/2.0"
GEOCODING_TIMEOUT: Final[int] = 5
GEOCODING_RATE_LIMIT_DELAY: Final[float] = 1.0  # 1 request per second

# =============================================================================
# Enums
# =============================================================================


class PipelineStage(str, Enum):
    """Estágios do pipeline (Medallion Architecture)."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    SERVING = "serving"


class DataQualityStatus(str, Enum):
    """Status de data quality."""

    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    SKIPPED = "skipped"


class JobStatus(str, Enum):
    """Status de execução de jobs."""

    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    TIMEOUT = "timeout"
    SKIPPED = "skipped"


class VehicleStatus(str, Enum):
    """Status do veículo."""

    OPERATIONAL = "operational"
    STOPPED = "stopped"
    MAINTENANCE = "maintenance"
    UNKNOWN = "unknown"


class ServicePeriod(str, Enum):
    """Período de serviço (para GTFS)."""

    WEEKDAY = "weekday"
    SATURDAY = "saturday"
    SUNDAY = "sunday"
    HOLIDAY = "holiday"


# =============================================================================
# Utility Functions
# =============================================================================


def get_sptrans_endpoint(endpoint_key: str) -> str:
    """
    Retorna a URL completa de um endpoint da API SPTrans.

    Args:
        endpoint_key: Chave do endpoint (ex: 'positions', 'lines')

    Returns:
        URL completa do endpoint

    Raises:
        ValueError: Se a chave do endpoint não existir
    """
    if endpoint_key not in API_ENDPOINTS:
        raise ValueError(f"Endpoint desconhecido: {endpoint_key}")

    return f"{SPTRANS_API_BASE_URL}{API_ENDPOINTS[endpoint_key]}"


def get_s3_path(bucket: str, prefix: str, filename: str = "") -> str:
    """
    Constrói path S3 completo.

    Args:
        bucket: Nome do bucket
        prefix: Prefixo (pasta)
        filename: Nome do arquivo (opcional)

    Returns:
        Path S3 completo (s3a://bucket/prefix/filename)
    """
    path = f"s3a://{bucket}/{prefix}"
    if filename:
        path = f"{path}/{filename}"
    return path


# Exemplo de uso
if __name__ == "__main__":
    print(f"API Base URL: {SPTRANS_API_BASE_URL}")
    print(f"Positions Endpoint: {get_sptrans_endpoint('positions')}")
    print(f"Bronze Bucket: {BUCKET_BRONZE}")
    print(f"S3 Path Example: {get_s3_path(BUCKET_BRONZE, PREFIX_API_POSITIONS, 'test.parquet')}")
    print(f"\nPipeline Stages: {[stage.value for stage in PipelineStage]}")
    print(f"DQ Statuses: {[status.value for status in DataQualityStatus]}")
