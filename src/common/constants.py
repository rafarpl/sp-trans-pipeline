# =============================================================================
# SPTRANS PIPELINE - CONSTANTS
# =============================================================================
# Constantes do sistema, enumerações e valores padrão
# =============================================================================

from enum import Enum
from typing import Final

# =============================================================================
# PROJECT INFO
# =============================================================================

PROJECT_NAME: Final[str] = "sptrans-pipeline"
PROJECT_VERSION: Final[str] = "1.0.0"

# =============================================================================
# DATA LAKE LAYERS
# =============================================================================

class DataLakeLayer(str, Enum):
    """Camadas do Data Lake"""
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    SERVING = "serving"

# Paths das camadas
BRONZE_LAYER_PATH: Final[str] = "s3a://sptrans-datalake/bronze"
SILVER_LAYER_PATH: Final[str] = "s3a://sptrans-datalake/silver"
GOLD_LAYER_PATH: Final[str] = "s3a://sptrans-datalake/gold"

# =============================================================================
# API SPTRANS
# =============================================================================

# Endpoints da API Olho Vivo
SPTRANS_API_BASE_URL: Final[str] = "http://api.olhovivo.sptrans.com.br/v2.1"

class SPTransEndpoint(str, Enum):
    """Endpoints da API SPTrans"""
    LOGIN = "/Login/Autenticar"
    POSITIONS = "/Posicao"
    POSITIONS_BY_LINE = "/Posicao/Linha"
    LINES = "/Linha/Buscar"
    LINE_DETAILS = "/Linha/BuscarLinhaTermo"
    STOPS = "/Parada/Buscar"
    STOP_BY_LINE = "/Parada/BuscarParadasPorLinha"
    CORRIDORS = "/Corredor"
    FORECAST = "/Previsao"
    FORECAST_BY_LINE = "/Previsao/Linha"
    FORECAST_BY_STOP = "/Previsao/Parada"

# Timeouts e retries
API_TIMEOUT: Final[int] = 30  # segundos
API_MAX_RETRIES: Final[int] = 3
API_RETRY_DELAY: Final[int] = 5  # segundos
API_BACKOFF_FACTOR: Final[float] = 2.0

# Rate limiting
API_MAX_REQUESTS_PER_MINUTE: Final[int] = 60
API_REQUEST_INTERVAL: Final[float] = 1.0  # segundos entre requests

# =============================================================================
# GTFS
# =============================================================================

# URL de download do GTFS
GTFS_DOWNLOAD_URL: Final[str] = "https://www.sptrans.com.br/umbraco/surface/PerfilDesenvolvedor/BaixarGTFS"

# Arquivos GTFS esperados
class GTFSFile(str, Enum):
    """Arquivos do feed GTFS"""
    AGENCY = "agency.txt"
    STOPS = "stops.txt"
    ROUTES = "routes.txt"
    TRIPS = "trips.txt"
    STOP_TIMES = "stop_times.txt"
    CALENDAR = "calendar.txt"
    CALENDAR_DATES = "calendar_dates.txt"
    SHAPES = "shapes.txt"
    FARE_ATTRIBUTES = "fare_attributes.txt"
    FARE_RULES = "fare_rules.txt"
    FREQUENCIES = "frequencies.txt"

GTFS_REQUIRED_FILES: Final[list] = [
    GTFSFile.STOPS.value,
    GTFSFile.ROUTES.value,
    GTFSFile.TRIPS.value,
    GTFSFile.STOP_TIMES.value,
]

# =============================================================================
# COORDENADAS SÃO PAULO
# =============================================================================

# Limites geográficos da cidade de São Paulo
SP_COORDINATES: Final[dict] = {
    "lat_min": -24.0,
    "lat_max": -23.3,
    "lon_min": -46.9,
    "lon_max": -46.3,
}

# Centro de São Paulo (Praça da Sé)
SP_CENTER: Final[dict] = {
    "latitude": -23.5505,
    "longitude": -46.6333,
}

# =============================================================================
# DATA QUALITY
# =============================================================================

class DataQualitySeverity(str, Enum):
    """Severidade de problemas de qualidade"""
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"

class DataQualityMetric(str, Enum):
    """Métricas de qualidade de dados"""
    COMPLETENESS = "completeness"
    VALIDITY = "validity"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    TIMELINESS = "timeliness"
    UNIQUENESS = "uniqueness"

# Thresholds de qualidade (percentuais)
DQ_THRESHOLDS: Final[dict] = {
    "completeness_min": 80.0,
    "validity_min": 90.0,
    "accuracy_min": 85.0,
    "overall_quality_min": 80.0,
}

# Campos obrigatórios
REQUIRED_FIELDS_POSITIONS: Final[list] = [
    "vehicle_id",
    "timestamp",
    "latitude",
    "longitude",
]

# =============================================================================
# SPARK
# =============================================================================

# Configurações padrão Spark
SPARK_APP_NAME: Final[str] = "SPTrans-Pipeline"
SPARK_MASTER: Final[str] = "spark://spark-master:7077"

# Particionamento
SPARK_SHUFFLE_PARTITIONS: Final[int] = 200
SPARK_DEFAULT_PARALLELISM: Final[int] = 100

# Batch sizes
SPARK_BATCH_SIZE: Final[int] = 10000
SPARK_WRITE_BATCH_SIZE: Final[int] = 5000

# =============================================================================
# POSTGRESQL
# =============================================================================

# Schemas
POSTGRES_SCHEMA_SERVING: Final[str] = "serving"

# Tabelas
class ServingTable(str, Enum):
    """Tabelas do serving layer"""
    KPIS_HOURLY = "kpis_hourly"
    ROUTE_METRICS = "route_metrics"
    HEADWAY_ANALYSIS = "headway_analysis"
    SYSTEM_SUMMARY = "system_summary"
    ALERTS_LOG = "alerts_log"
    DATA_QUALITY_METRICS = "data_quality_metrics"
    ROUTE_CATALOG = "route_catalog"

# Connection pool
POSTGRES_POOL_SIZE: Final[int] = 10
POSTGRES_MAX_OVERFLOW: Final[int] = 20
POSTGRES_POOL_TIMEOUT: Final[int] = 30

# Batch loading
POSTGRES_BATCH_SIZE: Final[int] = 1000

# =============================================================================
# REDIS
# =============================================================================

# Cache TTL (segundos)
REDIS_CACHE_TTL: Final[int] = 300  # 5 minutos

# Prefixos de cache keys
class RedisCacheKey(str, Enum):
    """Prefixos para cache keys"""
    VEHICLE_POSITION = "vehicle:position:"
    ROUTE_INFO = "route:info:"
    STOP_INFO = "stop:info:"
    API_RESPONSE = "api:response:"
    METRICS = "metrics:"

# =============================================================================
# SCHEDULING
# =============================================================================

# Schedules dos DAGs (cron format)
DAG_SCHEDULES: Final[dict] = {
    "gtfs_ingestion": "0 2 * * *",          # Daily at 2 AM
    "api_ingestion": "*/2 * * * *",         # Every 2 minutes
    "bronze_to_silver": "*/30 * * * *",     # Every 30 minutes
    "silver_to_gold": "0 * * * *",          # Every hour
    "gold_to_serving": "15 * * * *",        # Every hour at :15
    "data_quality": "30 * * * *",           # Every hour at :30
    "maintenance": "0 3 * * 0",             # Weekly on Sunday at 3 AM
}

# =============================================================================
# RETENTION PERIODS
# =============================================================================

# Períodos de retenção de dados (dias)
RETENTION_PERIODS: Final[dict] = {
    "bronze_api": 7,        # 7 dias
    "bronze_gtfs": 90,      # 90 dias
    "silver": 30,           # 30 dias
    "gold": 365,            # 1 ano
    "serving": 90,          # 90 dias
    "logs": 30,             # 30 dias
    "backups": 7,           # 7 dias
}

# =============================================================================
# FILE FORMATS
# =============================================================================

class FileFormat(str, Enum):
    """Formatos de arquivo suportados"""
    PARQUET = "parquet"
    CSV = "csv"
    JSON = "json"
    AVRO = "avro"
    ORC = "orc"

# Formato padrão por camada
LAYER_FILE_FORMAT: Final[dict] = {
    DataLakeLayer.BRONZE: FileFormat.JSON,
    DataLakeLayer.SILVER: FileFormat.PARQUET,
    DataLakeLayer.GOLD: FileFormat.PARQUET,
}

# =============================================================================
# PARTITIONING
# =============================================================================

# Colunas de particionamento
PARTITION_COLUMNS: Final[dict] = {
    "api_positions": ["date", "hour"],
    "gtfs_routes": ["date"],
    "silver_positions": ["date", "hour"],
    "gold_kpis": ["date"],
}

# =============================================================================
# MONITORING
# =============================================================================

class MetricType(str, Enum):
    """Tipos de métricas"""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"

# Portas de monitoramento
PROMETHEUS_PORT: Final[int] = 9090
GRAFANA_PORT: Final[int] = 3000
AIRFLOW_PORT: Final[int] = 8080
SPARK_UI_PORT: Final[int] = 8081

# =============================================================================
# ALERTS
# =============================================================================

class AlertType(str, Enum):
    """Tipos de alertas"""
    DATA_QUALITY = "data_quality"
    PIPELINE_FAILURE = "pipeline_failure"
    PERFORMANCE = "performance"
    RESOURCE = "resource"
    SECURITY = "security"

class AlertSeverity(str, Enum):
    """Severidade de alertas"""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"

# =============================================================================
# STATUS
# =============================================================================

class JobStatus(str, Enum):
    """Status de jobs"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"

class ServiceQuality(str, Enum):
    """Qualidade do serviço"""
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"

# =============================================================================
# VEHICLE TYPES
# =============================================================================

class VehicleType(str, Enum):
    """Tipos de veículos"""
    BUS = "bus"
    METRO = "metro"
    TRAIN = "train"

# =============================================================================
# TIMEZONE
# =============================================================================

TIMEZONE: Final[str] = "America/Sao_Paulo"

# =============================================================================
# DATE FORMATS
# =============================================================================

DATE_FORMAT: Final[str] = "%Y-%m-%d"
DATETIME_FORMAT: Final[str] = "%Y-%m-%d %H:%M:%S"
TIMESTAMP_FORMAT: Final[str] = "%Y-%m-%dT%H:%M:%S"

# =============================================================================
# LOGGING
# =============================================================================

LOG_LEVEL_DEFAULT: Final[str] = "INFO"
LOG_FORMAT: Final[str] = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# =============================================================================
# TIMEOUTS
# =============================================================================

DEFAULT_TIMEOUT: Final[int] = 300  # 5 minutos
LONG_TIMEOUT: Final[int] = 3600    # 1 hora
SHORT_TIMEOUT: Final[int] = 60     # 1 minuto

# =============================================================================
# REGEX PATTERNS
# =============================================================================

# Padrões de validação
REGEX_PATTERNS: Final[dict] = {
    "vehicle_id": r"^\d{4,6}$",
    "route_code": r"^[A-Z0-9\-]+$",
    "timestamp": r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}",
}

# =============================================================================
# FEATURE FLAGS
# =============================================================================

# Flags para habilitar/desabilitar features
FEATURE_FLAGS: Final[dict] = {
    "enable_real_time_ingestion": True,
    "enable_gtfs_ingestion": True,
    "enable_geocoding": False,
    "enable_ml_predictions": False,
    "enable_streaming": False,
    "enable_data_quality_checks": True,
}

# =============================================================================
# ERROR CODES
# =============================================================================

class ErrorCode(str, Enum):
    """Códigos de erro"""
    API_ERROR = "API_ERROR"
    DATABASE_ERROR = "DATABASE_ERROR"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    PROCESSING_ERROR = "PROCESSING_ERROR"
    CONFIGURATION_ERROR = "CONFIGURATION_ERROR"

# =============================================================================
# DEFAULTS
# =============================================================================

# Valores padrão
DEFAULTS: Final[dict] = {
    "batch_size": 1000,
    "max_retries": 3,
    "timeout": 30,
    "parallelism": 4,
}

# =============================================================================
# END
# =============================================================================
