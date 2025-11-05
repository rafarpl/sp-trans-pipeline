"""
Configuration management using Pydantic for type safety and validation.
All environment variables are loaded and validated here.
"""

from pydantic import Field
from pydantic_settings import BaseSettings
from functools import lru_cache
from typing import Optional
import os


class PostgresConfig(BaseSettings):
    """PostgreSQL database configuration"""
    
    host: str = Field(default="postgres", env="POSTGRES_HOST")
    port: int = Field(default=5432, env="POSTGRES_PORT")
    user: str = Field(default="sptrans", env="POSTGRES_USER")
    password: str = Field(default="sptrans123", env="POSTGRES_PASSWORD")
    database: str = Field(default="sptrans", env="POSTGRES_DB")
    
    @property
    def connection_string(self) -> str:
        """Generate SQLAlchemy connection string"""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    class Config:
        env_file = ".env"
        case_sensitive = False


class RedisConfig(BaseSettings):
    """Redis cache configuration"""
    
    host: str = Field(default="redis", env="REDIS_HOST")
    port: int = Field(default=6379, env="REDIS_PORT")
    db: int = Field(default=0, env="REDIS_DB")
    password: Optional[str] = Field(default=None, env="REDIS_PASSWORD")
    
    @property
    def connection_url(self) -> str:
        """Generate Redis connection URL"""
        if self.password:
            return f"redis://:{self.password}@{self.host}:{self.port}/{self.db}"
        return f"redis://{self.host}:{self.port}/{self.db}"
    
    class Config:
        env_file = ".env"
        case_sensitive = False


class MinioConfig(BaseSettings):
    """MinIO (S3-compatible) storage configuration"""
    
    endpoint: str = Field(default="minio:9000", env="MINIO_ENDPOINT")
    access_key: str = Field(default="minioadmin", env="MINIO_ROOT_USER")
    secret_key: str = Field(default="minioadmin123", env="MINIO_ROOT_PASSWORD")
    bucket: str = Field(default="sptrans-datalake", env="MINIO_BUCKET")
    use_ssl: bool = Field(default=False, env="MINIO_USE_SSL")
    region: str = Field(default="us-east-1", env="MINIO_REGION")
    
    @property
    def s3_endpoint(self) -> str:
        """Generate S3 endpoint URL"""
        protocol = "https" if self.use_ssl else "http"
        return f"{protocol}://{self.endpoint}"
    
    class Config:
        env_file = ".env"
        case_sensitive = False


class SparkConfig(BaseSettings):
    """Apache Spark configuration"""
    
    master: str = Field(default="spark://spark-master:7077", env="SPARK_MASTER")
    app_name: str = Field(default="SPTrans-Pipeline", env="SPARK_APP_NAME")
    executor_memory: str = Field(default="2g", env="SPARK_EXECUTOR_MEMORY")
    driver_memory: str = Field(default="2g", env="SPARK_DRIVER_MEMORY")
    
    class Config:
        env_file = ".env"
        case_sensitive = False


class SPTransAPIConfig(BaseSettings):
    """SPTrans API configuration"""
    
    token: str = Field(..., env="SPTRANS_API_TOKEN")  # Required!
    base_url: str = Field(default="http://api.olhovivo.sptrans.com.br/v2.1", env="SPTRANS_API_BASE_URL")
    timeout: int = Field(default=30, env="SPTRANS_API_TIMEOUT")
    max_retries: int = Field(default=3, env="SPTRANS_API_MAX_RETRIES")
    retry_delay: int = Field(default=5, env="SPTRANS_API_RETRY_DELAY")
    
    class Config:
        env_file = ".env"
        case_sensitive = False


class DataIngestionConfig(BaseSettings):
    """Data ingestion configuration"""
    
    api_polling_interval: int = Field(default=180, env="API_POLLING_INTERVAL_SECONDS")  # 3 minutes
    gtfs_update_schedule: str = Field(default="0 3 * * *", env="GTFS_UPDATE_SCHEDULE")  # Daily at 3 AM
    data_retention_days: int = Field(default=30, env="DATA_RETENTION_DAYS")
    batch_size: int = Field(default=1000, env="BATCH_SIZE")
    
    class Config:
        env_file = ".env"
        case_sensitive = False


class DataQualityConfig(BaseSettings):
    """Data quality configuration"""
    
    check_interval: int = Field(default=60, env="DQ_CHECK_INTERVAL_MINUTES")
    alert_threshold: float = Field(default=0.95, env="DQ_ALERT_THRESHOLD")
    enable_checks: bool = Field(default=True, env="ENABLE_DATA_QUALITY_CHECKS")
    null_threshold: float = Field(default=0.05, env="DQ_NULL_THRESHOLD")
    duplicate_threshold: float = Field(default=0.01, env="DQ_DUPLICATE_THRESHOLD")
    
    class Config:
        env_file = ".env"
        case_sensitive = False


class MonitoringConfig(BaseSettings):
    """Monitoring and observability configuration"""
    
    enable_prometheus: bool = Field(default=True, env="ENABLE_PROMETHEUS")
    enable_grafana: bool = Field(default=True, env="ENABLE_GRAFANA")
    metrics_port: int = Field(default=9091, env="METRICS_PORT")
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    
    class Config:
        env_file = ".env"
        case_sensitive = False


class GeocodingConfig(BaseSettings):
    """Geocoding configuration (Nominatim)"""
    
    api_url: str = Field(default="https://nominatim.openstreetmap.org", env="GEOCODING_API_URL")
    rate_limit: float = Field(default=1.0, env="GEOCODING_RATE_LIMIT")  # requests per second
    user_agent: str = Field(default="SPTrans-Pipeline/1.0", env="GEOCODING_USER_AGENT")
    timeout: int = Field(default=10, env="GEOCODING_TIMEOUT")
    
    class Config:
        env_file = ".env"
        case_sensitive = False


class AppConfig(BaseSettings):
    """Main application configuration"""
    
    env: str = Field(default="development", env="APP_ENV")
    debug: bool = Field(default=False, env="APP_DEBUG")
    timezone: str = Field(default="America/Sao_Paulo", env="TZ")
    project_name: str = Field(default="SPTrans Real-Time Pipeline", env="PROJECT_NAME")
    version: str = Field(default="1.0.0", env="APP_VERSION")
    
    # Sub-configurations
    postgres: PostgresConfig = PostgresConfig()
    redis: RedisConfig = RedisConfig()
    minio: MinioConfig = MinioConfig()
    spark: SparkConfig = SparkConfig()
    sptrans_api: SPTransAPIConfig = SPTransAPIConfig()
    data_ingestion: DataIngestionConfig = DataIngestionConfig()
    data_quality: DataQualityConfig = DataQualityConfig()
    monitoring: MonitoringConfig = MonitoringConfig()
    geocoding: GeocodingConfig = GeocodingConfig()
    
    @property
    def is_production(self) -> bool:
        """Check if running in production"""
        return self.env.lower() == "production"
    
    @property
    def is_development(self) -> bool:
        """Check if running in development"""
        return self.env.lower() == "development"
    
    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_config() -> AppConfig:
    """
    Get cached application configuration.
    
    Using lru_cache ensures we only load config once per process.
    
    Returns:
        AppConfig: Application configuration object
    """
    return AppConfig()


# Export convenience instances
config = get_config()
