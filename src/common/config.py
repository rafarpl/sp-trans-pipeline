"""
Configurações centralizadas do projeto.
Carrega variáveis de ambiente e fornece acesso tipado.
"""
import os
from typing import Optional
from dotenv import load_dotenv
from pydantic import BaseSettings, Field, validator


# Carregar variáveis de ambiente
load_dotenv()


class SPTransAPIConfig(BaseSettings):
    """Configurações da API SPTrans."""
    
    token: str = Field(..., env='SPTRANS_API_TOKEN')
    base_url: str = Field(
        default='https://api.olhovivo.sptrans.com.br/v2.1',
        env='SPTRANS_API_BASE_URL'
    )
    timeout: int = Field(default=30, env='SPTRANS_API_TIMEOUT')
    max_retries: int = Field(default=3, env='SPTRANS_API_MAX_RETRIES')
    retry_delay: int = Field(default=5, env='SPTRANS_API_RETRY_DELAY')
    
    @validator('token')
    def validate_token(cls, v):
        if not v or v == 'seu_token_aqui':
            raise ValueError(
                'SPTRANS_API_TOKEN não configurado. '
                'Obtenha um token em https://www.sptrans.com.br/desenvolvedores/'
            )
        return v
    
    class Config:
        env_file = '.env'
        case_sensitive = False


class MinIOConfig(BaseSettings):
    """Configurações do MinIO (Data Lake)."""
    
    endpoint: str = Field(default='minio:9000', env='MINIO_ENDPOINT')
    access_key: str = Field(default='minioadmin', env='MINIO_ROOT_USER')
    secret_key: str = Field(default='minioadmin123', env='MINIO_ROOT_PASSWORD')
    use_ssl: bool = Field(default=False, env='MINIO_USE_SSL')
    
    # Buckets
    bucket_bronze: str = Field(default='sptrans-bronze', env='MINIO_BUCKET_BRONZE')
    bucket_silver: str = Field(default='sptrans-silver', env='MINIO_BUCKET_SILVER')
    bucket_gold: str = Field(default='sptrans-gold', env='MINIO_BUCKET_GOLD')
    
    @property
    def s3a_endpoint(self) -> str:
        """Endpoint no formato s3a://"""
        protocol = 'https' if self.use_ssl else 'http'
        return f"{protocol}://{self.endpoint}"
    
    class Config:
        env_file = '.env'


class PostgreSQLConfig(BaseSettings):
    """Configurações do PostgreSQL."""
    
    host: str = Field(default='postgres', env='POSTGRES_HOST')
    port: int = Field(default=5432, env='POSTGRES_PORT')
    database: str = Field(default='sptrans', env='POSTGRES_DB_SPTRANS')
    user: str = Field(default='airflow', env='POSTGRES_USER')
    password: str = Field(default='airflow123', env='POSTGRES_PASSWORD')
    
    # Schemas
    schema_serving: str = Field(default='serving', env='POSTGRES_SCHEMA_SERVING')
    schema_metadata: str = Field(default='metadata', env='POSTGRES_SCHEMA_METADATA')
    
    @property
    def connection_string(self) -> str:
        """SQLAlchemy connection string."""
        return (
            f"postgresql://{self.user}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}"
        )
    
    @property
    def jdbc_url(self) -> str:
        """JDBC URL para Spark."""
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
    
    class Config:
        env_file = '.env'


class RedisConfig(BaseSettings):
    """Configurações do Redis."""
    
    host: str = Field(default='redis', env='REDIS_HOST')
    port: int = Field(default=6379, env='REDIS_PORT')
    password: str = Field(default='redis123', env='REDIS_PASSWORD')
    db: int = Field(default=0, env='REDIS_DB')
    
    @property
    def connection_string(self) -> str:
        """Redis connection string."""
        return f"redis://:{self.password}@{self.host}:{self.port}/{self.db}"
    
    class Config:
        env_file = '.env'


class SparkConfig(BaseSettings):
    """Configurações do Spark."""
    
    master: str = Field(default='spark://spark-master:7077', env='SPARK_MASTER_HOST')
    app_name: str = Field(default='SPTransPipeline')
    
    # Driver
    driver_memory: str = Field(default='2g', env='SPARK_DRIVER_MEMORY')
    
    # Executor
    executor_memory: str = Field(default='4g', env='SPARK_EXECUTOR_MEMORY')
    executor_cores: int = Field(default=2, env='SPARK_WORKER_CORES')
    
    # Parallelism
    default_parallelism: int = Field(default=12)
    sql_shuffle_partitions: int = Field(default=8)
    
    class Config:
        env_file = '.env'


class PipelineConfig(BaseSettings):
    """Configurações do pipeline de dados."""
    
    # Frequências de ingestão (em minutos)
    api_ingestion_interval: int = Field(
        default=2, 
        env='API_INGESTION_INTERVAL_MINUTES'
    )
    bronze_to_silver_interval: int = Field(
        default=10, 
        env='BRONZE_TO_SILVER_INTERVAL_MINUTES'
    )
    silver_to_gold_interval: int = Field(
        default=15, 
        env='SILVER_TO_GOLD_INTERVAL_MINUTES'
    )
    gold_to_serving_interval: int = Field(
        default=15, 
        env='GOLD_TO_SERVING_INTERVAL_MINUTES'
    )
    
    # Data Quality
    dq_score_threshold: float = Field(default=95.0, env='DQ_SCORE_THRESHOLD')
    dq_critical_threshold: float = Field(default=85.0, env='DQ_CRITICAL_THRESHOLD')
    
    # Retenção de dados (em dias)
    retention_bronze: int = Field(default=90, env='DATA_RETENTION_BRONZE_DAYS')
    retention_silver: int = Field(default=90, env='DATA_RETENTION_SILVER_DAYS')
    retention_gold: int = Field(default=365, env='DATA_RETENTION_GOLD_DAYS')
    retention_serving: int = Field(default=7, env='DATA_RETENTION_SERVING_DAYS')
    
    # Batch sizes
    batch_size_api: int = Field(default=15000, env='BATCH_SIZE_API_INGESTION')
    batch_size_bronze_silver: int = Field(default=100000, env='BATCH_SIZE_BRONZE_TO_SILVER')
    batch_size_silver_gold: int = Field(default=50000, env='BATCH_SIZE_SILVER_TO_GOLD')
    
    class Config:
        env_file = '.env'


class AppConfig(BaseSettings):
    """Configurações gerais da aplicação."""
    
    environment: str = Field(default='development', env='ENVIRONMENT')
    log_level: str = Field(default='INFO', env='LOG_LEVEL')
    timezone: str = Field(default='America/Sao_Paulo', env='TZ')
    
    project_name: str = Field(default='sptrans-realtime-pipeline', env='PROJECT_NAME')
    project_version: str = Field(default='1.0.0', env='PROJECT_VERSION')
    
    # Feature Flags
    enable_metrics: bool = Field(default=True, env='ENABLE_METRICS')
    enable_tracing: bool = Field(default=False, env='ENABLE_TRACING')
    
    # Geocoding
    geocoding_enabled: bool = Field(default=True, env='GEOCODING_ENABLED')
    geocoding_provider: str = Field(default='nominatim', env='GEOCODING_PROVIDER')
    
    @validator('environment')
    def validate_environment(cls, v):
        allowed = ['development', 'staging', 'production']
        if v not in allowed:
            raise ValueError(f'Environment deve ser um de: {allowed}')
        return v
    
    @validator('log_level')
    def validate_log_level(cls, v):
        allowed = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if v.upper() not in allowed:
            raise ValueError(f'Log level deve ser um de: {allowed}')
        return v.upper()
    
    @property
    def is_development(self) -> bool:
        return self.environment == 'development'
    
    @property
    def is_production(self) -> bool:
        return self.environment == 'production'
    
    class Config:
        env_file = '.env'


class Config:
    """
    Classe principal de configuração.
    Agrega todas as configurações do projeto.
    """
    
    def __init__(self):
        self.app = AppConfig()
        self.sptrans_api = SPTransAPIConfig()
        self.minio = MinIOConfig()
        self.postgres = PostgreSQLConfig()
        self.redis = RedisConfig()
        self.spark = SparkConfig()
        self.pipeline = PipelineConfig()
    
    def __repr__(self) -> str:
        return (
            f"Config("
            f"environment={self.app.environment}, "
            f"project={self.app.project_name})"
        )
    
    def validate(self) -> bool:
        """Valida todas as configurações."""
        try:
            # Tenta instanciar todas as configs
            _ = self.app
            _ = self.sptrans_api
            _ = self.minio
            _ = self.postgres
            _ = self.redis
            _ = self.spark
            _ = self.pipeline
            return True
        except Exception as e:
            print(f"Erro na validação de configurações: {e}")
            return False
    
    def get_spark_config(self) -> dict:
        """
        Retorna configurações do Spark no formato para SparkSession.
        """
        return {
            # Master
            'spark.master': self.spark.master,
            'spark.app.name': self.spark.app_name,
            
            # Driver
            'spark.driver.memory': self.spark.driver_memory,
            
            # Executor
            'spark.executor.memory': self.spark.executor_memory,
            'spark.executor.cores': str(self.spark.executor_cores),
            
            # Parallelism
            'spark.default.parallelism': str(self.spark.default_parallelism),
            'spark.sql.shuffle.partitions': str(self.spark.sql_shuffle_partitions),
            
            # Delta Lake
            'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
            'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog',
            
            # S3/MinIO
            'spark.hadoop.fs.s3a.endpoint': self.minio.s3a_endpoint,
            'spark.hadoop.fs.s3a.access.key': self.minio.access_key,
            'spark.hadoop.fs.s3a.secret.key': self.minio.secret_key,
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            
            # AQE
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
        }


# Instância global de configuração
config = Config()


# Funções auxiliares para acesso rápido
def get_config() -> Config:
    """Retorna instância global de configuração."""
    return config


def get_sptrans_api_config() -> SPTransAPIConfig:
    """Retorna configurações da API SPTrans."""
    return config.sptrans_api


def get_minio_config() -> MinIOConfig:
    """Retorna configurações do MinIO."""
    return config.minio


def get_postgres_config() -> PostgreSQLConfig:
    """Retorna configurações do PostgreSQL."""
    return config.postgres


def get_spark_config() -> dict:
    """Retorna configurações do Spark."""
    return config.get_spark_config()


if __name__ == '__main__':
    # Teste de configuração
    cfg = get_config()
    print(f"✓ Configuração carregada: {cfg}")
    print(f"✓ Environment: {cfg.app.environment}")
    print(f"✓ Log Level: {cfg.app.log_level}")
    print(f"✓ API Token: {cfg.sptrans_api.token[:10]}...")
    print(f"✓ MinIO Endpoint: {cfg.minio.endpoint}")
    print(f"✓ PostgreSQL: {cfg.postgres.connection_string}")
    
    if cfg.validate():
        print("✓ Todas as configurações são válidas!")
    else:
        print("✗ Erro na validação de configurações!")