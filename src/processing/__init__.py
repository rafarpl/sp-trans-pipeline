# =============================================================================
# SPTRANS PIPELINE - PROCESSING MODULE
# =============================================================================
# Módulo de processamento de dados com Apache Spark
# =============================================================================

"""
Processing Module

Responsável pelo processamento distribuído de dados através das camadas:
    - Bronze → Silver: Limpeza, validação e enriquecimento
    - Silver → Gold: Agregações e cálculo de KPIs
    - Gold → Serving: Load para PostgreSQL

Componentes:
    - jobs: Spark jobs para processamento de cada camada
    - transformations: Transformações reutilizáveis (DQ, dedup, enrichment)
    - streaming: Processamento em streaming (opcional)
"""

__version__ = "1.0.0"

# =============================================================================
# IMPORTS
# =============================================================================

from typing import Optional
import logging

# Jobs
try:
    from .jobs.bronze_to_silver import BronzeToSilverJob
except ImportError:
    BronzeToSilverJob = None

try:
    from .jobs.silver_to_gold import SilverToGoldJob
except ImportError:
    SilverToGoldJob = None

try:
    from .jobs.gold_to_postgres import GoldToPostgresJob
except ImportError:
    GoldToPostgresJob = None

try:
    from .jobs.ingest_api_to_bronze import IngestAPIToBronzeJob
except ImportError:
    IngestAPIToBronzeJob = None

try:
    from .jobs.ingest_gtfs_to_bronze import IngestGTFSToBronzeJob
except ImportError:
    IngestGTFSToBronzeJob = None

# Transformations
try:
    from .transformations.data_quality import DataQualityChecker
except ImportError:
    DataQualityChecker = None

try:
    from .transformations.deduplication import dedup_positions
except ImportError:
    dedup_positions = None

try:
    from .transformations.enrichment import enrich_with_gtfs
except ImportError:
    enrich_with_gtfs = None

try:
    from .transformations.geocoding import reverse_geocode
except ImportError:
    reverse_geocode = None

try:
    from .transformations.aggregations import (
        calculate_hourly_kpis,
        calculate_route_metrics,
        calculate_headway,
    )
except ImportError:
    calculate_hourly_kpis = None
    calculate_route_metrics = None
    calculate_headway = None

# =============================================================================
# EXPORTS
# =============================================================================

__all__ = [
    # Jobs
    "BronzeToSilverJob",
    "SilverToGoldJob",
    "GoldToPostgresJob",
    "IngestAPIToBronzeJob",
    "IngestGTFSToBronzeJob",
    
    # Transformations
    "DataQualityChecker",
    "dedup_positions",
    "enrich_with_gtfs",
    "reverse_geocode",
    "calculate_hourly_kpis",
    "calculate_route_metrics",
    "calculate_headway",
    
    # Factory functions
    "create_spark_session",
    "run_bronze_to_silver",
    "run_silver_to_gold",
    "run_gold_to_postgres",
]

# =============================================================================
# SPARK SESSION FACTORY
# =============================================================================

def create_spark_session(
    app_name: str = "SPTrans-Pipeline",
    master: Optional[str] = None,
    config: Optional[dict] = None
):
    """
    Cria uma SparkSession configurada para o pipeline.
    
    Args:
        app_name: Nome da aplicação Spark
        master: URL do Spark master (default: spark://spark-master:7077)
        config: Configurações adicionais do Spark
    
    Returns:
        SparkSession configurada
    
    Example:
        >>> spark = create_spark_session()
        >>> df = spark.read.parquet("s3a://bucket/path")
    """
    from pyspark.sql import SparkSession
    import os
    
    master = master or os.getenv("SPARK_MASTER", "spark://spark-master:7077")
    
    builder = SparkSession.builder \
        .appName(app_name) \
        .master(master)
    
    # Configurações padrão
    default_config = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.shuffle.partitions": "200",
        
        # S3/MinIO
        "spark.hadoop.fs.s3a.endpoint": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        "spark.hadoop.fs.s3a.access.key": os.getenv("MINIO_ROOT_USER", "admin"),
        "spark.hadoop.fs.s3a.secret.key": os.getenv("MINIO_ROOT_PASSWORD", "miniopassword123"),
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        
        # Performance
        "spark.sql.broadcastTimeout": "600",
        "spark.network.timeout": "600s",
        "spark.executor.heartbeatInterval": "60s",
    }
    
    # Merge com configurações customizadas
    if config:
        default_config.update(config)
    
    # Aplicar configurações
    for key, value in default_config.items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    
    # Definir log level
    spark.sparkContext.setLogLevel("WARN")
    
    return spark

# =============================================================================
# JOB RUNNER FUNCTIONS
# =============================================================================

def run_bronze_to_silver(
    input_path: str,
    output_path: str,
    date: str,
    spark: Optional[object] = None
) -> dict:
    """
    Executa job Bronze → Silver.
    
    Args:
        input_path: Caminho dos dados Bronze
        output_path: Caminho de saída Silver
        date: Data de processamento (YYYY-MM-DD)
        spark: SparkSession (cria nova se None)
    
    Returns:
        Dicionário com estatísticas da execução
    """
    if BronzeToSilverJob is None:
        raise ImportError("BronzeToSilverJob não disponível")
    
    if spark is None:
        spark = create_spark_session("Bronze-to-Silver")
    
    job = BronzeToSilverJob(spark)
    return job.run(input_path, output_path, date)


def run_silver_to_gold(
    input_path: str,
    output_path: str,
    date: str,
    spark: Optional[object] = None
) -> dict:
    """
    Executa job Silver → Gold.
    
    Args:
        input_path: Caminho dos dados Silver
        output_path: Caminho de saída Gold
        date: Data de processamento (YYYY-MM-DD)
        spark: SparkSession (cria nova se None)
    
    Returns:
        Dicionário com estatísticas da execução
    """
    if SilverToGoldJob is None:
        raise ImportError("SilverToGoldJob não disponível")
    
    if spark is None:
        spark = create_spark_session("Silver-to-Gold")
    
    job = SilverToGoldJob(spark)
    return job.run(input_path, output_path, date)


def run_gold_to_postgres(
    input_path: str,
    date: str,
    spark: Optional[object] = None
) -> dict:
    """
    Executa job Gold → PostgreSQL.
    
    Args:
        input_path: Caminho dos dados Gold
        date: Data de processamento (YYYY-MM-DD)
        spark: SparkSession (cria nova se None)
    
    Returns:
        Dicionário com estatísticas da execução
    """
    if GoldToPostgresJob is None:
        raise ImportError("GoldToPostgresJob não disponível")
    
    if spark is None:
        spark = create_spark_session("Gold-to-Postgres")
    
    job = GoldToPostgresJob(spark)
    return job.run(input_path, date)

# =============================================================================
# VALIDATION
# =============================================================================

def validate_processing_components() -> dict:
    """
    Valida disponibilidade de componentes do módulo.
    
    Returns:
        Dicionário com status de cada componente
    """
    components = {
        "bronze_to_silver": BronzeToSilverJob is not None,
        "silver_to_gold": SilverToGoldJob is not None,
        "gold_to_postgres": GoldToPostgresJob is not None,
        "ingest_api": IngestAPIToBronzeJob is not None,
        "ingest_gtfs": IngestGTFSToBronzeJob is not None,
        "data_quality": DataQualityChecker is not None,
        "deduplication": dedup_positions is not None,
        "enrichment": enrich_with_gtfs is not None,
        "geocoding": reverse_geocode is not None,
        "aggregations": calculate_hourly_kpis is not None,
    }
    
    components["all_available"] = all(components.values())
    
    return components

# =============================================================================
# MODULE INFO
# =============================================================================

def get_processing_info() -> dict:
    """
    Retorna informações sobre o módulo de processamento.
    
    Returns:
        Dicionário com metadados
    """
    return {
        "module": "processing",
        "version": __version__,
        "components": validate_processing_components(),
        "spark_available": check_spark_availability(),
    }


def check_spark_availability() -> bool:
    """Verifica se PySpark está disponível"""
    try:
        import pyspark
        return True
    except ImportError:
        return False

# =============================================================================
# LOGGING
# =============================================================================

logger = logging.getLogger(__name__)
logger.debug(f"Processing module v{__version__} loaded")

# Validar componentes
components = validate_processing_components()
if not components["all_available"]:
    missing = [k for k, v in components.items() if not v and k != "all_available"]
    logger.warning(f"Componentes faltando: {', '.join(missing)}")

# Verificar Spark
if not check_spark_availability():
    logger.warning("PySpark não disponível. Instale com: pip install pyspark")

# =============================================================================
# END
# =============================================================================
