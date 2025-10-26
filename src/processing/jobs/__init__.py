# =============================================================================
# SPTRANS PIPELINE - PROCESSING JOBS
# =============================================================================
# Spark Jobs para processamento de dados entre camadas
# =============================================================================

"""
Processing Jobs Module

Jobs Spark para transformação de dados:
    - IngestAPIToBronzeJob: API → Bronze
    - IngestGTFSToBronzeJob: GTFS → Bronze
    - BronzeToSilverJob: Bronze → Silver (cleaning, validation)
    - SilverToGoldJob: Silver → Gold (aggregations, KPIs)
    - GoldToPostgresJob: Gold → PostgreSQL (serving layer)
"""

__version__ = "1.0.0"

# =============================================================================
# IMPORTS
# =============================================================================

# Jobs
try:
    from .ingest_api_to_bronze import IngestAPIToBronzeJob
except ImportError:
    IngestAPIToBronzeJob = None

try:
    from .ingest_gtfs_to_bronze import IngestGTFSToBronzeJob
except ImportError:
    IngestGTFSToBronzeJob = None

try:
    from .bronze_to_silver import BronzeToSilverJob
except ImportError:
    BronzeToSilverJob = None

try:
    from .silver_to_gold import SilverToGoldJob
except ImportError:
    SilverToGoldJob = None

try:
    from .gold_to_postgres import GoldToPostgresJob
except ImportError:
    GoldToPostgresJob = None

# =============================================================================
# EXPORTS
# =============================================================================

__all__ = [
    "IngestAPIToBronzeJob",
    "IngestGTFSToBronzeJob",
    "BronzeToSilverJob",
    "SilverToGoldJob",
    "GoldToPostgresJob",
    "get_job_class",
    "list_available_jobs",
]

# =============================================================================
# JOB REGISTRY
# =============================================================================

JOB_REGISTRY = {
    "ingest_api_to_bronze": IngestAPIToBronzeJob,
    "ingest_gtfs_to_bronze": IngestGTFSToBronzeJob,
    "bronze_to_silver": BronzeToSilverJob,
    "silver_to_gold": SilverToGoldJob,
    "gold_to_postgres": GoldToPostgresJob,
}

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def get_job_class(job_name: str):
    """
    Retorna classe do job pelo nome.
    
    Args:
        job_name: Nome do job
    
    Returns:
        Classe do job ou None
    
    Example:
        >>> JobClass = get_job_class("bronze_to_silver")
        >>> job = JobClass(spark)
    """
    return JOB_REGISTRY.get(job_name)


def list_available_jobs() -> list:
    """
    Lista jobs disponíveis.
    
    Returns:
        Lista de nomes de jobs disponíveis
    """
    return [
        name for name, job_class in JOB_REGISTRY.items()
        if job_class is not None
    ]

# =============================================================================
# VALIDATION
# =============================================================================

def validate_jobs() -> dict:
    """Valida disponibilidade de jobs"""
    return {
        name: job_class is not None
        for name, job_class in JOB_REGISTRY.items()
    }

# =============================================================================
# LOGGING
# =============================================================================

import logging

logger = logging.getLogger(__name__)
logger.debug(f"Processing jobs module v{__version__} loaded")

# Validar jobs
jobs_status = validate_jobs()
available = [k for k, v in jobs_status.items() if v]
missing = [k for k, v in jobs_status.items() if not v]

if available:
    logger.info(f"Jobs disponíveis: {', '.join(available)}")
if missing:
    logger.warning(f"Jobs faltando: {', '.join(missing)}")

# =============================================================================
# END
# =============================================================================
