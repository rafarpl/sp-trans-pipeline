"""
SPTrans Pipeline - Ingestion Module

Módulo responsável pela ingestão de dados da API SPTrans e GTFS.
Inclui cliente API robusto, downloader GTFS e definições de schemas.
"""

from .sptrans_api_client import SPTransAPIClient
from .gtfs_downloader import GTFSDownloader
from .schema_definitions import (
    get_bronze_schema,
    get_silver_schema,
    get_gtfs_schema,
    validate_schema,
)

__all__ = [
    "SPTransAPIClient",
    "GTFSDownloader",
    "get_bronze_schema",
    "get_silver_schema",
    "get_gtfs_schema",
    "validate_schema",
]
