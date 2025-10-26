# =============================================================================
# SPTRANS PIPELINE - INGESTION MODULE
# =============================================================================
# Módulo de ingestão de dados (API SPTrans e GTFS)
# =============================================================================

"""
Ingestion Module

Responsável pela ingestão de dados de múltiplas fontes:
    - API Olho Vivo SPTrans (tempo real)
    - Arquivos GTFS (dados estáticos)

Componentes:
    - SPTransAPIClient: Cliente para API em tempo real
    - GTFSDownloader: Download e parse de arquivos GTFS
    - SchemaDefinitions: Schemas Spark para validação
"""

__version__ = "1.0.0"

# =============================================================================
# IMPORTS
# =============================================================================

from typing import Optional

# Importar componentes principais
try:
    from .sptrans_api_client import SPTransAPIClient
except ImportError:
    SPTransAPIClient = None

try:
    from .gtfs_downloader import GTFSDownloader
except ImportError:
    GTFSDownloader = None

try:
    from .schema_definitions import (
        get_bronze_api_schema,
        get_gtfs_routes_schema,
        get_gtfs_stops_schema,
        get_gtfs_trips_schema,
        get_gtfs_stop_times_schema,
        get_gtfs_shapes_schema,
    )
except ImportError:
    get_bronze_api_schema = None
    get_gtfs_routes_schema = None
    get_gtfs_stops_schema = None
    get_gtfs_trips_schema = None
    get_gtfs_stop_times_schema = None
    get_gtfs_shapes_schema = None

# =============================================================================
# EXPORTS
# =============================================================================

__all__ = [
    # Classes
    "SPTransAPIClient",
    "GTFSDownloader",
    
    # Schema functions
    "get_bronze_api_schema",
    "get_gtfs_routes_schema",
    "get_gtfs_stops_schema",
    "get_gtfs_trips_schema",
    "get_gtfs_stop_times_schema",
    "get_gtfs_shapes_schema",
    
    # Factory functions
    "create_api_client",
    "create_gtfs_downloader",
]

# =============================================================================
# FACTORY FUNCTIONS
# =============================================================================

def create_api_client(
    api_token: Optional[str] = None,
    base_url: Optional[str] = None,
    timeout: int = 30,
    max_retries: int = 3
) -> Optional[SPTransAPIClient]:
    """
    Factory function para criar cliente da API.
    
    Args:
        api_token: Token de autenticação
        base_url: URL base da API
        timeout: Timeout para requests
        max_retries: Número máximo de retries
    
    Returns:
        Instância de SPTransAPIClient ou None se não disponível
    
    Example:
        >>> client = create_api_client(api_token="your-token")
        >>> positions = client.get_all_positions()
    """
    if SPTransAPIClient is None:
        raise ImportError(
            "SPTransAPIClient não disponível. "
            "Verifique se o módulo sptrans_api_client está presente."
        )
    
    return SPTransAPIClient(
        api_token=api_token,
        base_url=base_url,
        timeout=timeout,
        max_retries=max_retries
    )


def create_gtfs_downloader(
    download_url: Optional[str] = None,
    output_dir: str = "./data/gtfs"
) -> Optional[GTFSDownloader]:
    """
    Factory function para criar downloader de GTFS.
    
    Args:
        download_url: URL para download do GTFS
        output_dir: Diretório de saída
    
    Returns:
        Instância de GTFSDownloader ou None se não disponível
    
    Example:
        >>> downloader = create_gtfs_downloader()
        >>> gtfs_files = downloader.download_and_extract()
    """
    if GTFSDownloader is None:
        raise ImportError(
            "GTFSDownloader não disponível. "
            "Verifique se o módulo gtfs_downloader está presente."
        )
    
    return GTFSDownloader(
        download_url=download_url,
        output_dir=output_dir
    )

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def validate_ingestion_config() -> dict:
    """
    Valida configurações de ingestão.
    
    Returns:
        Dicionário com status da validação
    """
    import os
    
    status = {
        "api_token_configured": bool(os.getenv("SPTRANS_API_TOKEN")),
        "api_client_available": SPTransAPIClient is not None,
        "gtfs_downloader_available": GTFSDownloader is not None,
        "schemas_available": get_bronze_api_schema is not None,
    }
    
    status["all_ready"] = all(status.values())
    
    return status


def get_ingestion_info() -> dict:
    """
    Retorna informações sobre o módulo de ingestão.
    
    Returns:
        Dicionário com informações do módulo
    """
    return {
        "module": "ingestion",
        "version": __version__,
        "components": {
            "api_client": SPTransAPIClient is not None,
            "gtfs_downloader": GTFSDownloader is not None,
            "schemas": get_bronze_api_schema is not None,
        },
        "config_status": validate_ingestion_config(),
    }

# =============================================================================
# LOGGING
# =============================================================================

import logging

logger = logging.getLogger(__name__)
logger.debug(f"Ingestion module v{__version__} loaded")

# Verificar componentes disponíveis
config_status = validate_ingestion_config()
if not config_status["all_ready"]:
    missing = [k for k, v in config_status.items() if not v and k != "all_ready"]
    logger.warning(f"Componentes faltando: {', '.join(missing)}")

# =============================================================================
# END
# =============================================================================
