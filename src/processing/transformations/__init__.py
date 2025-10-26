# =============================================================================
# SPTRANS PIPELINE - TRANSFORMATIONS MODULE
# =============================================================================
# Transformações reutilizáveis para processamento de dados
# =============================================================================

"""
Transformations Module

Transformações aplicadas aos dados durante o processamento:
    - Data Quality: Verificações de qualidade
    - Deduplication: Remoção de duplicatas
    - Enrichment: Enriquecimento com dados GTFS
    - Geocoding: Geocoding reverso (coordenadas → endereços)
    - Aggregations: Agregações e cálculo de KPIs
"""

__version__ = "1.0.0"

# =============================================================================
# IMPORTS
# =============================================================================

# Data Quality
try:
    from .data_quality import (
        DataQualityChecker,
        calculate_quality_score,
        validate_positions_dataframe,
    )
except ImportError:
    DataQualityChecker = None
    calculate_quality_score = None
    validate_positions_dataframe = None

# Deduplication
try:
    from .deduplication import (
        dedup_positions,
        remove_duplicates,
        deduplicate_by_key,
    )
except ImportError:
    dedup_positions = None
    remove_duplicates = None
    deduplicate_by_key = None

# Enrichment
try:
    from .enrichment import (
        enrich_with_gtfs,
        join_with_routes,
        join_with_stops,
        add_route_info,
    )
except ImportError:
    enrich_with_gtfs = None
    join_with_routes = None
    join_with_stops = None
    add_route_info = None

# Geocoding
try:
    from .geocoding import (
        reverse_geocode,
        geocode_coordinates,
        add_address_info,
    )
except ImportError:
    reverse_geocode = None
    geocode_coordinates = None
    add_address_info = None

# Aggregations
try:
    from .aggregations import (
        calculate_hourly_kpis,
        calculate_route_metrics,
        calculate_headway,
        calculate_system_summary,
        aggregate_by_route,
        aggregate_by_hour,
    )
except ImportError:
    calculate_hourly_kpis = None
    calculate_route_metrics = None
    calculate_headway = None
    calculate_system_summary = None
    aggregate_by_route = None
    aggregate_by_hour = None

# =============================================================================
# EXPORTS
# =============================================================================

__all__ = [
    # Data Quality
    "DataQualityChecker",
    "calculate_quality_score",
    "validate_positions_dataframe",
    
    # Deduplication
    "dedup_positions",
    "remove_duplicates",
    "deduplicate_by_key",
    
    # Enrichment
    "enrich_with_gtfs",
    "join_with_routes",
    "join_with_stops",
    "add_route_info",
    
    # Geocoding
    "reverse_geocode",
    "geocode_coordinates",
    "add_address_info",
    
    # Aggregations
    "calculate_hourly_kpis",
    "calculate_route_metrics",
    "calculate_headway",
    "calculate_system_summary",
    "aggregate_by_route",
    "aggregate_by_hour",
]

# =============================================================================
# VALIDATION
# =============================================================================

def validate_transformations() -> dict:
    """
    Valida disponibilidade de transformações.
    
    Returns:
        Dicionário com status de cada módulo
    """
    return {
        "data_quality": DataQualityChecker is not None,
        "deduplication": dedup_positions is not None,
        "enrichment": enrich_with_gtfs is not None,
        "geocoding": reverse_geocode is not None,
        "aggregations": calculate_hourly_kpis is not None,
    }

# =============================================================================
# MODULE INFO
# =============================================================================

def get_transformations_info() -> dict:
    """Retorna informações sobre transformações disponíveis"""
    return {
        "module": "transformations",
        "version": __version__,
        "available": validate_transformations(),
    }

# =============================================================================
# LOGGING
# =============================================================================

import logging

logger = logging.getLogger(__name__)
logger.debug(f"Transformations module v{__version__} loaded")

# Validar módulos
status = validate_transformations()
available = [k for k, v in status.items() if v]
missing = [k for k, v in status.items() if not v]

if available:
    logger.info(f"Transformações disponíveis: {', '.join(available)}")
if missing:
    logger.warning(f"Transformações faltando: {', '.join(missing)}")

# =============================================================================
# END
# =============================================================================
