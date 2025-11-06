"""
SPTrans Pipeline - Transformations Module

Módulo responsável por todas as transformações de dados:
- Data Quality (validações e limpeza)
- Deduplication (remoção de duplicatas)
- Enrichment (enriquecimento com dados externos)
- Geocoding (conversão coordenadas ↔ endereços)
- Aggregations (agregações e cálculo de KPIs)
"""

from .data_quality import DataQualityChecker, validate_vehicle_position
from .deduplication import Deduplicator, deduplicate_vehicles
from .enrichment import DataEnricher, enrich_with_gtfs
from .geocoding import GeocodingService, reverse_geocode
from .aggregations import KPICalculator, calculate_fleet_coverage

__all__ = [
    # Data Quality
    "DataQualityChecker",
    "validate_vehicle_position",
    
    # Deduplication
    "Deduplicator",
    "deduplicate_vehicles",
    
    # Enrichment
    "DataEnricher",
    "enrich_with_gtfs",
    
    # Geocoding
    "GeocodingService",
    "reverse_geocode",
    
    # Aggregations
    "KPICalculator",
    "calculate_fleet_coverage",
]

__version__ = "2.0.0"