# =============================================================================
# SPTRANS PIPELINE - UNIT TESTS
# =============================================================================
# Testes unitários de componentes individuais
# =============================================================================

"""
Unit Tests Package

Testes unitários para componentes individuais do pipeline:
    - API clients
    - Validators
    - Transformations
    - Aggregations
    - Utils
    - Data quality checkers

Cada módulo é testado isoladamente com mocks e fixtures.
"""

__version__ = "1.0.0"

# =============================================================================
# TEST UTILITIES
# =============================================================================

def get_mock_vehicle_position():
    """Retorna posição de veículo mock para testes"""
    from datetime import datetime
    
    return {
        "vehicle_id": "12345",
        "timestamp": datetime(2024, 1, 15, 10, 30, 0),
        "latitude": -23.5505,
        "longitude": -46.6333,
        "speed": 35.5,
        "route_code": "8000-10",
        "accessibility": True,
    }


def get_mock_route_data():
    """Retorna dados de rota mock para testes"""
    return {
        "route_id": "1",
        "route_code": "8000-10",
        "route_short_name": "8000",
        "route_long_name": "Terminal Lapa - Terminal Pinheiros",
        "route_type": 3,
    }

# =============================================================================
# END
# =============================================================================
