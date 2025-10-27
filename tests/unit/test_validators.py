# =============================================================================
# UNIT TESTS - VALIDATORS
# =============================================================================
# Testes para validadores de dados
# =============================================================================

import pytest
from datetime import datetime, timedelta

# =============================================================================
# TESTS - COORDINATE VALIDATOR
# =============================================================================

def test_validate_coordinates_valid():
    """Testa validação de coordenadas válidas"""
    from src.common.validators import validate_coordinates
    
    is_valid, msg = validate_coordinates(-23.5505, -46.6333)
    
    assert is_valid is True
    assert msg == ""


def test_validate_coordinates_out_of_bounds():
    """Testa coordenadas fora dos limites de SP"""
    from src.common.validators import validate_coordinates
    
    is_valid, msg = validate_coordinates(-25.0, -50.0)
    
    assert is_valid is False
    assert "limites" in msg.lower()


def test_validate_coordinates_invalid_type():
    """Testa coordenadas com tipo inválido"""
    from src.common.validators import validate_coordinates
    
    is_valid, msg = validate_coordinates("invalid", "invalid")
    
    assert is_valid is False
    assert "números" in msg.lower()


def test_validate_coordinates_zero_zero():
    """Testa coordenadas (0, 0) inválidas"""
    from src.common.validators import validate_coordinates
    
    is_valid, msg = validate_coordinates(0.0, 0.0)
    
    assert is_valid is False
    assert "inválidas" in msg.lower()


def test_coordinate_validator_class():
    """Testa classe CoordinateValidator"""
    from src.common.validators import CoordinateValidator
    
    validator = CoordinateValidator()
    
    # Coordenada válida
    is_valid, msg = validator.validate(-23.5505, -46.6333)
    assert is_valid is True
    
    # Coordenada inválida
    is_valid, msg = validator.validate(-25.0, -50.0)
    assert is_valid is False


def test_coordinate_validator_non_strict():
    """Testa validador em modo não-strict"""
    from src.common.validators import CoordinateValidator
    
    validator = CoordinateValidator(strict=False)
    
    # Coordenadas fora de SP mas válidas globalmente
    is_valid, msg = validator.validate(-22.0, -43.0)  # Rio de Janeiro
    
    assert is_valid is True  # Aceita em modo não-strict


# =============================================================================
# TESTS - TIMESTAMP VALIDATOR
# =============================================================================

def test_validate_timestamp_valid_string():
    """Testa validação de timestamp válido (string)"""
    from src.common.validators import validate_timestamp
    
    timestamp = "2024-01-15T10:30:00"
    is_valid, msg = validate_timestamp(timestamp)
    
    assert is_valid is True


def test_validate_timestamp_valid_datetime():
    """Testa validação de timestamp válido (datetime)"""
    from src.common.validators import validate_timestamp
    
    timestamp = datetime(2024, 1, 15, 10, 30, 0)
    is_valid, msg = validate_timestamp(timestamp)
    
    assert is_valid is True


def test_validate_timestamp_too_future():
    """Testa timestamp muito no futuro"""
    from src.common.validators import validate_timestamp
    
    future_time = datetime.now() + timedelta(days=10)
    is_valid, msg = validate_timestamp(future_time)
    
    assert is_valid is False
    assert "futuro" in msg.lower()


def test_validate_timestamp_too_past():
    """Testa timestamp muito no passado"""
    from src.common.validators import validate_timestamp
    
    past_time = datetime.now() - timedelta(days=100)
    is_valid, msg = validate_timestamp(past_time)
    
    assert is_valid is False
    assert "passado" in msg.lower()


def test_validate_timestamp_invalid_format():
    """Testa timestamp com formato inválido"""
    from src.common.validators import validate_timestamp
    
    invalid_timestamp = "invalid-date"
    is_valid, msg = validate_timestamp(invalid_timestamp)
    
    assert is_valid is False


# =============================================================================
# TESTS - VEHICLE ID VALIDATOR
# =============================================================================

def test_validate_vehicle_id_valid():
    """Testa validação de vehicle ID válido"""
    from src.common.validators import validate_vehicle_id
    
    is_valid, msg = validate_vehicle_id("12345")
    
    assert is_valid is True


def test_validate_vehicle_id_invalid_format():
    """Testa vehicle ID com formato inválido"""
    from src.common.validators import validate_vehicle_id
    
    is_valid, msg = validate_vehicle_id("ABC")
    
    assert is_valid is False


def test_validate_vehicle_id_too_short():
    """Testa vehicle ID muito curto"""
    from src.common.validators import validate_vehicle_id
    
    is_valid, msg = validate_vehicle_id("123")
    
    assert is_valid is False


def test_validate_vehicle_id_too_long():
    """Testa vehicle ID muito longo"""
    from src.common.validators import validate_vehicle_id
    
    is_valid, msg = validate_vehicle_id("1234567")
    
    assert is_valid is False


def test_validate_vehicle_id_none():
    """Testa vehicle ID None"""
    from src.common.validators import validate_vehicle_id
    
    is_valid, msg = validate_vehicle_id(None)
    
    assert is_valid is False


# =============================================================================
# TESTS - ROUTE CODE VALIDATOR
# =============================================================================

def test_validate_route_code_valid():
    """Testa validação de route code válido"""
    from src.common.validators import validate_route_code
    
    is_valid, msg = validate_route_code("8000-10")
    
    assert is_valid is True


def test_validate_route_code_valid_variations():
    """Testa variações válidas de route code"""
    from src.common.validators import validate_route_code
    
    valid_codes = ["8000", "8000-10", "A123", "B-456"]
    
    for code in valid_codes:
        is_valid, msg = validate_route_code(code)
        assert is_valid is True, f"Code {code} should be valid"


def test_validate_route_code_invalid():
    """Testa route code inválido"""
    from src.common.validators import validate_route_code
    
    is_valid, msg = validate_route_code("invalid@code")
    
    assert is_valid is False


def test_validate_route_code_empty():
    """Testa route code vazio"""
    from src.common.validators import validate_route_code
    
    is_valid, msg = validate_route_code("")
    
    assert is_valid is False


# =============================================================================
# TESTS - DATA QUALITY VALIDATOR
# =============================================================================

def test_data_quality_validator_valid_record():
    """Testa validação de registro válido"""
    from src.common.validators import DataQualityValidator
    
    validator = DataQualityValidator()
    
    record = {
        "vehicle_id": "12345",
        "timestamp": datetime(2024, 1, 15, 10, 30, 0),
        "latitude": -23.5505,
        "longitude": -46.6333,
        "speed": 35.5,
    }
    
    result = validator.validate_record(record)
    
    assert result.is_valid is True
    assert len(result.errors) == 0


def test_data_quality_validator_missing_fields():
    """Testa validação com campos faltando"""
    from src.common.validators import DataQualityValidator
    
    validator = DataQualityValidator()
    
    record = {
        "vehicle_id": "12345",
        # timestamp, latitude, longitude missing
    }
    
    result = validator.validate_record(record)
    
    assert result.is_valid is False
    assert len(result.errors) > 0


def test_data_quality_validator_invalid_coordinates():
    """Testa validação com coordenadas inválidas"""
    from src.common.validators import DataQualityValidator
    
    validator = DataQualityValidator()
    
    record = {
        "vehicle_id": "12345",
        "timestamp": datetime.now(),
        "latitude": 0.0,
        "longitude": 0.0,
    }
    
    result = validator.validate_record(record)
    
    assert result.is_valid is False
    assert any("coordenadas" in str(e).lower() for e in result.errors)


def test_data_quality_validator_batch():
    """Testa validação em lote"""
    from src.common.validators import DataQualityValidator
    
    validator = DataQualityValidator()
    
    records = [
        {
            "vehicle_id": "12345",
            "timestamp": datetime.now(),
            "latitude": -23.5505,
            "longitude": -46.6333,
        },
        {
            "vehicle_id": "67890",
            "timestamp": datetime.now(),
            "latitude": 0.0,  # Invalid
            "longitude": 0.0,  # Invalid
        },
    ]
    
    result = validator.validate_batch(records)
    
    assert result['total_records'] == 2
    assert result['valid_records'] == 1
    assert result['invalid_records'] == 1


# =============================================================================
# TESTS - SCHEMA VALIDATOR
# =============================================================================

def test_validate_schema_valid():
    """Testa validação de schema válido"""
    from src.common.validators import validate_schema
    
    data = {
        "vehicle_id": "12345",
        "latitude": -23.5505,
        "longitude": -46.6333,
    }
    
    expected_schema = {
        "vehicle_id": str,
        "latitude": float,
        "longitude": float,
    }
    
    is_valid, errors = validate_schema(data, expected_schema)
    
    assert is_valid is True
    assert len(errors) == 0


def test_validate_schema_missing_field():
    """Testa schema com campo faltando"""
    from src.common.validators import validate_schema
    
    data = {
        "vehicle_id": "12345",
        # latitude missing
    }
    
    expected_schema = {
        "vehicle_id": str,
        "latitude": float,
    }
    
    is_valid, errors = validate_schema(data, expected_schema)
    
    assert is_valid is False
    assert len(errors) > 0


def test_validate_schema_wrong_type():
    """Testa schema com tipo incorreto"""
    from src.common.validators import validate_schema
    
    data = {
        "vehicle_id": 12345,  # Should be str
        "latitude": "-23.5505",  # Should be float
    }
    
    expected_schema = {
        "vehicle_id": str,
        "latitude": float,
    }
    
    is_valid, errors = validate_schema(data, expected_schema)
    
    assert is_valid is False
    assert len(errors) > 0


# =============================================================================
# TESTS - COMPLETENESS
# =============================================================================

def test_calculate_completeness_full():
    """Testa cálculo de completude com 100%"""
    from src.common.validators import calculate_completeness
    
    data = {
        "field1": "value1",
        "field2": "value2",
        "field3": "value3",
    }
    
    required = ["field1", "field2", "field3"]
    
    completeness = calculate_completeness(data, required)
    
    assert completeness == 100.0


def test_calculate_completeness_partial():
    """Testa cálculo de completude parcial"""
    from src.common.validators import calculate_completeness
    
    data = {
        "field1": "value1",
        "field2": None,
        "field3": "value3",
    }
    
    required = ["field1", "field2", "field3"]
    
    completeness = calculate_completeness(data, required)
    
    assert completeness == pytest.approx(66.67, rel=0.1)


def test_calculate_completeness_empty():
    """Testa cálculo de completude vazia"""
    from src.common.validators import calculate_completeness
    
    data = {
        "field1": None,
        "field2": None,
    }
    
    required = ["field1", "field2"]
    
    completeness = calculate_completeness(data, required)
    
    assert completeness == 0.0


# =============================================================================
# TESTS - UTILITY VALIDATORS
# =============================================================================

def test_is_valid_date():
    """Testa validação de data"""
    from src.common.validators import is_valid_date
    
    assert is_valid_date("2024-01-15") is True
    assert is_valid_date("invalid") is False


def test_sanitize_string():
    """Testa sanitização de string"""
    from src.common.validators import sanitize_string
    
    dirty = "Test @ String! 123"
    clean = sanitize_string(dirty)
    
    assert "@" not in clean
    assert "!" not in clean
    assert "Test" in clean


def test_sanitize_string_max_length():
    """Testa sanitização com limite de tamanho"""
    from src.common.validators import sanitize_string
    
    long_string = "a" * 300
    clean = sanitize_string(long_string, max_length=100)
    
    assert len(clean) <= 100


# =============================================================================
# END
# =============================================================================
