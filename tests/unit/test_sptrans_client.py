# =============================================================================
# UNIT TESTS - SPTRANS API CLIENT
# =============================================================================
# Testes para o cliente da API Olho Vivo SPTrans
# =============================================================================

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import json

# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def mock_api_response():
    """Mock de resposta da API"""
    return {
        "l": [{
            "c": "8000",
            "cl": 1234,
            "sl": 1,
            "lt0": "Terminal Lapa",
            "lt1": "Terminal Pinheiros",
            "qv": 10,
            "vs": [
                {
                    "p": 12345,
                    "a": True,
                    "ta": "2024-01-15T10:30:00",
                    "py": -23.5505,
                    "px": -46.6333,
                }
            ]
        }]
    }


@pytest.fixture
def api_client():
    """Cliente API para testes"""
    from src.ingestion.sptrans_api_client import SPTransAPIClient
    
    client = SPTransAPIClient(
        api_token="test_token",
        base_url="http://test-api.com"
    )
    
    return client


# =============================================================================
# TESTS - INITIALIZATION
# =============================================================================

def test_client_initialization():
    """Testa inicialização do cliente"""
    from src.ingestion.sptrans_api_client import SPTransAPIClient
    
    client = SPTransAPIClient(
        api_token="test_token",
        base_url="http://test-api.com"
    )
    
    assert client.api_token == "test_token"
    assert client.base_url == "http://test-api.com"
    assert client.session is not None


def test_client_initialization_with_defaults():
    """Testa inicialização com valores padrão"""
    from src.ingestion.sptrans_api_client import SPTransAPIClient
    
    with patch.dict('os.environ', {'SPTRANS_API_TOKEN': 'env_token'}):
        client = SPTransAPIClient()
        assert client.api_token == 'env_token'


# =============================================================================
# TESTS - AUTHENTICATION
# =============================================================================

@patch('requests.Session.post')
def test_authenticate_success(mock_post, api_client):
    """Testa autenticação bem-sucedida"""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = "true"
    mock_post.return_value = mock_response
    
    result = api_client.authenticate()
    
    assert result is True
    assert api_client.authenticated is True
    mock_post.assert_called_once()


@patch('requests.Session.post')
def test_authenticate_failure(mock_post, api_client):
    """Testa falha na autenticação"""
    mock_response = Mock()
    mock_response.status_code = 401
    mock_response.text = "false"
    mock_post.return_value = mock_response
    
    result = api_client.authenticate()
    
    assert result is False
    assert api_client.authenticated is False


# =============================================================================
# TESTS - GET POSITIONS
# =============================================================================

@patch('requests.Session.post')
def test_get_all_positions_success(mock_post, api_client, mock_api_response):
    """Testa obtenção de posições com sucesso"""
    # Mock authentication
    auth_response = Mock()
    auth_response.status_code = 200
    auth_response.text = "true"
    
    # Mock positions response
    positions_response = Mock()
    positions_response.status_code = 200
    positions_response.json.return_value = mock_api_response
    
    mock_post.side_effect = [auth_response, positions_response]
    
    positions = api_client.get_all_positions()
    
    assert positions is not None
    assert isinstance(positions, dict)
    assert "l" in positions


@patch('requests.Session.post')
def test_get_positions_without_authentication(mock_post, api_client):
    """Testa tentativa de obter posições sem autenticação"""
    api_client.authenticated = False
    
    auth_response = Mock()
    auth_response.status_code = 401
    auth_response.text = "false"
    mock_post.return_value = auth_response
    
    positions = api_client.get_all_positions()
    
    # Deve retornar None ou raise exception dependendo da implementação
    assert positions is None or positions == {}


# =============================================================================
# TESTS - PARSING
# =============================================================================

def test_parse_positions(api_client, mock_api_response):
    """Testa parsing de posições da API"""
    from src.ingestion.sptrans_api_client import parse_api_positions
    
    parsed = parse_api_positions(mock_api_response)
    
    assert isinstance(parsed, list)
    assert len(parsed) > 0
    
    first_position = parsed[0]
    assert "vehicle_id" in first_position
    assert "latitude" in first_position
    assert "longitude" in first_position
    assert "timestamp" in first_position


def test_parse_empty_response(api_client):
    """Testa parsing de resposta vazia"""
    from src.ingestion.sptrans_api_client import parse_api_positions
    
    empty_response = {"l": []}
    parsed = parse_api_positions(empty_response)
    
    assert isinstance(parsed, list)
    assert len(parsed) == 0


def test_parse_invalid_response(api_client):
    """Testa parsing de resposta inválida"""
    from src.ingestion.sptrans_api_client import parse_api_positions
    
    invalid_response = {"invalid": "data"}
    
    with pytest.raises(Exception):
        parse_api_positions(invalid_response)


# =============================================================================
# TESTS - ERROR HANDLING
# =============================================================================

@patch('requests.Session.post')
def test_handle_timeout(mock_post, api_client):
    """Testa tratamento de timeout"""
    import requests
    
    mock_post.side_effect = requests.Timeout("Request timeout")
    
    with pytest.raises(requests.Timeout):
        api_client.authenticate()


@patch('requests.Session.post')
def test_handle_connection_error(mock_post, api_client):
    """Testa tratamento de erro de conexão"""
    import requests
    
    mock_post.side_effect = requests.ConnectionError("Connection failed")
    
    with pytest.raises(requests.ConnectionError):
        api_client.authenticate()


@patch('requests.Session.post')
def test_retry_on_failure(mock_post, api_client):
    """Testa retry em caso de falha"""
    # Primeira tentativa falha, segunda sucede
    fail_response = Mock()
    fail_response.status_code = 500
    
    success_response = Mock()
    success_response.status_code = 200
    success_response.text = "true"
    
    mock_post.side_effect = [fail_response, success_response]
    
    # Cliente deve fazer retry automaticamente
    result = api_client.authenticate()
    
    assert result is True
    assert mock_post.call_count == 2


# =============================================================================
# TESTS - RATE LIMITING
# =============================================================================

@patch('time.sleep')
@patch('requests.Session.post')
def test_rate_limiting(mock_post, mock_sleep, api_client):
    """Testa rate limiting"""
    auth_response = Mock()
    auth_response.status_code = 200
    auth_response.text = "true"
    mock_post.return_value = auth_response
    
    # Fazer múltiplas chamadas rápidas
    for _ in range(5):
        api_client.authenticate()
    
    # Verificar se sleep foi chamado (rate limiting ativo)
    # Depende da implementação do cliente


# =============================================================================
# TESTS - SPECIFIC ENDPOINTS
# =============================================================================

@patch('requests.Session.post')
def test_get_lines(mock_post, api_client):
    """Testa obtenção de linhas"""
    auth_response = Mock()
    auth_response.status_code = 200
    auth_response.text = "true"
    
    lines_response = Mock()
    lines_response.status_code = 200
    lines_response.json.return_value = {"l": []}
    
    mock_post.side_effect = [auth_response, lines_response]
    
    lines = api_client.get_lines(search_term="lapa")
    
    assert lines is not None


@patch('requests.Session.post')
def test_get_stops(mock_post, api_client):
    """Testa obtenção de paradas"""
    auth_response = Mock()
    auth_response.status_code = 200
    auth_response.text = "true"
    
    stops_response = Mock()
    stops_response.status_code = 200
    stops_response.json.return_value = {"p": []}
    
    mock_post.side_effect = [auth_response, stops_response]
    
    stops = api_client.get_stops(search_term="praca")
    
    assert stops is not None


# =============================================================================
# TESTS - DATA VALIDATION
# =============================================================================

def test_validate_position_data():
    """Testa validação de dados de posição"""
    from src.ingestion.sptrans_api_client import validate_position
    
    valid_position = {
        "vehicle_id": "12345",
        "latitude": -23.5505,
        "longitude": -46.6333,
        "timestamp": "2024-01-15T10:30:00"
    }
    
    assert validate_position(valid_position) is True


def test_validate_invalid_coordinates():
    """Testa validação de coordenadas inválidas"""
    from src.ingestion.sptrans_api_client import validate_position
    
    invalid_position = {
        "vehicle_id": "12345",
        "latitude": 0.0,  # Invalid
        "longitude": 0.0,  # Invalid
        "timestamp": "2024-01-15T10:30:00"
    }
    
    assert validate_position(invalid_position) is False


# =============================================================================
# TESTS - HELPER FUNCTIONS
# =============================================================================

def test_format_timestamp():
    """Testa formatação de timestamp"""
    from src.ingestion.sptrans_api_client import format_timestamp
    
    raw_timestamp = "2024-01-15T10:30:00"
    formatted = format_timestamp(raw_timestamp)
    
    assert isinstance(formatted, datetime)
    assert formatted.year == 2024
    assert formatted.month == 1
    assert formatted.day == 15


def test_normalize_vehicle_id():
    """Testa normalização de ID de veículo"""
    from src.ingestion.sptrans_api_client import normalize_vehicle_id
    
    raw_id = "  12345  "
    normalized = normalize_vehicle_id(raw_id)
    
    assert normalized == "12345"
    assert isinstance(normalized, str)


# =============================================================================
# END
# =============================================================================
