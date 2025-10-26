"""
Pytest Configuration and Fixtures
==================================
Configurações globais e fixtures para todos os testes.

Autor: Rafael - SPTrans Pipeline
"""

import pytest
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, Any
import pandas as pd
import json

# Adicionar src ao path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.common.config import Config
from src.ingestion.sptrans_api_client import SPTransClient


# ============================================================================
# CONFIGURAÇÕES GERAIS
# ============================================================================

@pytest.fixture(scope='session')
def test_config():
    """
    Configuração de teste.
    
    Returns:
        Config object com variáveis de ambiente de teste
    """
    # Configurar variáveis de ambiente de teste
    os.environ['ENV'] = 'test'
    os.environ['SPTRANS_API_TOKEN'] = 'test_token_12345'
    os.environ['MINIO_ENDPOINT'] = 'http://localhost:9000'
    os.environ['POSTGRES_HOST'] = 'localhost'
    
    config = Config()
    return config


@pytest.fixture(scope='session')
def spark_session():
    """
    Sessão Spark para testes.
    
    Returns:
        SparkSession configurada para testes
    """
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("SPTrans_Tests") \
        .master("local[2]") \
        .config("spark.driver.memory", "1g") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    yield spark
    
    spark.stop()


# ============================================================================
# FIXTURES DE DADOS MOCK
# ============================================================================

@pytest.fixture
def mock_api_response() -> Dict[str, Any]:
    """
    Response mockada da API SPTrans.
    
    Returns:
        Dict com estrutura de resposta da API
    """
    return {
        "hr": "14:30",
        "l": [
            {
                "c": "8000",
                "cl": 32458,
                "sl": 1,
                "lt0": "Terminal Lapa",
                "lt1": "Terminal Pinheiros",
                "qv": 2,
                "vs": [
                    {
                        "p": 51234,
                        "a": True,
                        "ta": "2025-01-15T14:30:00",
                        "py": -23.5475,
                        "px": -46.6361
                    },
                    {
                        "p": 51235,
                        "a": False,
                        "ta": "2025-01-15T14:29:30",
                        "py": -23.5480,
                        "px": -46.6370
                    }
                ]
            },
            {
                "c": "8001",
                "cl": 32459,
                "sl": 1,
                "lt0": "Terminal Sacomã",
                "lt1": "Terminal Vila Prudente",
                "qv": 1,
                "vs": [
                    {
                        "p": 52001,
                        "a": True,
                        "ta": "2025-01-15T14:28:00",
                        "py": -23.5950,
                        "px": -46.6200
                    }
                ]
            }
        ]
    }


@pytest.fixture
def mock_gtfs_routes() -> pd.DataFrame:
    """
    DataFrame mockado de rotas GTFS.
    
    Returns:
        DataFrame com dados de rotas
    """
    return pd.DataFrame({
        'route_id': ['8000', '8001', '8002'],
        'agency_id': ['1', '1', '1'],
        'route_short_name': ['8000-10', '8001-10', '8002-10'],
        'route_long_name': [
            'Terminal Lapa - Terminal Pinheiros',
            'Terminal Sacomã - Terminal Vila Prudente',
            'Terminal Parque Dom Pedro II - Terminal Grajaú'
        ],
        'route_type': [3, 3, 3],  # 3 = Ônibus
        'route_color': ['FF0000', '00FF00', '0000FF'],
        'route_text_color': ['FFFFFF', 'FFFFFF', 'FFFFFF']
    })


@pytest.fixture
def mock_gtfs_stops() -> pd.DataFrame:
    """
    DataFrame mockado de paradas GTFS.
    
    Returns:
        DataFrame com dados de paradas
    """
    return pd.DataFrame({
        'stop_id': ['1001', '1002', '1003', '1004', '1005'],
        'stop_code': ['P001', 'P002', 'P003', 'P004', 'P005'],
        'stop_name': [
            'Av. Paulista, 1000',
            'Av. Paulista, 1200',
            'Av. Paulista, 1400',
            'Av. Faria Lima, 500',
            'Av. Faria Lima, 800'
        ],
        'stop_lat': [-23.5630, -23.5640, -23.5650, -23.5700, -23.5710],
        'stop_lon': [-46.6544, -46.6554, -46.6564, -46.6800, -46.6810]
    })


@pytest.fixture
def mock_vehicle_positions_df(spark_session) -> 'DataFrame':
    """
    DataFrame Spark mockado com posições de veículos.
    
    Args:
        spark_session: Sessão Spark
    
    Returns:
        Spark DataFrame com posições
    """
    data = [
        {
            'vehicle_id': '51234',
            'route_code': '8000',
            'route_name': 'Terminal Lapa - Terminal Pinheiros',
            'latitude': -23.5475,
            'longitude': -46.6361,
            'timestamp': datetime(2025, 1, 15, 14, 30, 0),
            'has_accessibility': True,
            'speed_kmh': 25.5,
            'is_moving': True
        },
        {
            'vehicle_id': '51235',
            'route_code': '8000',
            'route_name': 'Terminal Lapa - Terminal Pinheiros',
            'latitude': -23.5480,
            'longitude': -46.6370,
            'timestamp': datetime(2025, 1, 15, 14, 29, 30),
            'has_accessibility': False,
            'speed_kmh': 15.2,
            'is_moving': True
        },
        {
            'vehicle_id': '52001',
            'route_code': '8001',
            'route_name': 'Terminal Sacomã - Terminal Vila Prudente',
            'latitude': -23.5950,
            'longitude': -46.6200,
            'timestamp': datetime(2025, 1, 15, 14, 28, 0),
            'has_accessibility': True,
            'speed_kmh': 0.0,
            'is_moving': False
        }
    ]
    
    return spark_session.createDataFrame(data)


# ============================================================================
# FIXTURES DE CLIENTES E SERVIÇOS
# ============================================================================

@pytest.fixture
def mock_sptrans_client(monkeypatch, mock_api_response):
    """
    Cliente SPTrans mockado.
    
    Returns:
        SPTransClient com métodos mockados
    """
    def mock_authenticate(self):
        return True
    
    def mock_get_all_positions(self):
        return mock_api_response
    
    # Aplicar mocks
    monkeypatch.setattr(SPTransClient, 'authenticate', mock_authenticate)
    monkeypatch.setattr(SPTransClient, 'get_all_positions', mock_get_all_positions)
    
    client = SPTransClient(token='test_token')
    return client


# ============================================================================
# FIXTURES DE ARQUIVOS
# ============================================================================

@pytest.fixture
def temp_dir(tmp_path):
    """
    Diretório temporário para testes.
    
    Args:
        tmp_path: Fixture pytest para path temporário
    
    Returns:
        Path do diretório temporário
    """
    # Criar estrutura de diretórios
    (tmp_path / "bronze").mkdir()
    (tmp_path / "silver").mkdir()
    (tmp_path / "gold").mkdir()
    
    return tmp_path


@pytest.fixture
def sample_gtfs_file(tmp_path) -> str:
    """
    Arquivo GTFS de exemplo.
    
    Returns:
        Path do arquivo criado
    """
    gtfs_dir = tmp_path / "gtfs"
    gtfs_dir.mkdir()
    
    # Criar routes.txt
    routes_file = gtfs_dir / "routes.txt"
    routes_content = """route_id,agency_id,route_short_name,route_long_name,route_type
8000,1,8000-10,Terminal Lapa - Terminal Pinheiros,3
8001,1,8001-10,Terminal Sacomã - Terminal Vila Prudente,3
"""
    routes_file.write_text(routes_content)
    
    return str(gtfs_dir)


# ============================================================================
# FIXTURES DE DADOS DE QUALIDADE
# ============================================================================

@pytest.fixture
def valid_coordinates():
    """
    Coordenadas válidas para São Paulo.
    
    Returns:
        Lista de tuplas (latitude, longitude)
    """
    return [
        (-23.5505, -46.6333),  # Centro de SP
        (-23.5475, -46.6361),  # Av. Paulista
        (-23.5630, -46.6544),  # Jardins
        (-23.5950, -46.6200),  # Vila Prudente
    ]


@pytest.fixture
def invalid_coordinates():
    """
    Coordenadas inválidas.
    
    Returns:
        Lista de tuplas (latitude, longitude)
    """
    return [
        (0, 0),              # Origem
        (100, 200),          # Fora dos limites
        (-23.5505, 0),       # Longitude zero
        (None, -46.6333),    # Latitude None
        (-23.5505, None),    # Longitude None
    ]


# ============================================================================
# FIXTURES DE TIMESTAMPS
# ============================================================================

@pytest.fixture
def valid_timestamps():
    """
    Timestamps válidos para testes.
    
    Returns:
        Lista de datetime objects
    """
    now = datetime.now()
    return [
        now,
        now - timedelta(minutes=5),
        now - timedelta(hours=1),
        now - timedelta(hours=12),
    ]


@pytest.fixture
def invalid_timestamps():
    """
    Timestamps inválidos.
    
    Returns:
        Lista de datetime objects ou None
    """
    now = datetime.now()
    return [
        now + timedelta(hours=1),      # Futuro
        now - timedelta(days=100),     # Muito antigo
        None,                          # None
        datetime(1900, 1, 1),          # Data muito antiga
    ]


# ============================================================================
# FIXTURES DE MÉTRICAS
# ============================================================================

@pytest.fixture
def sample_kpis():
    """
    KPIs de exemplo.
    
    Returns:
        Dict com KPIs
    """
    return {
        'date': datetime.now().date(),
        'hour': 14,
        'route_code': '8000',
        'total_vehicles': 15,
        'vehicles_moving': 12,
        'vehicles_stopped': 3,
        'avg_speed_kmh': 22.5,
        'max_speed_kmh': 45.0,
        'data_quality_score': 95.5
    }


# ============================================================================
# MARKERS
# ============================================================================

def pytest_configure(config):
    """
    Configuração de markers customizados.
    """
    config.addinivalue_line(
        "markers", "slow: marca testes como lentos"
    )
    config.addinivalue_line(
        "markers", "integration: marca testes de integração"
    )
    config.addinivalue_line(
        "markers", "spark: marca testes que usam Spark"
    )
    config.addinivalue_line(
        "markers", "api: marca testes que chamam APIs externas"
    )


# ============================================================================
# HOOKS
# ============================================================================

@pytest.fixture(autouse=True)
def reset_environment():
    """
    Reseta ambiente após cada teste.
    """
    yield
    # Cleanup após teste
    pass


def pytest_collection_modifyitems(config, items):
    """
    Modifica items da coleção de testes.
    """
    # Adicionar marker 'slow' para testes de integração
    for item in items:
        if "integration" in item.nodeid:
            item.add_marker(pytest.mark.slow)


# ============================================================================
# ASSERTIONS CUSTOMIZADAS
# ============================================================================

@pytest.fixture
def assert_dataframe_equal():
    """
    Função para comparar DataFrames.
    """
    def _assert_equal(df1: pd.DataFrame, df2: pd.DataFrame, **kwargs):
        """
        Compara dois DataFrames.
        
        Args:
            df1: Primeiro DataFrame
            df2: Segundo DataFrame
            **kwargs: Argumentos para pd.testing.assert_frame_equal
        """
        pd.testing.assert_frame_equal(
            df1.sort_index(axis=1).reset_index(drop=True),
            df2.sort_index(axis=1).reset_index(drop=True),
            **kwargs
        )
    
    return _assert_equal


# ============================================================================
# LOGGING PARA TESTES
# ============================================================================

@pytest.fixture(autouse=True)
def configure_test_logging(caplog):
    """
    Configura logging para testes.
    """
    import logging
    caplog.set_level(logging.INFO)


# ============================================================================
# CLEANUP
# ============================================================================

@pytest.fixture(scope="session", autouse=True)
def cleanup(request):
    """
    Cleanup após todos os testes.
    """
    def remove_test_files():
        """Remove arquivos temporários de teste."""
        import shutil
        test_dirs = ['test_output', 'test_data', '.pytest_cache']
        for dir_name in test_dirs:
            if os.path.exists(dir_name):
                shutil.rmtree(dir_name)
    
    request.addfinalizer(remove_test_files)


# ============================================================================
# INFORMAÇÕES
# ============================================================================

def pytest_report_header(config):
    """
    Header customizado para relatório de testes.
    """
    return [
        "SPTrans Pipeline - Test Suite",
        f"Python: {sys.version}",
        f"Pytest: {pytest.__version__}",
    ]
