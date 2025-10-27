# =============================================================================
# SPTRANS PIPELINE - TESTS PACKAGE
# =============================================================================
# Testes automatizados do pipeline
# =============================================================================

"""
Tests Package

Suite completa de testes para o SPTrans Pipeline:
    - Unit tests: Testes de unidade de funções e classes
    - Integration tests: Testes de integração entre componentes
    - End-to-end tests: Testes de fluxo completo
    - Fixtures: Dados e configurações para testes

Estrutura:
    - conftest.py: Configurações e fixtures do pytest
    - test_ingestion/: Testes de ingestão
    - test_processing/: Testes de processamento
    - test_serving/: Testes de serving
    - test_monitoring/: Testes de monitoramento
"""

__version__ = "1.0.0"

# =============================================================================
# IMPORTS
# =============================================================================

import sys
import os

# Adicionar src ao path para imports nos testes
src_path = os.path.join(os.path.dirname(__file__), '..', 'src')
if src_path not in sys.path:
    sys.path.insert(0, src_path)

# =============================================================================
# TEST CONFIGURATION
# =============================================================================

# Configurações globais para testes
TEST_CONFIG = {
    "environment": "test",
    "use_mock_data": True,
    "spark_master": "local[2]",
    "postgres_db": "test_db",
    "redis_db": 1,  # DB separado para testes
}

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_test_data_path(filename: str = None) -> str:
    """
    Retorna caminho para arquivos de teste.
    
    Args:
        filename: Nome do arquivo (opcional)
    
    Returns:
        Caminho completo
    
    Example:
        >>> path = get_test_data_path("sample_positions.json")
    """
    test_data_dir = os.path.join(os.path.dirname(__file__), 'test_data')
    
    if filename:
        return os.path.join(test_data_dir, filename)
    
    return test_data_dir


def create_test_spark_session(app_name: str = "test"):
    """
    Cria SparkSession para testes.
    
    Args:
        app_name: Nome da aplicação
    
    Returns:
        SparkSession configurada para testes
    
    Example:
        >>> spark = create_test_spark_session()
    """
    try:
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName(f"test-{app_name}") \
            .master("local[2]") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.ui.enabled", "false") \
            .config("spark.driver.memory", "1g") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        
        return spark
    
    except ImportError:
        raise ImportError(
            "PySpark não instalado. Instale com: pip install pyspark"
        )


def cleanup_test_spark_session(spark):
    """
    Limpa SparkSession de teste.
    
    Args:
        spark: SparkSession a ser fechada
    """
    if spark:
        spark.stop()


def get_mock_api_response(endpoint: str) -> dict:
    """
    Retorna resposta mock da API para testes.
    
    Args:
        endpoint: Nome do endpoint
    
    Returns:
        Dicionário com resposta mock
    """
    mock_responses = {
        "positions": {
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
        },
        "lines": {
            "l": [{
                "cl": 1234,
                "lc": False,
                "lt": "8000-10 - Terminal Lapa",
                "tl": 1,
                "ts": "10",
                "tp": "Terminal Lapa",
                "ts1": "Terminal Pinheiros"
            }]
        },
    }
    
    return mock_responses.get(endpoint, {})


def get_sample_dataframe(spark, data_type: str = "positions"):
    """
    Cria DataFrame de exemplo para testes.
    
    Args:
        spark: SparkSession
        data_type: Tipo de dados (positions, routes, etc)
    
    Returns:
        DataFrame de exemplo
    """
    from pyspark.sql import Row
    from datetime import datetime
    
    if data_type == "positions":
        data = [
            Row(
                vehicle_id="12345",
                timestamp=datetime(2024, 1, 15, 10, 30, 0),
                latitude=-23.5505,
                longitude=-46.6333,
                speed=35.5,
                route_code="8000-10",
                accessibility=True
            ),
            Row(
                vehicle_id="67890",
                timestamp=datetime(2024, 1, 15, 10, 31, 0),
                latitude=-23.5515,
                longitude=-46.6343,
                speed=40.2,
                route_code="8000-10",
                accessibility=False
            ),
        ]
    
    elif data_type == "routes":
        data = [
            Row(
                route_id="1",
                route_code="8000-10",
                route_short_name="8000",
                route_long_name="Terminal Lapa - Terminal Pinheiros",
                route_type=3
            ),
        ]
    
    else:
        data = []
    
    return spark.createDataFrame(data)


# =============================================================================
# TEST MARKERS
# =============================================================================

# Markers personalizados para categorizar testes
# Usar com: @pytest.mark.unit, @pytest.mark.integration, etc.

PYTEST_MARKERS = """
markers =
    unit: Unit tests
    integration: Integration tests
    e2e: End-to-end tests
    slow: Slow running tests
    spark: Tests requiring Spark
    postgres: Tests requiring PostgreSQL
    redis: Tests requiring Redis
    api: Tests for API clients
"""

# =============================================================================
# ASSERTIONS HELPERS
# =============================================================================

def assert_dataframe_equal(df1, df2, check_order: bool = False):
    """
    Verifica se dois DataFrames são iguais.
    
    Args:
        df1: Primeiro DataFrame
        df2: Segundo DataFrame
        check_order: Se True, verifica ordem das linhas
    
    Raises:
        AssertionError: Se DataFrames não forem iguais
    """
    # Verificar schema
    assert df1.schema == df2.schema, "Schemas diferentes"
    
    # Verificar contagem
    count1 = df1.count()
    count2 = df2.count()
    assert count1 == count2, f"Contagens diferentes: {count1} vs {count2}"
    
    # Verificar dados
    if check_order:
        # Comparar linha por linha
        rows1 = df1.collect()
        rows2 = df2.collect()
        assert rows1 == rows2, "Dados diferentes"
    else:
        # Comparar sem considerar ordem
        diff = df1.subtract(df2).count()
        assert diff == 0, f"Diferenças encontradas: {diff} linhas"


def assert_quality_score_above(score: float, threshold: float = 80.0):
    """
    Verifica se score de qualidade está acima do threshold.
    
    Args:
        score: Score obtido
        threshold: Threshold mínimo
    
    Raises:
        AssertionError: Se score abaixo do threshold
    """
    assert score >= threshold, f"Quality score {score} abaixo de {threshold}"


# =============================================================================
# MOCK HELPERS
# =============================================================================

class MockRedisClient:
    """Mock do Redis para testes"""
    
    def __init__(self):
        self._data = {}
    
    def set(self, key, value, ex=None):
        self._data[key] = value
        return True
    
    def get(self, key):
        return self._data.get(key)
    
    def delete(self, *keys):
        for key in keys:
            self._data.pop(key, None)
        return len(keys)
    
    def ping(self):
        return True
    
    def dbsize(self):
        return len(self._data)


class MockPostgresConnection:
    """Mock do PostgreSQL para testes"""
    
    def __init__(self):
        self.committed = False
        self.closed = False
    
    def cursor(self):
        return MockPostgresCursor()
    
    def commit(self):
        self.committed = True
    
    def close(self):
        self.closed = True


class MockPostgresCursor:
    """Mock do cursor PostgreSQL"""
    
    def __init__(self):
        self.executed_queries = []
    
    def execute(self, query, params=None):
        self.executed_queries.append((query, params))
    
    def fetchall(self):
        return []
    
    def close(self):
        pass


# =============================================================================
# LOGGING
# =============================================================================

import logging

logger = logging.getLogger(__name__)
logger.debug(f"Tests package v{__version__} loaded")

# =============================================================================
# END
# =============================================================================
