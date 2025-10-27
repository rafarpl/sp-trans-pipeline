# =============================================================================
# SPTRANS PIPELINE - SERVING MODULE
# =============================================================================
# Camada de serving - PostgreSQL e Redis
# =============================================================================

"""
Serving Module

Fornece dados processados para consultas e dashboards:
    - PostgreSQL: Armazenamento de dados agregados
    - Redis: Cache de dados em tempo real
    - API Layer: Interface para consumo de dados

Componentes:
    - postgres_loader: Carrega dados Gold no PostgreSQL
    - redis_cache: Cache de dados em memória com Redis
"""

__version__ = "1.0.0"

# =============================================================================
# IMPORTS
# =============================================================================

# PostgreSQL
try:
    from .postgres_loader import (
        PostgresLoader,
        create_postgres_loader,
        load_to_postgres,
    )
except ImportError:
    PostgresLoader = None
    create_postgres_loader = None
    load_to_postgres = None

# Redis
try:
    from .redis_cache import (
        RedisCache,
        create_redis_cache,
        cache_vehicle_positions,
        get_cached_positions,
    )
except ImportError:
    RedisCache = None
    create_redis_cache = None
    cache_vehicle_positions = None
    get_cached_positions = None

# =============================================================================
# EXPORTS
# =============================================================================

__all__ = [
    # PostgreSQL
    "PostgresLoader",
    "create_postgres_loader",
    "load_to_postgres",
    
    # Redis
    "RedisCache",
    "create_redis_cache",
    "cache_vehicle_positions",
    "get_cached_positions",
    
    # Validation
    "validate_serving_components",
    "get_serving_info",
]

# =============================================================================
# VALIDATION
# =============================================================================

def validate_serving_components() -> dict:
    """
    Valida disponibilidade dos componentes de serving.
    
    Returns:
        Dicionário com status de cada componente
    """
    import os
    
    status = {
        "postgres_loader_available": PostgresLoader is not None,
        "redis_cache_available": RedisCache is not None,
        "postgres_configured": bool(os.getenv("POSTGRES_HOST")),
        "redis_configured": bool(os.getenv("REDIS_HOST")),
    }
    
    # Verificar dependências Python
    try:
        import psycopg2
        status["psycopg2_installed"] = True
    except ImportError:
        status["psycopg2_installed"] = False
    
    try:
        import redis
        status["redis_python_installed"] = True
    except ImportError:
        status["redis_python_installed"] = False
    
    status["all_available"] = all([
        status["postgres_loader_available"],
        status["redis_cache_available"],
        status["psycopg2_installed"],
        status["redis_python_installed"],
    ])
    
    return status


def check_serving_connections() -> dict:
    """
    Verifica conectividade com serviços de serving.
    
    Returns:
        Dicionário com status de conexão
    """
    connections = {
        "postgres": False,
        "redis": False,
    }
    
    # Testar PostgreSQL
    if PostgresLoader is not None:
        try:
            loader = create_postgres_loader()
            connections["postgres"] = loader.test_connection()
        except Exception as e:
            connections["postgres_error"] = str(e)
    
    # Testar Redis
    if RedisCache is not None:
        try:
            cache = create_redis_cache()
            connections["redis"] = cache.ping()
        except Exception as e:
            connections["redis_error"] = str(e)
    
    connections["all_connected"] = all([
        connections.get("postgres", False),
        connections.get("redis", False),
    ])
    
    return connections


def get_serving_info() -> dict:
    """
    Retorna informações sobre o módulo de serving.
    
    Returns:
        Dicionário com metadados
    """
    import os
    
    return {
        "module": "serving",
        "version": __version__,
        "components": validate_serving_components(),
        "connections": check_serving_connections(),
        "postgres_host": os.getenv("POSTGRES_HOST", "not configured"),
        "redis_host": os.getenv("REDIS_HOST", "not configured"),
    }

# =============================================================================
# INITIALIZATION HELPERS
# =============================================================================

def initialize_serving_layer():
    """
    Inicializa a camada de serving.
    
    - Cria conexões com PostgreSQL e Redis
    - Valida schemas
    - Prepara cache
    """
    import logging
    
    logger = logging.getLogger(__name__)
    
    logger.info("Inicializando camada de serving...")
    
    # Validar componentes
    components = validate_serving_components()
    
    if not components["all_available"]:
        missing = [k for k, v in components.items() if not v and k != "all_available"]
        logger.warning(f"Componentes faltando: {', '.join(missing)}")
        return False
    
    # Verificar conexões
    connections = check_serving_connections()
    
    if not connections.get("postgres", False):
        logger.error("Falha ao conectar com PostgreSQL")
        return False
    
    if not connections.get("redis", False):
        logger.error("Falha ao conectar com Redis")
        return False
    
    logger.info("✓ Camada de serving inicializada com sucesso")
    
    return True

# =============================================================================
# QUICK ACCESS FUNCTIONS
# =============================================================================

def quick_load_to_postgres(
    data: dict,
    table_name: str,
    schema: str = "serving"
) -> bool:
    """
    Atalho para carregar dados no PostgreSQL.
    
    Args:
        data: Dados a carregar (dict ou DataFrame)
        table_name: Nome da tabela
        schema: Schema do PostgreSQL
    
    Returns:
        True se sucesso
    
    Example:
        >>> quick_load_to_postgres(df, "kpis_hourly")
    """
    if PostgresLoader is None:
        raise ImportError("PostgresLoader não disponível")
    
    loader = create_postgres_loader()
    
    return loader.load_data(data, table_name, schema)


def quick_cache_data(
    key: str,
    value: any,
    ttl: int = 300
) -> bool:
    """
    Atalho para cachear dados no Redis.
    
    Args:
        key: Chave do cache
        value: Valor a cachear
        ttl: Tempo de vida em segundos
    
    Returns:
        True se sucesso
    
    Example:
        >>> quick_cache_data("vehicle:12345", position_data)
    """
    if RedisCache is None:
        raise ImportError("RedisCache não disponível")
    
    cache = create_redis_cache()
    
    return cache.set(key, value, ttl)


def quick_get_cached(key: str) -> any:
    """
    Atalho para obter dados do cache.
    
    Args:
        key: Chave do cache
    
    Returns:
        Valor cacheado ou None
    
    Example:
        >>> data = quick_get_cached("vehicle:12345")
    """
    if RedisCache is None:
        raise ImportError("RedisCache não disponível")
    
    cache = create_redis_cache()
    
    return cache.get(key)

# =============================================================================
# MONITORING
# =============================================================================

def get_serving_metrics() -> dict:
    """
    Retorna métricas da camada de serving.
    
    Returns:
        Dicionário com métricas
    """
    metrics = {
        "timestamp": None,
        "postgres": {},
        "redis": {},
    }
    
    from datetime import datetime
    metrics["timestamp"] = datetime.now().isoformat()
    
    # Métricas PostgreSQL
    if PostgresLoader is not None:
        try:
            loader = create_postgres_loader()
            metrics["postgres"]["connected"] = loader.test_connection()
            # Adicionar mais métricas conforme necessário
        except Exception as e:
            metrics["postgres"]["error"] = str(e)
    
    # Métricas Redis
    if RedisCache is not None:
        try:
            cache = create_redis_cache()
            info = cache.info()
            metrics["redis"]["connected"] = True
            metrics["redis"]["keys_count"] = info.get("keys_count", 0)
            metrics["redis"]["memory_used_mb"] = info.get("memory_used_mb", 0)
        except Exception as e:
            metrics["redis"]["error"] = str(e)
    
    return metrics

# =============================================================================
# LOGGING
# =============================================================================

import logging

logger = logging.getLogger(__name__)
logger.debug(f"Serving module v{__version__} loaded")

# Validar componentes
components_status = validate_serving_components()
if not components_status["all_available"]:
    missing = [k for k, v in components_status.items() if not v and k != "all_available"]
    logger.warning(f"Componentes faltando no módulo serving: {', '.join(missing)}")
else:
    logger.info("✓ Todos os componentes de serving disponíveis")

# Verificar conexões (apenas em modo não-teste)
import sys
if 'pytest' not in sys.modules:
    try:
        connections = check_serving_connections()
        if connections.get("all_connected", False):
            logger.info("✓ Conexões de serving estabelecidas")
        else:
            failed = [k for k, v in connections.items() 
                     if k in ["postgres", "redis"] and not v]
            if failed:
                logger.warning(f"Falha nas conexões: {', '.join(failed)}")
    except Exception as e:
        logger.debug(f"Não foi possível verificar conexões: {e}")

# =============================================================================
# END
# =============================================================================
