"""
SPTrans Pipeline - Serving Module

Módulo responsável pela camada de serving:
- Carregamento de dados no PostgreSQL
- Cache em Redis
- APIs de consulta
"""

from .postgres_loader import PostgresLoader, load_to_postgres
from .redis_cache import RedisCache, cache_kpis

__all__ = [
    "PostgresLoader",
    "load_to_postgres",
    "RedisCache",
    "cache_kpis",
]

__version__ = "2.0.0"