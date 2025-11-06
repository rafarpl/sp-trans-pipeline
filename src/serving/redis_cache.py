"""
Redis Cache Module

Responsável por cache de dados em Redis:
- Cache de KPIs
- Cache de queries frequentes
- Real-time data caching
"""

from typing import Any, Dict, List, Optional, Union
import json
from datetime import datetime, timedelta

import redis
from redis.exceptions import RedisError

from src.common.config import get_config
from src.common.logging_config import get_logger
from src.common.exceptions import StorageException
from src.common.metrics import MetricsCollector

logger = get_logger(__name__)


class RedisCache:
    """
    Classe para gerenciar cache em Redis.
    
    Suporta:
    - Cache de objetos JSON
    - TTL (Time To Live)
    - Bulk operations
    - Pattern matching
    """
    
    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        db: int = 0,
        password: Optional[str] = None,
        decode_responses: bool = True
    ):
        """
        Inicializa o RedisCache.
        
        Args:
            host: Host do Redis (usa config se None)
            port: Porta do Redis (usa config se None)
            db: Número do database Redis
            password: Senha do Redis (opcional)
            decode_responses: Se deve decodificar respostas como strings
        """
        config = get_config()
        
        self.host = host or config.REDIS_HOST
        self.port = port or config.REDIS_PORT
        self.db = db
        self.password = password or config.REDIS_PASSWORD
        
        self.metrics = MetricsCollector()
        self.logger = get_logger(self.__class__.__name__)
        
        # Conectar ao Redis
        try:
            self.client = redis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password,
                decode_responses=decode_responses,
                socket_connect_timeout=5,
                socket_timeout=5
            )
            
            # Testar conexão
            self.client.ping()
            self.logger.info(f"Connected to Redis at {self.host}:{self.port}")
            
        except RedisError as e:
            self.logger.error(f"Failed to connect to Redis: {e}")
            raise StorageException(f"Redis connection failed: {e}")
    
    def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: Optional[int] = None
    ) -> bool:
        """
        Define um valor no cache.
        
        Args:
            key: Chave
            value: Valor (será serializado para JSON)
            ttl_seconds: Tempo de vida em segundos (opcional)
            
        Returns:
            True se sucesso, False caso contrário
        """
        try:
            # Serializar para JSON se não for string
            if not isinstance(value, str):
                value = json.dumps(value)
            
            if ttl_seconds:
                result = self.client.setex(key, ttl_seconds, value)
            else:
                result = self.client.set(key, value)
            
            self.metrics.counter("redis.set_operations", 1)
            return bool(result)
            
        except Exception as e:
            self.logger.error(f"Redis SET failed for key '{key}': {e}")
            self.metrics.counter("redis.errors", 1)
            return False
    
    def get(self, key: str, deserialize: bool = True) -> Optional[Any]:
        """
        Obtém um valor do cache.
        
        Args:
            key: Chave
            deserialize: Se deve deserializar JSON
            
        Returns:
            Valor ou None se não encontrado
        """
        try:
            value = self.client.get(key)
            
            if value is None:
                self.metrics.counter("redis.cache_misses", 1)
                return None
            
            self.metrics.counter("redis.cache_hits", 1)
            
            # Deserializar JSON se solicitado
            if deserialize and isinstance(value, str):
                try:
                    value = json.loads(value)
                except json.JSONDecodeError:
                    pass  # Retornar string se não for JSON
            
            return value
            
        except Exception as e:
            self.logger.error(f"Redis GET failed for key '{key}': {e}")
            self.metrics.counter("redis.errors", 1)
            return None
    
    def delete(self, *keys: str) -> int:
        """
        Remove uma ou mais chaves do cache.
        
        Args:
            *keys: Chaves a remover
            
        Returns:
            Número de chaves removidas
        """
        try:
            deleted_count = self.client.delete(*keys)
            self.metrics.counter("redis.delete_operations", len(keys))
            return deleted_count
            
        except Exception as e:
            self.logger.error(f"Redis DELETE failed: {e}")
            self.metrics.counter("redis.errors", 1)
            return 0
    
    def exists(self, *keys: str) -> int:
        """
        Verifica se chaves existem.
        
        Args:
            *keys: Chaves a verificar
            
        Returns:
            Número de chaves que existem
        """
        try:
            return self.client.exists(*keys)
        except Exception as e:
            self.logger.error(f"Redis EXISTS failed: {e}")
            return 0
    
    def expire(self, key: str, ttl_seconds: int) -> bool:
        """
        Define TTL para uma chave existente.
        
        Args:
            key: Chave
            ttl_seconds: Tempo de vida em segundos
            
        Returns:
            True se sucesso
        """
        try:
            return bool(self.client.expire(key, ttl_seconds))
        except Exception as e:
            self.logger.error(f"Redis EXPIRE failed for key '{key}': {e}")
            return False
    
    def ttl(self, key: str) -> int:
        """
        Obtém TTL restante de uma chave.
        
        Args:
            key: Chave
            
        Returns:
            TTL em segundos (-1 se não tem TTL, -2 se não existe)
        """
        try:
            return self.client.ttl(key)
        except Exception as e:
            self.logger.error(f"Redis TTL failed for key '{key}': {e}")
            return -2
    
    def set_multiple(
        self,
        mapping: Dict[str, Any],
        ttl_seconds: Optional[int] = None
    ) -> bool:
        """
        Define múltiplos valores de uma vez.
        
        Args:
            mapping: Dicionário chave-valor
            ttl_seconds: TTL para todas as chaves (opcional)
            
        Returns:
            True se sucesso
        """
        try:
            # Serializar valores
            serialized = {}
            for key, value in mapping.items():
                if not isinstance(value, str):
                    value = json.dumps(value)
                serialized[key] = value
            
            # SET múltiplo
            result = self.client.mset(serialized)
            
            # Aplicar TTL se especificado
            if ttl_seconds and result:
                for key in serialized.keys():
                    self.client.expire(key, ttl_seconds)
            
            self.metrics.counter("redis.mset_operations", len(mapping))
            return bool(result)
            
        except Exception as e:
            self.logger.error(f"Redis MSET failed: {e}")
            self.metrics.counter("redis.errors", 1)
            return False
    
    def get_multiple(
        self,
        keys: List[str],
        deserialize: bool = True
    ) -> Dict[str, Optional[Any]]:
        """
        Obtém múltiplos valores de uma vez.
        
        Args:
            keys: Lista de chaves
            deserialize: Se deve deserializar JSON
            
        Returns:
            Dicionário chave-valor
        """
        try:
            values = self.client.mget(keys)
            
            result = {}
            for key, value in zip(keys, values):
                if value and deserialize:
                    try:
                        value = json.loads(value)
                    except json.JSONDecodeError:
                        pass
                result[key] = value
            
            hits = sum(1 for v in values if v is not None)
            self.metrics.counter("redis.cache_hits", hits)
            self.metrics.counter("redis.cache_misses", len(keys) - hits)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Redis MGET failed: {e}")
            self.metrics.counter("redis.errors", 1)
            return {key: None for key in keys}
    
    def keys_matching(self, pattern: str) -> List[str]:
        """
        Busca chaves que correspondem a um padrão.
        
        Args:
            pattern: Padrão (ex: "kpi:*", "vehicle:*")
            
        Returns:
            Lista de chaves que correspondem
        """
        try:
            return self.client.keys(pattern)
        except Exception as e:
            self.logger.error(f"Redis KEYS failed for pattern '{pattern}': {e}")
            return []
    
    def delete_pattern(self, pattern: str) -> int:
        """
        Remove todas as chaves que correspondem a um padrão.
        
        Args:
            pattern: Padrão
            
        Returns:
            Número de chaves removidas
        """
        try:
            keys = self.keys_matching(pattern)
            if keys:
                return self.delete(*keys)
            return 0
        except Exception as e:
            self.logger.error(f"Redis DELETE PATTERN failed: {e}")
            return 0
    
    def flush_db(self) -> bool:
        """
        Remove todas as chaves do database atual.
        
        ⚠️ CUIDADO: Operação destrutiva!
        
        Returns:
            True se sucesso
        """
        self.logger.warning(f"Flushing Redis database {self.db}")
        try:
            return bool(self.client.flushdb())
        except Exception as e:
            self.logger.error(f"Redis FLUSHDB failed: {e}")
            return False
    
    def info(self) -> Dict[str, Any]:
        """
        Obtém informações do servidor Redis.
        
        Returns:
            Dicionário com informações
        """
        try:
            return self.client.info()
        except Exception as e:
            self.logger.error(f"Redis INFO failed: {e}")
            return {}
    
    def get_cache_statistics(self) -> Dict[str, int]:
        """
        Obtém estatísticas de uso do cache.
        
        Returns:
            Dicionário com estatísticas
        """
        info = self.info()
        
        return {
            "total_keys": info.get("db0", {}).get("keys", 0),
            "used_memory_bytes": info.get("used_memory", 0),
            "connected_clients": info.get("connected_clients", 0),
            "total_commands_processed": info.get("total_commands_processed", 0),
            "keyspace_hits": info.get("keyspace_hits", 0),
            "keyspace_misses": info.get("keyspace_misses", 0),
        }


def cache_kpis(
    kpis: Dict[str, Any],
    ttl_minutes: int = 60,
    prefix: str = "kpi"
) -> bool:
    """
    Função utilitária para cachear KPIs.
    
    Args:
        kpis: Dicionário com KPIs
        ttl_minutes: TTL em minutos
        prefix: Prefixo para as chaves
        
    Returns:
        True se sucesso
    """
    cache = RedisCache()
    
    # Adicionar timestamp
    kpis["cached_at"] = datetime.now().isoformat()
    
    # Construir chaves
    cache_data = {}
    for kpi_name, kpi_value in kpis.items():
        key = f"{prefix}:{kpi_name}"
        cache_data[key] = kpi_value
    
    # Cachear
    return cache.set_multiple(cache_data, ttl_seconds=ttl_minutes * 60)


def get_cached_kpi(kpi_name: str, prefix: str = "kpi") -> Optional[Any]:
    """
    Obtém um KPI do cache.
    
    Args:
        kpi_name: Nome do KPI
        prefix: Prefixo das chaves
        
    Returns:
        Valor do KPI ou None
    """
    cache = RedisCache()
    key = f"{prefix}:{kpi_name}"
    return cache.get(key)


def cache_vehicle_positions(
    vehicle_positions: List[Dict],
    ttl_seconds: int = 180
) -> bool:
    """
    Cacheia posições de veículos (dados em tempo real).
    
    Args:
        vehicle_positions: Lista de posições
        ttl_seconds: TTL em segundos (3 minutos padrão)
        
    Returns:
        True se sucesso
    """
    cache = RedisCache()
    
    # Agrupar por vehicle_id
    cache_data = {}
    for position in vehicle_positions:
        vehicle_id = position.get("vehicle_id")
        if vehicle_id:
            key = f"vehicle:position:{vehicle_id}"
            cache_data[key] = position
    
    return cache.set_multiple(cache_data, ttl_seconds=ttl_seconds)