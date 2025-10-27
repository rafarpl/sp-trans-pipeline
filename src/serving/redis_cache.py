# =============================================================================
# REDIS CACHE
# =============================================================================
# Gerenciador de cache Redis para dados em tempo real
# =============================================================================

"""
Redis Cache Manager

Gerencia cache de dados em memória usando Redis:
    - Posições de veículos em tempo real
    - Informações de rotas
    - Métricas agregadas
    - Dados de API

Funcionalidades:
    - Set/Get com TTL
    - Batch operations
    - Cache patterns (vehicle:*, route:*, etc)
    - Limpeza automática
"""

import json
import logging
from typing import Optional, Dict, Any, List, Union
from datetime import datetime, timedelta

try:
    import redis
    from redis.exceptions import RedisError
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    redis = None
    RedisError = Exception

# =============================================================================
# LOGGING
# =============================================================================

logger = logging.getLogger(__name__)

# =============================================================================
# REDIS CACHE CLASS
# =============================================================================

class RedisCache:
    """Gerenciador de cache Redis"""
    
    def __init__(
        self,
        host: str = "redis",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        default_ttl: int = 300,  # 5 minutos
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Inicializa conexão com Redis.
        
        Args:
            host: Host do Redis
            port: Porta do Redis
            db: Número do database
            password: Senha (se necessário)
            default_ttl: TTL padrão em segundos
            config: Configurações adicionais
        
        Raises:
            ImportError: Se redis-py não estiver instalado
        """
        if not REDIS_AVAILABLE:
            raise ImportError(
                "redis não instalado. "
                "Instale com: pip install redis"
            )
        
        self.host = host
        self.port = port
        self.db = db
        self.default_ttl = default_ttl
        self.config = config or {}
        
        # Conectar ao Redis
        try:
            self.client = redis.Redis(
                host=host,
                port=port,
                db=db,
                password=password,
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5,
                **self.config
            )
            
            # Testar conexão
            self.client.ping()
            
            logger.info(f"✓ Redis conectado: {host}:{port} (DB: {db})")
        
        except Exception as e:
            logger.error(f"Erro ao conectar com Redis: {e}")
            raise
        
        # Estatísticas
        self.hits = 0
        self.misses = 0
        self.sets = 0
    
    def ping(self) -> bool:
        """
        Testa conexão com Redis.
        
        Returns:
            True se conectado
        """
        try:
            return self.client.ping()
        except Exception as e:
            logger.error(f"Ping falhou: {e}")
            return False
    
    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None
    ) -> bool:
        """
        Define valor no cache.
        
        Args:
            key: Chave do cache
            value: Valor a cachear (será serializado como JSON)
            ttl: Tempo de vida em segundos (None = default_ttl)
        
        Returns:
            True se sucesso
        
        Example:
            >>> cache.set("vehicle:12345", {"lat": -23.5, "lon": -46.6})
        """
        try:
            ttl = ttl or self.default_ttl
            
            # Serializar valor
            if not isinstance(value, str):
                value = json.dumps(value)
            
            # Set com TTL
            result = self.client.setex(key, ttl, value)
            
            self.sets += 1
            
            logger.debug(f"Cache SET: {key} (TTL: {ttl}s)")
            
            return result
        
        except Exception as e:
            logger.error(f"Erro ao fazer SET: {e}")
            return False
    
    def get(self, key: str) -> Optional[Any]:
        """
        Obtém valor do cache.
        
        Args:
            key: Chave do cache
        
        Returns:
            Valor ou None se não encontrado
        
        Example:
            >>> data = cache.get("vehicle:12345")
        """
        try:
            value = self.client.get(key)
            
            if value is None:
                self.misses += 1
                logger.debug(f"Cache MISS: {key}")
                return None
            
            self.hits += 1
            logger.debug(f"Cache HIT: {key}")
            
            # Tentar desserializar JSON
            try:
                return json.loads(value)
            except:
                return value
        
        except Exception as e:
            logger.error(f"Erro ao fazer GET: {e}")
            return None
    
    def delete(self, key: str) -> bool:
        """
        Remove chave do cache.
        
        Args:
            key: Chave a remover
        
        Returns:
            True se removida
        """
        try:
            result = self.client.delete(key)
            logger.debug(f"Cache DELETE: {key}")
            return bool(result)
        
        except Exception as e:
            logger.error(f"Erro ao fazer DELETE: {e}")
            return False
    
    def exists(self, key: str) -> bool:
        """
        Verifica se chave existe.
        
        Args:
            key: Chave a verificar
        
        Returns:
            True se existe
        """
        try:
            return bool(self.client.exists(key))
        except Exception as e:
            logger.error(f"Erro ao verificar EXISTS: {e}")
            return False
    
    def get_ttl(self, key: str) -> Optional[int]:
        """
        Obtém TTL restante de uma chave.
        
        Args:
            key: Chave
        
        Returns:
            TTL em segundos ou None
        """
        try:
            ttl = self.client.ttl(key)
            return ttl if ttl >= 0 else None
        except Exception as e:
            logger.error(f"Erro ao obter TTL: {e}")
            return None
    
    def set_many(self, mapping: Dict[str, Any], ttl: Optional[int] = None) -> int:
        """
        Define múltiplos valores no cache.
        
        Args:
            mapping: Dicionário {key: value}
            ttl: TTL para todas as chaves
        
        Returns:
            Número de chaves inseridas
        """
        count = 0
        
        for key, value in mapping.items():
            if self.set(key, value, ttl):
                count += 1
        
        logger.info(f"Cache SET_MANY: {count}/{len(mapping)} chaves")
        
        return count
    
    def get_many(self, keys: List[str]) -> Dict[str, Any]:
        """
        Obtém múltiplos valores do cache.
        
        Args:
            keys: Lista de chaves
        
        Returns:
            Dicionário {key: value}
        """
        result = {}
        
        for key in keys:
            value = self.get(key)
            if value is not None:
                result[key] = value
        
        logger.debug(f"Cache GET_MANY: {len(result)}/{len(keys)} encontrados")
        
        return result
    
    def delete_pattern(self, pattern: str) -> int:
        """
        Remove chaves que correspondem a um pattern.
        
        Args:
            pattern: Pattern com wildcards (ex: "vehicle:*")
        
        Returns:
            Número de chaves removidas
        
        Example:
            >>> cache.delete_pattern("vehicle:*")
        """
        try:
            keys = self.client.keys(pattern)
            
            if not keys:
                return 0
            
            deleted = self.client.delete(*keys)
            
            logger.info(f"Cache DELETE_PATTERN '{pattern}': {deleted} chaves")
            
            return deleted
        
        except Exception as e:
            logger.error(f"Erro ao deletar pattern: {e}")
            return 0
    
    def get_keys(self, pattern: str = "*") -> List[str]:
        """
        Lista chaves que correspondem a um pattern.
        
        Args:
            pattern: Pattern (default: todas)
        
        Returns:
            Lista de chaves
        """
        try:
            keys = self.client.keys(pattern)
            return [k.decode() if isinstance(k, bytes) else k for k in keys]
        except Exception as e:
            logger.error(f"Erro ao listar chaves: {e}")
            return []
    
    def flush_db(self) -> bool:
        """
        Limpa todo o database.
        
        WARNING: Remove TODAS as chaves!
        
        Returns:
            True se sucesso
        """
        try:
            self.client.flushdb()
            logger.warning("Cache FLUSH: todas as chaves removidas")
            return True
        except Exception as e:
            logger.error(f"Erro ao fazer FLUSH: {e}")
            return False
    
    def info(self) -> Dict[str, Any]:
        """
        Retorna informações do Redis.
        
        Returns:
            Dicionário com informações
        """
        try:
            redis_info = self.client.info()
            
            return {
                "connected": True,
                "keys_count": self.client.dbsize(),
                "memory_used_mb": redis_info.get("used_memory", 0) / 1024 / 1024,
                "total_connections": redis_info.get("total_connections_received", 0),
                "uptime_seconds": redis_info.get("uptime_in_seconds", 0),
                "hit_rate": self._calculate_hit_rate(),
            }
        
        except Exception as e:
            logger.error(f"Erro ao obter info: {e}")
            return {"connected": False, "error": str(e)}
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Retorna estatísticas de uso do cache.
        
        Returns:
            Dicionário com estatísticas
        """
        return {
            "hits": self.hits,
            "misses": self.misses,
            "sets": self.sets,
            "hit_rate": self._calculate_hit_rate(),
            "total_requests": self.hits + self.misses,
        }
    
    def _calculate_hit_rate(self) -> float:
        """Calcula taxa de acerto do cache"""
        total = self.hits + self.misses
        if total == 0:
            return 0.0
        return (self.hits / total) * 100


# =============================================================================
# SPECIALIZED CACHE FUNCTIONS
# =============================================================================

def cache_vehicle_positions(
    cache: RedisCache,
    positions: List[Dict[str, Any]],
    ttl: int = 300
) -> int:
    """
    Cacheia posições de veículos.
    
    Args:
        cache: Instância de RedisCache
        positions: Lista de posições
        ttl: TTL em segundos
    
    Returns:
        Número de posições cacheadas
    
    Example:
        >>> positions = [{"vehicle_id": "12345", "lat": -23.5, "lon": -46.6}]
        >>> cache_vehicle_positions(cache, positions)
    """
    count = 0
    
    for position in positions:
        vehicle_id = position.get("vehicle_id")
        
        if not vehicle_id:
            continue
        
        key = f"vehicle:position:{vehicle_id}"
        
        if cache.set(key, position, ttl):
            count += 1
    
    logger.info(f"Cacheadas {count} posições de veículos")
    
    return count


def get_cached_positions(
    cache: RedisCache,
    vehicle_ids: Optional[List[str]] = None
) -> Dict[str, Dict[str, Any]]:
    """
    Obtém posições cacheadas.
    
    Args:
        cache: Instância de RedisCache
        vehicle_ids: Lista de IDs (None = todos)
    
    Returns:
        Dicionário {vehicle_id: position}
    """
    if vehicle_ids:
        keys = [f"vehicle:position:{vid}" for vid in vehicle_ids]
    else:
        keys = cache.get_keys("vehicle:position:*")
    
    positions = {}
    
    for key in keys:
        value = cache.get(key)
        if value:
            vehicle_id = key.split(":")[-1]
            positions[vehicle_id] = value
    
    logger.debug(f"Obtidas {len(positions)} posições do cache")
    
    return positions


def cache_route_info(
    cache: RedisCache,
    route_code: str,
    route_info: Dict[str, Any],
    ttl: int = 3600  # 1 hora
) -> bool:
    """
    Cacheia informações de rota.
    
    Args:
        cache: Instância de RedisCache
        route_code: Código da rota
        route_info: Informações da rota
        ttl: TTL em segundos
    
    Returns:
        True se sucesso
    """
    key = f"route:info:{route_code}"
    return cache.set(key, route_info, ttl)


def get_cached_route_info(
    cache: RedisCache,
    route_code: str
) -> Optional[Dict[str, Any]]:
    """Obtém informações de rota do cache"""
    key = f"route:info:{route_code}"
    return cache.get(key)


# =============================================================================
# FACTORY FUNCTION
# =============================================================================

def create_redis_cache(
    host: Optional[str] = None,
    port: Optional[int] = None,
    db: Optional[int] = None,
    **kwargs
) -> RedisCache:
    """
    Factory function para criar RedisCache.
    
    Args:
        host: Host do Redis (default: env REDIS_HOST)
        port: Porta (default: env REDIS_PORT)
        db: Database (default: 0)
        **kwargs: Configurações adicionais
    
    Returns:
        RedisCache configurado
    
    Example:
        >>> cache = create_redis_cache()
        >>> cache.set("key", "value")
    """
    import os
    
    host = host or os.getenv("REDIS_HOST", "redis")
    port = port or int(os.getenv("REDIS_PORT", "6379"))
    db = db or int(os.getenv("REDIS_DB", "0"))
    password = os.getenv("REDIS_PASSWORD")
    
    return RedisCache(
        host=host,
        port=port,
        db=db,
        password=password,
        config=kwargs
    )


# =============================================================================
# MAIN (para testes)
# =============================================================================

def main():
    """Função main para testes"""
    
    try:
        # Criar cache
        cache = create_redis_cache()
        
        logger.info("=== Testando Redis Cache ===")
        
        # Test 1: Set/Get
        logger.info("\n1. Test SET/GET")
        cache.set("test:key", {"data": "value"}, ttl=60)
        value = cache.get("test:key")
        logger.info(f"Value: {value}")
        
        # Test 2: Posições
        logger.info("\n2. Test Vehicle Positions")
        positions = [
            {"vehicle_id": "12345", "latitude": -23.5, "longitude": -46.6},
            {"vehicle_id": "67890", "latitude": -23.6, "longitude": -46.7},
        ]
        cache_vehicle_positions(cache, positions)
        
        cached = get_cached_positions(cache, ["12345", "67890"])
        logger.info(f"Cached positions: {len(cached)}")
        
        # Test 3: Info
        logger.info("\n3. Redis Info")
        info = cache.info()
        logger.info(f"Keys: {info['keys_count']}")
        logger.info(f"Memory: {info['memory_used_mb']:.2f} MB")
        
        # Test 4: Stats
        logger.info("\n4. Cache Stats")
        stats = cache.get_stats()
        logger.info(f"Hit rate: {stats['hit_rate']:.1f}%")
        
        logger.info("\n✓ Testes concluídos")
    
    except Exception as e:
        logger.error(f"Erro nos testes: {e}", exc_info=True)


if __name__ == "__main__":
    main()

# =============================================================================
# END
# =============================================================================
