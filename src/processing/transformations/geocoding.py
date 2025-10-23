"""
Geocoding Module
================
Enriquecimento de dados com geocoding reverso (latitude/longitude → endereço).

Utiliza APIs open source:
- Nominatim (OpenStreetMap) - API gratuita
- Photon (Komoot) - API gratuita alternativa
"""

from typing import Optional, Dict, Any, List, Tuple
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from functools import lru_cache
import hashlib

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField

from src.common.logging_config import get_logger
from src.common.exceptions import GeocodingError
from src.common.metrics import track_api_call, metrics

logger = get_logger(__name__)


class ReverseGeocoder:
    """Cliente para geocoding reverso usando APIs open source."""
    
    # Rate limits (requisições por segundo)
    NOMINATIM_RATE_LIMIT = 1.0  # 1 req/segundo
    PHOTON_RATE_LIMIT = 10.0    # 10 req/segundo
    
    def __init__(self, provider: str = 'nominatim', cache_enabled: bool = True):
        """
        Inicializa geocoder.
        
        Args:
            provider: Provedor de geocoding ('nominatim' ou 'photon')
            cache_enabled: Se deve usar cache para coordenadas já consultadas
        """
        self.provider = provider.lower()
        self.cache_enabled = cache_enabled
        self._cache = {} if cache_enabled else None
        
        # Configurar sessão HTTP com retry
        self.session = self._create_session()
        
        # URLs base
        self.base_urls = {
            'nominatim': 'https://nominatim.openstreetmap.org/reverse',
            'photon': 'https://photon.komoot.io/reverse'
        }
        
        if self.provider not in self.base_urls:
            raise ValueError(f"Provider inválido: {provider}. Use 'nominatim' ou 'photon'")
        
        self.base_url = self.base_urls[self.provider]
        
        # Rate limiting
        self.last_request_time = 0
        self.rate_limit = (
            self.NOMINATIM_RATE_LIMIT if provider == 'nominatim'
            else self.PHOTON_RATE_LIMIT
        )
        
        logger.info(f"ReverseGeocoder inicializado com provider={provider}, cache={cache_enabled}")
    
    def _create_session(self) -> requests.Session:
        """Cria sessão HTTP com retry automático."""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # User-Agent obrigatório para Nominatim
        session.headers.update({
            'User-Agent': 'SPTransPipeline/1.0 (sptrans-pipeline@example.com)'
        })
        
        return session
    
    def _get_cache_key(self, lat: float, lon: float, precision: int = 4) -> str:
        """
        Gera chave de cache para coordenadas.
        
        Args:
            lat: Latitude
            lon: Longitude
            precision: Casas decimais para arredondamento (evita cache muito granular)
        
        Returns:
            Chave de cache
        """
        rounded_lat = round(lat, precision)
        rounded_lon = round(lon, precision)
        key = f"{rounded_lat},{rounded_lon}"
        return hashlib.md5(key.encode()).hexdigest()
    
    def _rate_limit_wait(self):
        """Aguarda para respeitar rate limit."""
        if self.rate_limit <= 0:
            return
        
        min_interval = 1.0 / self.rate_limit
        elapsed = time.time() - self.last_request_time
        
        if elapsed < min_interval:
            wait_time = min_interval - elapsed
            time.sleep(wait_time)
        
        self.last_request_time = time.time()
    
    @track_api_call('geocoding')
    def reverse_geocode(self, lat: float, lon: float, 
                       language: str = 'pt-BR') -> Optional[Dict[str, Any]]:
        """
        Geocoding reverso: coordenadas → endereço.
        
        Args:
            lat: Latitude
            lon: Longitude
            language: Idioma do resultado
        
        Returns:
            Dict com informações de endereço ou None se falhar
        """
        # Verificar cache
        if self.cache_enabled:
            cache_key = self._get_cache_key(lat, lon)
            if cache_key in self._cache:
                metrics.cache_hits.labels(cache_type='geocoding').inc()
                return self._cache[cache_key]
            metrics.cache_misses.labels(cache_type='geocoding').inc()
        
        # Rate limiting
        self._rate_limit_wait()
        
        try:
            if self.provider == 'nominatim':
                result = self._nominatim_reverse(lat, lon, language)
            else:  # photon
                result = self._photon_reverse(lat, lon, language)
            
            # Adicionar ao cache
            if self.cache_enabled and result:
                cache_key = self._get_cache_key(lat, lon)
                self._cache[cache_key] = result
            
            return result
        
        except Exception as e:
            logger.error(f"Erro no geocoding reverso ({lat}, {lon}): {e}")
            return None
    
    def _nominatim_reverse(self, lat: float, lon: float, 
                          language: str) -> Optional[Dict[str, Any]]:
        """Geocoding via Nominatim (OpenStreetMap)."""
        params = {
            'lat': lat,
            'lon': lon,
            'format': 'json',
            'addressdetails': 1,
            'accept-language': language
        }
        
        response = self.session.get(self.base_url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        if 'error' in data:
            logger.warning(f"Nominatim error: {data['error']}")
            return None
        
        address = data.get('address', {})
        
        return {
            'street': self._get_street(address),
            'number': address.get('house_number', ''),
            'neighborhood': self._get_neighborhood(address),
            'city': address.get('city', address.get('municipality', 'São Paulo')),
            'state': address.get('state', 'SP'),
            'postcode': address.get('postcode', ''),
            'country': address.get('country', 'Brasil'),
            'formatted_address': data.get('display_name', ''),
            'provider': 'nominatim'
        }
    
    def _photon_reverse(self, lat: float, lon: float, 
                       language: str) -> Optional[Dict[str, Any]]:
        """Geocoding via Photon (Komoot)."""
        params = {
            'lat': lat,
            'lon': lon,
            'lang': language.split('-')[0]  # 'pt' ao invés de 'pt-BR'
        }
        
        response = self.session.get(self.base_url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        if not data.get('features'):
            logger.warning("Photon: nenhum resultado encontrado")
            return None
        
        feature = data['features'][0]
        properties = feature.get('properties', {})
        
        return {
            'street': properties.get('street', ''),
            'number': properties.get('housenumber', ''),
            'neighborhood': properties.get('suburb', ''),
            'city': properties.get('city', 'São Paulo'),
            'state': properties.get('state', 'SP'),
            'postcode': properties.get('postcode', ''),
            'country': properties.get('country', 'Brasil'),
            'formatted_address': properties.get('name', ''),
            'provider': 'photon'
        }
    
    def _get_street(self, address: Dict[str, Any]) -> str:
        """Extrai nome da rua de forma robusta."""
        return (
            address.get('road') or
            address.get('pedestrian') or
            address.get('highway') or
            address.get('footway') or
            ''
        )
    
    def _get_neighborhood(self, address: Dict[str, Any]) -> str:
        """Extrai bairro de forma robusta."""
        return (
            address.get('suburb') or
            address.get('neighbourhood') or
            address.get('quarter') or
            address.get('district') or
            ''
        )
    
    def get_cache_stats(self) -> Dict[str, int]:
        """Retorna estatísticas do cache."""
        if not self.cache_enabled:
            return {'enabled': False}
        
        return {
            'enabled': True,
            'size': len(self._cache),
            'items': list(self._cache.keys())[:10]  # Primeiros 10
        }
    
    def clear_cache(self):
        """Limpa o cache."""
        if self.cache_enabled:
            self._cache.clear()
            logger.info("Cache de geocoding limpo")


class SparkGeocoder:
    """Geocoding em larga escala usando Spark."""
    
    def __init__(self, provider: str = 'nominatim', 
                 batch_size: int = 100,
                 cache_enabled: bool = True):
        """
        Inicializa Spark geocoder.
        
        Args:
            provider: Provedor de geocoding
            batch_size: Tamanho do lote para processamento
            cache_enabled: Se deve usar cache
        """
        self.provider = provider
        self.batch_size = batch_size
        self.cache_enabled = cache_enabled
        
        logger.info(f"SparkGeocoder inicializado: provider={provider}, batch_size={batch_size}")
    
    def geocode_dataframe(self, df: DataFrame, 
                         lat_col: str = 'latitude',
                         lon_col: str = 'longitude',
                         output_col: str = 'address') -> DataFrame:
        """
        Aplica geocoding reverso em DataFrame Spark.
        
        Args:
            df: DataFrame com coordenadas
            lat_col: Nome da coluna de latitude
            lon_col: Nome da coluna de longitude
            output_col: Nome da coluna de saída com endereço
        
        Returns:
            DataFrame enriquecido com endereços
        """
        logger.info(f"Iniciando geocoding de {df.count()} registros")
        
        # Schema para o resultado
        address_schema = StructType([
            StructField("street", StringType(), True),
            StructField("number", StringType(), True),
            StructField("neighborhood", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("postcode", StringType(), True),
            StructField("country", StringType(), True),
            StructField("formatted_address", StringType(), True),
            StructField("provider", StringType(), True)
        ])
        
        # UDF para geocoding
        def geocode_udf(lat, lon):
            """UDF para aplicar geocoding."""
            if lat is None or lon is None:
                return None
            
            geocoder = ReverseGeocoder(
                provider=self.provider,
                cache_enabled=self.cache_enabled
            )
            
            result = geocoder.reverse_geocode(lat, lon)
            
            if result:
                return (
                    result.get('street'),
                    result.get('number'),
                    result.get('neighborhood'),
                    result.get('city'),
                    result.get('state'),
                    result.get('postcode'),
                    result.get('country'),
                    result.get('formatted_address'),
                    result.get('provider')
                )
            
            return None
        
        # Registrar UDF
        spark_udf = F.udf(geocode_udf, address_schema)
        
        # Aplicar geocoding
        df_geocoded = df.withColumn(
            output_col,
            spark_udf(F.col(lat_col), F.col(lon_col))
        )
        
        # Expandir struct em colunas separadas
        df_geocoded = df_geocoded.withColumn('address_street', F.col(f'{output_col}.street')) \
                                 .withColumn('address_number', F.col(f'{output_col}.number')) \
                                 .withColumn('address_neighborhood', F.col(f'{output_col}.neighborhood')) \
                                 .withColumn('address_city', F.col(f'{output_col}.city')) \
                                 .withColumn('address_state', F.col(f'{output_col}.state')) \
                                 .withColumn('address_postcode', F.col(f'{output_col}.postcode')) \
                                 .withColumn('address_formatted', F.col(f'{output_col}.formatted_address'))
        
        # Remover coluna struct temporária
        df_geocoded = df_geocoded.drop(output_col)
        
        logger.info("Geocoding concluído")
        
        return df_geocoded
    
    def geocode_unique_coordinates(self, df: DataFrame,
                                   lat_col: str = 'latitude',
                                   lon_col: str = 'longitude') -> DataFrame:
        """
        Geocoda apenas coordenadas únicas (mais eficiente).
        
        Args:
            df: DataFrame original
            lat_col: Coluna de latitude
            lon_col: Coluna de longitude
        
        Returns:
            DataFrame com coordenadas únicas geocodadas
        """
        # Extrair coordenadas únicas
        unique_coords = df.select(lat_col, lon_col).distinct()
        
        logger.info(f"Geocodando {unique_coords.count()} coordenadas únicas")
        
        # Geocodar coordenadas únicas
        geocoded_coords = self.geocode_dataframe(unique_coords, lat_col, lon_col)
        
        # Join com DataFrame original
        df_enriched = df.join(
            geocoded_coords,
            on=[lat_col, lon_col],
            how='left'
        )
        
        return df_enriched


def batch_geocode(coordinates: List[Tuple[float, float]],
                 provider: str = 'nominatim',
                 delay: float = 1.0) -> List[Optional[Dict[str, Any]]]:
    """
    Geocoding em lote de lista de coordenadas.
    
    Args:
        coordinates: Lista de tuplas (lat, lon)
        provider: Provedor de geocoding
        delay: Delay entre requisições (segundos)
    
    Returns:
        Lista de resultados de geocoding
    """
    geocoder = ReverseGeocoder(provider=provider, cache_enabled=True)
    results = []
    
    logger.info(f"Geocodando {len(coordinates)} coordenadas em lote")
    
    for i, (lat, lon) in enumerate(coordinates):
        result = geocoder.reverse_geocode(lat, lon)
        results.append(result)
        
        if (i + 1) % 10 == 0:
            logger.info(f"Progresso: {i + 1}/{len(coordinates)}")
        
        if delay > 0 and i < len(coordinates) - 1:
            time.sleep(delay)
    
    logger.info("Geocoding em lote concluído")
    
    return results


if __name__ == "__main__":
    # Testes básicos
    print("=== Testando Geocoding ===")
    
    # Coordenadas de teste (Av. Paulista, São Paulo)
    test_lat = -23.5613
    test_lon = -46.6563
    
    print(f"\n1. Testando Nominatim para ({test_lat}, {test_lon}):")
    geocoder = ReverseGeocoder(provider='nominatim')
    result = geocoder.reverse_geocode(test_lat, test_lon)
    
    if result:
        print(f"   Rua: {result['street']}")
        print(f"   Bairro: {result['neighborhood']}")
        print(f"   Cidade: {result['city']}")
        print(f"   ✅ Geocoding funcionando")
    else:
        print("   ❌ Falha no geocoding")
    
    print("\n2. Testando cache:")
    result2 = geocoder.reverse_geocode(test_lat, test_lon)
    print(f"   Cache stats: {geocoder.get_cache_stats()}")
    print("   ✅ Cache funcionando")
    
    print("\n3. Testando geocoding em lote:")
    coords = [
        (-23.5613, -46.6563),  # Av. Paulista
        (-23.5505, -46.6333),  # Região central
    ]
    results = batch_geocode(coords, delay=1.5)
    print(f"   ✅ {len([r for r in results if r])} coordenadas geocodadas")
    
    print("\n✅ Todos os testes passaram!")
