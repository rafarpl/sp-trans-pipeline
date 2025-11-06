"""
Geocoding Module

Responsável por conversão entre coordenadas e endereços:
- Reverse geocoding (lat/lon → endereço)
- Geocoding (endereço → lat/lon)
- Cache de resultados para performance
"""

from typing import Dict, Optional, Tuple
from functools import lru_cache
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

from src.common.logging_config import get_logger
from src.common.metrics import MetricsCollector
from src.common.exceptions import SPTransPipelineException

logger = get_logger(__name__)


class GeocodingException(SPTransPipelineException):
    """Exceção para erros de geocoding."""
    pass


class GeocodingService:
    """
    Serviço de geocoding e reverse geocoding.
    
    Usa Nominatim (OpenStreetMap) para conversões gratuitas com cache.
    Para produção, considere usar serviços pagos como Google Maps API.
    """
    
    def __init__(
        self,
        user_agent: str = "sptrans-pipeline",
        timeout: int = 5,
        use_cache: bool = True
    ):
        """
        Inicializa o serviço de geocoding.
        
        Args:
            user_agent: User agent para requisições
            timeout: Timeout em segundos
            use_cache: Se deve usar cache local
        """
        self.geolocator = Nominatim(user_agent=user_agent, timeout=timeout)
        self.use_cache = use_cache
        self.metrics = MetricsCollector()
        self.logger = get_logger(self.__class__.__name__)
        
        # Contadores
        self.cache_hits = 0
        self.cache_misses = 0
        self.api_calls = 0
        self.failures = 0
    
    @lru_cache(maxsize=10000)
    def _cached_reverse_geocode(
        self,
        lat: float,
        lon: float,
        precision: int = 3
    ) -> Optional[Dict[str, str]]:
        """
        Reverse geocoding com cache (interno).
        
        Args:
            lat: Latitude (arredondada para precision)
            lon: Longitude (arredondada para precision)
            precision: Casas decimais para arredondar (para cache)
            
        Returns:
            Dicionário com informações de endereço ou None
        """
        try:
            location = self.geolocator.reverse(f"{lat}, {lon}", language="pt-BR")
            
            if location and location.raw:
                address = location.raw.get("address", {})
                return {
                    "full_address": location.address,
                    "road": address.get("road"),
                    "suburb": address.get("suburb"),
                    "city": address.get("city", address.get("municipality")),
                    "state": address.get("state"),
                    "postcode": address.get("postcode"),
                    "country": address.get("country"),
                }
            
            return None
            
        except (GeocoderTimedOut, GeocoderServiceError) as e:
            self.logger.warning(f"Geocoding error for ({lat}, {lon}): {e}")
            self.failures += 1
            return None
    
    def reverse_geocode(
        self,
        latitude: float,
        longitude: float,
        precision: int = 3
    ) -> Optional[Dict[str, str]]:
        """
        Converte coordenadas em endereço (reverse geocoding).
        
        Args:
            latitude: Latitude
            longitude: Longitude
            precision: Precisão para cache (casas decimais)
            
        Returns:
            Dicionário com informações de endereço ou None
        """
        # Arredondar para cache
        lat_rounded = round(latitude, precision)
        lon_rounded = round(longitude, precision)
        
        self.api_calls += 1
        
        if self.use_cache:
            result = self._cached_reverse_geocode(lat_rounded, lon_rounded, precision)
            if result:
                self.cache_hits += 1
            else:
                self.cache_misses += 1
            return result
        else:
            return self._cached_reverse_geocode(lat_rounded, lon_rounded, precision)
    
    def batch_reverse_geocode(
        self,
        coordinates: list[Tuple[float, float]],
        delay_seconds: float = 1.0
    ) -> list[Optional[Dict[str, str]]]:
        """
        Reverse geocoding em lote.
        
        Args:
            coordinates: Lista de tuplas (latitude, longitude)
            delay_seconds: Delay entre requisições (rate limiting)
            
        Returns:
            Lista de dicionários com endereços
        """
        self.logger.info(f"Batch reverse geocoding {len(coordinates)} coordinates")
        
        results = []
        
        for i, (lat, lon) in enumerate(coordinates):
            result = self.reverse_geocode(lat, lon)
            results.append(result)
            
            # Rate limiting (respeitar política de uso da API)
            if i < len(coordinates) - 1:
                time.sleep(delay_seconds)
            
            if (i + 1) % 100 == 0:
                self.logger.info(f"Processed {i + 1}/{len(coordinates)} coordinates")
        
        success_count = sum(1 for r in results if r is not None)
        self.logger.info(
            f"Batch geocoding completed: {success_count}/{len(coordinates)} successful"
        )
        
        return results
    
    def get_statistics(self) -> Dict[str, int]:
        """
        Retorna estatísticas de uso do serviço.
        
        Returns:
            Dicionário com estatísticas
        """
        total_requests = self.cache_hits + self.cache_misses
        cache_hit_rate = (
            (self.cache_hits / total_requests * 100) if total_requests > 0 else 0
        )
        
        stats = {
            "total_requests": total_requests,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "cache_hit_rate_percent": round(cache_hit_rate, 2),
            "api_calls": self.api_calls,
            "failures": self.failures,
        }
        
        # Publicar métricas
        self.metrics.gauge("geocoding.cache_hit_rate", cache_hit_rate)
        self.metrics.counter("geocoding.api_calls", self.api_calls)
        self.metrics.counter("geocoding.failures", self.failures)
        
        return stats


class SparkGeocodingUDF:
    """
    UDF para usar geocoding em Spark DataFrames.
    
    Wrapper para usar o GeocodingService como UDF no Spark.
    """
    
    def __init__(self, geocoding_service: Optional[GeocodingService] = None):
        """
        Inicializa o UDF de geocoding.
        
        Args:
            geocoding_service: Serviço de geocoding (opcional)
        """
        self.service = geocoding_service or GeocodingService()
        self.logger = get_logger(self.__class__.__name__)
    
    def create_reverse_geocode_udf(self):
        """
        Cria UDF para reverse geocoding.
        
        Returns:
            PySpark UDF
        """
        def reverse_geocode_udf(lat: float, lon: float) -> Optional[str]:
            """UDF para reverse geocoding."""
            if lat is None or lon is None:
                return None
            
            try:
                result = self.service.reverse_geocode(lat, lon)
                return result.get("full_address") if result else None
            except Exception as e:
                logger.warning(f"Error in geocoding UDF: {e}")
                return None
        
        return F.udf(reverse_geocode_udf, StringType())
    
    def create_get_road_udf(self):
        """
        Cria UDF para obter nome da rua.
        
        Returns:
            PySpark UDF
        """
        def get_road_udf(lat: float, lon: float) -> Optional[str]:
            """UDF para obter nome da rua."""
            if lat is None or lon is None:
                return None
            
            try:
                result = self.service.reverse_geocode(lat, lon)
                return result.get("road") if result else None
            except Exception as e:
                logger.warning(f"Error in get_road UDF: {e}")
                return None
        
        return F.udf(get_road_udf, StringType())
    
    def create_get_neighborhood_udf(self):
        """
        Cria UDF para obter bairro.
        
        Returns:
            PySpark UDF
        """
        def get_neighborhood_udf(lat: float, lon: float) -> Optional[str]:
            """UDF para obter bairro."""
            if lat is None or lon is None:
                return None
            
            try:
                result = self.service.reverse_geocode(lat, lon)
                return result.get("suburb") if result else None
            except Exception as e:
                logger.warning(f"Error in get_neighborhood UDF: {e}")
                return None
        
        return F.udf(get_neighborhood_udf, StringType())


def add_geocoding_to_dataframe(
    df: DataFrame,
    latitude_col: str = "latitude",
    longitude_col: str = "longitude",
    address_col: str = "address",
    road_col: str = "road",
    neighborhood_col: str = "neighborhood"
) -> DataFrame:
    """
    Adiciona colunas de geocoding a um DataFrame.
    
    Args:
        df: DataFrame de entrada com coordenadas
        latitude_col: Nome da coluna de latitude
        longitude_col: Nome da coluna de longitude
        address_col: Nome para coluna de endereço completo
        road_col: Nome para coluna de rua
        neighborhood_col: Nome para coluna de bairro
        
    Returns:
        DataFrame com colunas de geocoding adicionadas
    """
    logger.info("Adding geocoding columns to DataFrame")
    
    # Criar UDFs
    spark_geocoding = SparkGeocodingUDF()
    reverse_geocode_udf = spark_geocoding.create_reverse_geocode_udf()
    get_road_udf = spark_geocoding.create_get_road_udf()
    get_neighborhood_udf = spark_geocoding.create_get_neighborhood_udf()
    
    # Aplicar UDFs
    df_with_geocoding = (
        df
        .withColumn(address_col, reverse_geocode_udf(F.col(latitude_col), F.col(longitude_col)))
        .withColumn(road_col, get_road_udf(F.col(latitude_col), F.col(longitude_col)))
        .withColumn(neighborhood_col, get_neighborhood_udf(F.col(latitude_col), F.col(longitude_col)))
    )
    
    # Contar sucessos
    geocoded_count = df_with_geocoding.filter(F.col(address_col).isNotNull()).count()
    total_count = df.count()
    
    success_rate = (geocoded_count / total_count * 100) if total_count > 0 else 0
    
    logger.info(
        f"Geocoding completed: {geocoded_count}/{total_count} records "
        f"({success_rate:.2f}% success rate)"
    )
    
    return df_with_geocoding


def reverse_geocode(
    latitude: float,
    longitude: float
) -> Optional[Dict[str, str]]:
    """
    Função utilitária para reverse geocoding simples.
    
    Args:
        latitude: Latitude
        longitude: Longitude
        
    Returns:
        Dicionário com informações de endereço ou None
    """
    service = GeocodingService()
    return service.reverse_geocode(latitude, longitude)


def create_geocoding_lookup_table(
    spark: SparkSession,
    coordinates_df: DataFrame,
    output_path: str
) -> None:
    """
    Cria tabela de lookup de geocoding para reuso.
    
    Útil para pré-processar coordenadas comuns e evitar chamadas repetidas à API.
    
    Args:
        spark: SparkSession
        coordinates_df: DataFrame com coordenadas únicas
        output_path: Caminho para salvar tabela de lookup
    """
    logger.info("Creating geocoding lookup table")
    
    # Obter coordenadas únicas
    unique_coords = (
        coordinates_df
        .select("latitude", "longitude")
        .distinct()
    )
    
    logger.info(f"Found {unique_coords.count()} unique coordinates")
    
    # Aplicar geocoding
    df_with_geocoding = add_geocoding_to_dataframe(unique_coords)
    
    # Salvar
    df_with_geocoding.write.mode("overwrite").parquet(output_path)
    
    logger.info(f"Geocoding lookup table saved to {output_path}")