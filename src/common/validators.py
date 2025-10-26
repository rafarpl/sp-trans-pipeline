"""
Data Validators Module
======================
Funções de validação para garantir qualidade dos dados do pipeline SPTrans.

Inclui validações para:
- Coordenadas geográficas
- Timestamps
- IDs de veículos
- Códigos de linhas/rotas
- Dados GTFS
"""

from typing import Optional, Tuple, List, Dict, Any
from datetime import datetime, timedelta
import re
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

from src.common.logging_config import get_logger
from src.common.exceptions import ValidationError

logger = get_logger(__name__)


class CoordinateValidator:
    """Validador de coordenadas geográficas."""
    
    # Limites aproximados da cidade de São Paulo
    SAO_PAULO_BOUNDS = {
        'lat_min': -24.008,
        'lat_max': -23.357,
        'lon_min': -46.826,
        'lon_max': -46.365
    }
    
    # Limites mais amplos (região metropolitana)
    METRO_SP_BOUNDS = {
        'lat_min': -24.5,
        'lat_max': -23.0,
        'lon_min': -47.5,
        'lon_max': -46.0
    }
    
    @classmethod
    def is_valid_latitude(cls, lat: float, strict: bool = True) -> bool:
        """
        Valida latitude.
        
        Args:
            lat: Latitude para validar
            strict: Se True, usa limites da cidade. Se False, usa limites da região metropolitana
        
        Returns:
            True se válida, False caso contrário
        """
        if lat is None or not isinstance(lat, (int, float)):
            return False
        
        bounds = cls.SAO_PAULO_BOUNDS if strict else cls.METRO_SP_BOUNDS
        return bounds['lat_min'] <= lat <= bounds['lat_max']
    
    @classmethod
    def is_valid_longitude(cls, lon: float, strict: bool = True) -> bool:
        """
        Valida longitude.
        
        Args:
            lon: Longitude para validar
            strict: Se True, usa limites da cidade. Se False, usa limites da região metropolitana
        
        Returns:
            True se válida, False caso contrário
        """
        if lon is None or not isinstance(lon, (int, float)):
            return False
        
        bounds = cls.SAO_PAULO_BOUNDS if strict else cls.METRO_SP_BOUNDS
        return bounds['lon_min'] <= lon <= bounds['lon_max']
    
    @classmethod
    def is_valid_coordinate(cls, lat: float, lon: float, strict: bool = True) -> bool:
        """
        Valida par de coordenadas.
        
        Args:
            lat: Latitude
            lon: Longitude
            strict: Se True, usa limites da cidade
        
        Returns:
            True se ambas coordenadas são válidas
        """
        return cls.is_valid_latitude(lat, strict) and cls.is_valid_longitude(lon, strict)
    
    @classmethod
    def validate_coordinates_df(cls, df: DataFrame, lat_col: str = 'latitude', 
                               lon_col: str = 'longitude', strict: bool = True) -> DataFrame:
        """
        Adiciona coluna de validação de coordenadas em DataFrame Spark.
        
        Args:
            df: DataFrame Spark
            lat_col: Nome da coluna de latitude
            lon_col: Nome da coluna de longitude
            strict: Se True, usa limites da cidade
        
        Returns:
            DataFrame com coluna 'is_valid_coordinate'
        """
        bounds = cls.SAO_PAULO_BOUNDS if strict else cls.METRO_SP_BOUNDS
        
        df = df.withColumn(
            'is_valid_coordinate',
            (F.col(lat_col).between(bounds['lat_min'], bounds['lat_max'])) &
            (F.col(lon_col).between(bounds['lon_min'], bounds['lon_max']))
        )
        
        return df


class TimestampValidator:
    """Validador de timestamps."""
    
    @staticmethod
    def is_valid_timestamp(ts: datetime, max_future_seconds: int = 300,
                          max_past_hours: int = 24) -> bool:
        """
        Valida se timestamp está em intervalo aceitável.
        
        Args:
            ts: Timestamp para validar
            max_future_seconds: Máximo de segundos no futuro permitido
            max_past_hours: Máximo de horas no passado permitido
        
        Returns:
            True se válido
        """
        if ts is None or not isinstance(ts, datetime):
            return False
        
        now = datetime.now()
        max_future = now + timedelta(seconds=max_future_seconds)
        max_past = now - timedelta(hours=max_past_hours)
        
        return max_past <= ts <= max_future
    
    @staticmethod
    def parse_sptrans_timestamp(ts_str: str) -> Optional[datetime]:
        """
        Parse timestamp do formato SPTrans (ex: "2024-01-20T18:45:30").
        
        Args:
            ts_str: String de timestamp
        
        Returns:
            datetime object ou None se inválido
        """
        if not ts_str:
            return None
        
        try:
            # Formato: "2024-01-20T18:45:30"
            return datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            try:
                # Formato alternativo com milissegundos
                return datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%f")
            except ValueError:
                logger.warning(f"Formato de timestamp inválido: {ts_str}")
                return None
    
    @classmethod
    def validate_timestamps_df(cls, df: DataFrame, ts_col: str = 'timestamp',
                              max_future_seconds: int = 300,
                              max_past_hours: int = 24) -> DataFrame:
        """
        Adiciona coluna de validação de timestamps em DataFrame Spark.
        
        Args:
            df: DataFrame Spark
            ts_col: Nome da coluna de timestamp
            max_future_seconds: Máximo de segundos no futuro
            max_past_hours: Máximo de horas no passado
        
        Returns:
            DataFrame com coluna 'is_valid_timestamp'
        """
        now = F.current_timestamp()
        max_future = F.expr(f"current_timestamp() + interval {max_future_seconds} seconds")
        max_past = F.expr(f"current_timestamp() - interval {max_past_hours} hours")
        
        df = df.withColumn(
            'is_valid_timestamp',
            (F.col(ts_col).between(max_past, max_future)) & F.col(ts_col).isNotNull()
        )
        
        return df


class VehicleValidator:
    """Validador de dados de veículos."""
    
    @staticmethod
    def is_valid_vehicle_id(vehicle_id: Any) -> bool:
        """
        Valida ID de veículo (prefixo).
        
        Args:
            vehicle_id: ID do veículo
        
        Returns:
            True se válido
        """
        if vehicle_id is None:
            return False
        
        # Converter para string e validar
        vid_str = str(vehicle_id)
        
        # ID deve ser numérico e ter entre 1 e 10 dígitos
        return vid_str.isdigit() and 1 <= len(vid_str) <= 10
    
    @staticmethod
    def validate_vehicles_df(df: DataFrame, vehicle_col: str = 'vehicle_id') -> DataFrame:
        """
        Adiciona coluna de validação de IDs de veículos.
        
        Args:
            df: DataFrame Spark
            vehicle_col: Nome da coluna de vehicle ID
        
        Returns:
            DataFrame com coluna 'is_valid_vehicle_id'
        """
        df = df.withColumn(
            'is_valid_vehicle_id',
            (F.col(vehicle_col).isNotNull()) &
            (F.length(F.col(vehicle_col).cast('string')).between(1, 10)) &
            (F.col(vehicle_col).cast('string').rlike('^[0-9]+$'))
        )
        
        return df


class RouteValidator:
    """Validador de rotas/linhas."""
    
    @staticmethod
    def is_valid_route_code(route_code: Any) -> bool:
        """
        Valida código de rota.
        
        Args:
            route_code: Código da rota
        
        Returns:
            True se válido
        """
        if route_code is None:
            return False
        
        rc_str = str(route_code)
        
        # Código deve ser alfanumérico e ter entre 1 e 20 caracteres
        return bool(re.match(r'^[A-Z0-9\-]+$', rc_str, re.IGNORECASE)) and 1 <= len(rc_str) <= 20
    
    @staticmethod
    def is_valid_direction(direction: Any) -> bool:
        """
        Valida direção da linha (1 ou 2).
        
        Args:
            direction: Direção da linha
        
        Returns:
            True se válido
        """
        return direction in [1, 2, '1', '2']
    
    @staticmethod
    def validate_routes_df(df: DataFrame, route_col: str = 'route_code',
                          direction_col: str = 'direction') -> DataFrame:
        """
        Adiciona coluna de validação de rotas.
        
        Args:
            df: DataFrame Spark
            route_col: Nome da coluna de código da rota
            direction_col: Nome da coluna de direção
        
        Returns:
            DataFrame com coluna 'is_valid_route'
        """
        df = df.withColumn(
            'is_valid_route',
            (F.col(route_col).isNotNull()) &
            (F.length(F.col(route_col).cast('string')).between(1, 20)) &
            (F.col(direction_col).isin([1, 2]))
        )
        
        return df


class GTFSValidator:
    """Validador de dados GTFS."""
    
    @staticmethod
    def validate_gtfs_routes(df: DataFrame) -> Dict[str, Any]:
        """
        Valida arquivo routes.txt do GTFS.
        
        Args:
            df: DataFrame Spark com dados de routes
        
        Returns:
            Dict com estatísticas de validação
        """
        total = df.count()
        
        # Validações
        valid_route_id = df.filter(F.col('route_id').isNotNull()).count()
        valid_route_short_name = df.filter(F.col('route_short_name').isNotNull()).count()
        valid_route_type = df.filter(F.col('route_type').isin([0, 1, 2, 3, 4, 5, 6, 7])).count()
        
        return {
            'total_records': total,
            'valid_route_id': valid_route_id,
            'valid_route_short_name': valid_route_short_name,
            'valid_route_type': valid_route_type,
            'completeness_pct': (valid_route_id / total * 100) if total > 0 else 0
        }
    
    @staticmethod
    def validate_gtfs_stops(df: DataFrame) -> Dict[str, Any]:
        """
        Valida arquivo stops.txt do GTFS.
        
        Args:
            df: DataFrame Spark com dados de stops
        
        Returns:
            Dict com estatísticas de validação
        """
        total = df.count()
        
        # Validações
        valid_stop_id = df.filter(F.col('stop_id').isNotNull()).count()
        valid_stop_name = df.filter(F.col('stop_name').isNotNull()).count()
        
        # Validar coordenadas
        df_with_validation = CoordinateValidator.validate_coordinates_df(
            df, 'stop_lat', 'stop_lon', strict=False
        )
        valid_coordinates = df_with_validation.filter(F.col('is_valid_coordinate')).count()
        
        return {
            'total_records': total,
            'valid_stop_id': valid_stop_id,
            'valid_stop_name': valid_stop_name,
            'valid_coordinates': valid_coordinates,
            'completeness_pct': (valid_stop_id / total * 100) if total > 0 else 0
        }


class DataQualityValidator:
    """Validador geral de qualidade de dados."""
    
    @staticmethod
    def check_nulls(df: DataFrame, columns: Optional[List[str]] = None) -> Dict[str, float]:
        """
        Verifica porcentagem de nulos por coluna.
        
        Args:
            df: DataFrame Spark
            columns: Lista de colunas para verificar (None = todas)
        
        Returns:
            Dict com % de nulos por coluna
        """
        if columns is None:
            columns = df.columns
        
        total = df.count()
        null_counts = {}
        
        for col in columns:
            null_count = df.filter(F.col(col).isNull()).count()
            null_counts[col] = (null_count / total * 100) if total > 0 else 0
        
        return null_counts
    
    @staticmethod
    def check_duplicates(df: DataFrame, key_columns: List[str]) -> Tuple[int, float]:
        """
        Verifica duplicatas baseado em colunas chave.
        
        Args:
            df: DataFrame Spark
            key_columns: Colunas que formam a chave única
        
        Returns:
            Tupla (número de duplicatas, % de duplicatas)
        """
        total = df.count()
        unique = df.dropDuplicates(key_columns).count()
        duplicates = total - unique
        
        return duplicates, (duplicates / total * 100) if total > 0 else 0
    
    @staticmethod
    def check_completeness(df: DataFrame, required_columns: List[str]) -> Dict[str, Any]:
        """
        Verifica completude dos dados.
        
        Args:
            df: DataFrame Spark
            required_columns: Colunas obrigatórias
        
        Returns:
            Dict com estatísticas de completude
        """
        total = df.count()
        
        # Registros completos (sem nulos nas colunas obrigatórias)
        complete_condition = None
        for col in required_columns:
            if complete_condition is None:
                complete_condition = F.col(col).isNotNull()
            else:
                complete_condition = complete_condition & F.col(col).isNotNull()
        
        complete_records = df.filter(complete_condition).count() if complete_condition else total
        
        return {
            'total_records': total,
            'complete_records': complete_records,
            'incomplete_records': total - complete_records,
            'completeness_pct': (complete_records / total * 100) if total > 0 else 0
        }
    
    @staticmethod
    def validate_all(df: DataFrame, validation_rules: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executa todas as validações configuradas.
        
        Args:
            df: DataFrame Spark
            validation_rules: Dict com regras de validação
        
        Returns:
            Dict com resultados de todas validações
        """
        results = {}
        
        # Validar coordenadas se especificado
        if 'coordinates' in validation_rules:
            coord_rules = validation_rules['coordinates']
            df = CoordinateValidator.validate_coordinates_df(
                df, 
                lat_col=coord_rules.get('lat_col', 'latitude'),
                lon_col=coord_rules.get('lon_col', 'longitude'),
                strict=coord_rules.get('strict', True)
            )
            valid_coords = df.filter(F.col('is_valid_coordinate')).count()
            results['valid_coordinates'] = valid_coords
            results['valid_coordinates_pct'] = (valid_coords / df.count() * 100) if df.count() > 0 else 0
        
        # Validar timestamps se especificado
        if 'timestamps' in validation_rules:
            ts_rules = validation_rules['timestamps']
            df = TimestampValidator.validate_timestamps_df(
                df,
                ts_col=ts_rules.get('ts_col', 'timestamp')
            )
            valid_ts = df.filter(F.col('is_valid_timestamp')).count()
            results['valid_timestamps'] = valid_ts
            results['valid_timestamps_pct'] = (valid_ts / df.count() * 100) if df.count() > 0 else 0
        
        # Verificar nulos
        if 'null_check_columns' in validation_rules:
            results['null_percentages'] = DataQualityValidator.check_nulls(
                df, validation_rules['null_check_columns']
            )
        
        # Verificar duplicatas
        if 'duplicate_key_columns' in validation_rules:
            dup_count, dup_pct = DataQualityValidator.check_duplicates(
                df, validation_rules['duplicate_key_columns']
            )
            results['duplicate_count'] = dup_count
            results['duplicate_pct'] = dup_pct
        
        # Verificar completude
        if 'required_columns' in validation_rules:
            results['completeness'] = DataQualityValidator.check_completeness(
                df, validation_rules['required_columns']
            )
        
        return results


# Funções auxiliares para uso direto
def validate_sptrans_position_data(df: DataFrame) -> Dict[str, Any]:
    """
    Valida dados de posição da API SPTrans.
    
    Args:
        df: DataFrame com dados de posição
    
    Returns:
        Dict com resultados de validação
    """
    validation_rules = {
        'coordinates': {
            'lat_col': 'latitude',
            'lon_col': 'longitude',
            'strict': True
        },
        'timestamps': {
            'ts_col': 'timestamp'
        },
        'required_columns': ['vehicle_id', 'route_code', 'latitude', 'longitude', 'timestamp'],
        'duplicate_key_columns': ['vehicle_id', 'timestamp'],
        'null_check_columns': ['vehicle_id', 'route_code', 'latitude', 'longitude']
    }
    
    return DataQualityValidator.validate_all(df, validation_rules)


if __name__ == "__main__":
    # Testes básicos
    print("=== Testando Validadores ===")
    
    # Testar coordenadas
    print("\n1. Validando coordenadas:")
    print(f"   São Paulo Centro (-23.5505, -46.6333): {CoordinateValidator.is_valid_coordinate(-23.5505, -46.6333)}")
    print(f"   Coordenada inválida (0, 0): {CoordinateValidator.is_valid_coordinate(0, 0)}")
    
    # Testar timestamps
    print("\n2. Validando timestamps:")
    now = datetime.now()
    print(f"   Timestamp atual: {TimestampValidator.is_valid_timestamp(now)}")
    print(f"   Timestamp futuro (1 dia): {TimestampValidator.is_valid_timestamp(now + timedelta(days=1))}")
    
    # Testar vehicle IDs
    print("\n3. Validando vehicle IDs:")
    print(f"   ID válido (12345): {VehicleValidator.is_valid_vehicle_id(12345)}")
    print(f"   ID inválido (abc): {VehicleValidator.is_valid_vehicle_id('abc')}")
    
    print("\n✅ Testes concluídos!")
