"""
Data Quality Module

Responsável por validações e verificações de qualidade dos dados:
- Validação de schemas
- Regras de negócio
- Detecção de anomalias
- Quality scores
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

from src.common.logging_config import get_logger
from src.common.exceptions import DataQualityException
from src.common.metrics import MetricsCollector

logger = get_logger(__name__)


@dataclass
class QualityMetrics:
    """Métricas de qualidade de dados."""
    total_records: int
    valid_records: int
    invalid_records: int
    null_records: int
    duplicate_records: int
    out_of_range_records: int
    quality_score: float
    timestamp: datetime


class DataQualityChecker:
    """
    Classe para validação de qualidade de dados.
    
    Implementa validações em múltiplas camadas:
    - Schema validation
    - Business rules
    - Anomaly detection
    - Statistical checks
    """
    
    # Limites geográficos de São Paulo
    SP_LAT_MIN = -24.0
    SP_LAT_MAX = -23.0
    SP_LON_MIN = -47.0
    SP_LON_MAX = -46.0
    
    # Limites de velocidade (km/h)
    MIN_SPEED = 0.0
    MAX_SPEED = 120.0
    
    # Timestamp válido (últimas 24 horas)
    MAX_TIMESTAMP_DELAY_HOURS = 24
    
    def __init__(self, spark: SparkSession):
        """
        Inicializa o Data Quality Checker.
        
        Args:
            spark: SparkSession ativa
        """
        self.spark = spark
        self.metrics = MetricsCollector()
        self.logger = get_logger(self.__class__.__name__)
    
    def validate_schema(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Valida se o DataFrame possui o schema esperado.
        
        Args:
            df: DataFrame a validar
            
        Returns:
            Tuple com (is_valid, errors_list)
        """
        expected_schema = StructType([
            StructField("vehicle_id", StringType(), False),
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("speed", DoubleType(), True),
            StructField("line_id", StringType(), True),
        ])
        
        errors = []
        
        # Verificar colunas obrigatórias
        required_cols = [field.name for field in expected_schema.fields if not field.nullable]
        missing_cols = set(required_cols) - set(df.columns)
        
        if missing_cols:
            errors.append(f"Missing required columns: {missing_cols}")
        
        # Verificar tipos de dados
        for field in expected_schema.fields:
            if field.name in df.columns:
                actual_type = dict(df.dtypes)[field.name]
                expected_type = str(field.dataType)
                if actual_type != expected_type:
                    errors.append(
                        f"Column '{field.name}' has wrong type. "
                        f"Expected {expected_type}, got {actual_type}"
                    )
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Schema validation passed")
        else:
            self.logger.error(f"Schema validation failed: {errors}")
        
        return is_valid, errors
    
    def check_nulls(self, df: DataFrame) -> DataFrame:
        """
        Identifica e marca registros com valores nulos em campos obrigatórios.
        
        Args:
            df: DataFrame de entrada
            
        Returns:
            DataFrame com coluna 'has_nulls' adicionada
        """
        null_condition = (
            F.col("vehicle_id").isNull() |
            F.col("latitude").isNull() |
            F.col("longitude").isNull() |
            F.col("timestamp").isNull()
        )
        
        df_with_null_flag = df.withColumn("has_nulls", null_condition)
        
        null_count = df_with_null_flag.filter(F.col("has_nulls")).count()
        total_count = df.count()
        
        self.logger.info(f"Null check: {null_count}/{total_count} records have nulls")
        self.metrics.gauge("data_quality.null_records", null_count)
        
        return df_with_null_flag
    
    def check_geographic_bounds(self, df: DataFrame) -> DataFrame:
        """
        Valida se coordenadas estão dentro dos limites de São Paulo.
        
        Args:
            df: DataFrame de entrada
            
        Returns:
            DataFrame com coluna 'out_of_bounds' adicionada
        """
        out_of_bounds_condition = (
            (F.col("latitude") < self.SP_LAT_MIN) |
            (F.col("latitude") > self.SP_LAT_MAX) |
            (F.col("longitude") < self.SP_LON_MIN) |
            (F.col("longitude") > self.SP_LON_MAX)
        )
        
        df_with_bounds_flag = df.withColumn("out_of_bounds", out_of_bounds_condition)
        
        out_of_bounds_count = df_with_bounds_flag.filter(F.col("out_of_bounds")).count()
        total_count = df.count()
        
        self.logger.info(
            f"Geographic bounds check: {out_of_bounds_count}/{total_count} "
            f"records are out of São Paulo bounds"
        )
        self.metrics.gauge("data_quality.out_of_bounds_records", out_of_bounds_count)
        
        return df_with_bounds_flag
    
    def check_speed_limits(self, df: DataFrame) -> DataFrame:
        """
        Valida se velocidades estão dentro de limites razoáveis.
        
        Args:
            df: DataFrame de entrada
            
        Returns:
            DataFrame com coluna 'invalid_speed' adicionada
        """
        invalid_speed_condition = (
            F.col("speed").isNotNull() &
            ((F.col("speed") < self.MIN_SPEED) | (F.col("speed") > self.MAX_SPEED))
        )
        
        df_with_speed_flag = df.withColumn("invalid_speed", invalid_speed_condition)
        
        invalid_speed_count = df_with_speed_flag.filter(F.col("invalid_speed")).count()
        total_count = df.count()
        
        self.logger.info(
            f"Speed check: {invalid_speed_count}/{total_count} "
            f"records have invalid speed"
        )
        self.metrics.gauge("data_quality.invalid_speed_records", invalid_speed_count)
        
        return df_with_speed_flag
    
    def check_timestamp_freshness(self, df: DataFrame) -> DataFrame:
        """
        Valida se timestamps estão dentro de um período aceitável.
        
        Args:
            df: DataFrame de entrada
            
        Returns:
            DataFrame com coluna 'stale_timestamp' adicionada
        """
        current_time = F.current_timestamp()
        max_delay = F.expr(f"INTERVAL {self.MAX_TIMESTAMP_DELAY_HOURS} HOURS")
        
        stale_condition = (
            F.col("timestamp") < (current_time - max_delay)
        )
        
        df_with_freshness_flag = df.withColumn("stale_timestamp", stale_condition)
        
        stale_count = df_with_freshness_flag.filter(F.col("stale_timestamp")).count()
        total_count = df.count()
        
        self.logger.info(
            f"Timestamp freshness check: {stale_count}/{total_count} "
            f"records have stale timestamps"
        )
        self.metrics.gauge("data_quality.stale_records", stale_count)
        
        return df_with_freshness_flag
    
    def calculate_quality_score(self, df: DataFrame) -> float:
        """
        Calcula um score geral de qualidade (0-100).
        
        Args:
            df: DataFrame com flags de qualidade
            
        Returns:
            Quality score (0-100)
        """
        total_count = df.count()
        
        if total_count == 0:
            return 0.0
        
        # Contar registros com problemas
        invalid_count = df.filter(
            F.col("has_nulls") |
            F.col("out_of_bounds") |
            F.col("invalid_speed") |
            F.col("stale_timestamp")
        ).count()
        
        valid_count = total_count - invalid_count
        quality_score = (valid_count / total_count) * 100
        
        self.logger.info(
            f"Quality score: {quality_score:.2f}% "
            f"({valid_count}/{total_count} valid records)"
        )
        self.metrics.gauge("data_quality.score", quality_score)
        
        return quality_score
    
    def run_all_checks(self, df: DataFrame) -> Tuple[DataFrame, QualityMetrics]:
        """
        Executa todas as validações de qualidade.
        
        Args:
            df: DataFrame de entrada
            
        Returns:
            Tuple com (DataFrame validado, QualityMetrics)
        """
        self.logger.info("Starting data quality checks")
        
        # Validar schema
        is_valid, errors = self.validate_schema(df)
        if not is_valid:
            raise DataQualityException(f"Schema validation failed: {errors}")
        
        # Executar checks
        df_checked = df
        df_checked = self.check_nulls(df_checked)
        df_checked = self.check_geographic_bounds(df_checked)
        df_checked = self.check_speed_limits(df_checked)
        df_checked = self.check_timestamp_freshness(df_checked)
        
        # Adicionar coluna de qualidade geral
        df_checked = df_checked.withColumn(
            "is_valid",
            ~(
                F.col("has_nulls") |
                F.col("out_of_bounds") |
                F.col("invalid_speed") |
                F.col("stale_timestamp")
            )
        )
        
        # Calcular métricas
        total_records = df.count()
        valid_records = df_checked.filter(F.col("is_valid")).count()
        invalid_records = total_records - valid_records
        null_records = df_checked.filter(F.col("has_nulls")).count()
        
        quality_score = self.calculate_quality_score(df_checked)
        
        metrics = QualityMetrics(
            total_records=total_records,
            valid_records=valid_records,
            invalid_records=invalid_records,
            null_records=null_records,
            duplicate_records=0,  # Será calculado no deduplicator
            out_of_range_records=df_checked.filter(F.col("out_of_bounds")).count(),
            quality_score=quality_score,
            timestamp=datetime.now()
        )
        
        self.logger.info(f"Data quality checks completed: {metrics}")
        
        return df_checked, metrics
    
    def filter_valid_records(self, df: DataFrame) -> DataFrame:
        """
        Filtra apenas registros válidos.
        
        Args:
            df: DataFrame com flags de qualidade
            
        Returns:
            DataFrame apenas com registros válidos
        """
        df_valid = df.filter(F.col("is_valid"))
        
        valid_count = df_valid.count()
        total_count = df.count()
        
        self.logger.info(
            f"Filtered valid records: {valid_count}/{total_count} "
            f"({(valid_count/total_count)*100:.2f}%)"
        )
        
        return df_valid
    
    def quarantine_invalid_records(
        self,
        df: DataFrame,
        quarantine_path: str
    ) -> None:
        """
        Move registros inválidos para área de quarentena.
        
        Args:
            df: DataFrame com flags de qualidade
            quarantine_path: Caminho para salvar registros inválidos
        """
        df_invalid = df.filter(~F.col("is_valid"))
        
        invalid_count = df_invalid.count()
        
        if invalid_count > 0:
            self.logger.warning(
                f"Moving {invalid_count} invalid records to quarantine: {quarantine_path}"
            )
            
            df_invalid.write.mode("append").parquet(quarantine_path)
            
            self.metrics.counter("data_quality.quarantined_records", invalid_count)
        else:
            self.logger.info("No invalid records to quarantine")


def validate_vehicle_position(
    latitude: float,
    longitude: float,
    speed: Optional[float] = None
) -> bool:
    """
    Função utilitária para validar uma posição de veículo.
    
    Args:
        latitude: Latitude
        longitude: Longitude
        speed: Velocidade (opcional)
        
    Returns:
        True se válido, False caso contrário
    """
    # Verificar bounds geográficos
    if not (DataQualityChecker.SP_LAT_MIN <= latitude <= DataQualityChecker.SP_LAT_MAX):
        return False
    
    if not (DataQualityChecker.SP_LON_MIN <= longitude <= DataQualityChecker.SP_LON_MAX):
        return False
    
    # Verificar velocidade se fornecida
    if speed is not None:
        if not (DataQualityChecker.MIN_SPEED <= speed <= DataQualityChecker.MAX_SPEED):
            return False
    
    return True


# Alias para compatibilidade
DataQualityValidator = DataQualityChecker