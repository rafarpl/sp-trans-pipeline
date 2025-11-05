"""
Validadores de Dados para SPTrans Pipeline.

Fornece funções de validação para garantir qualidade e integridade
dos dados em diferentes estágios do pipeline.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .constants import (
    DQ_LATITUDE_RANGE,
    DQ_LONGITUDE_RANGE,
    DQ_MAX_DUPLICATE_RATE,
    DQ_MAX_NULL_RATE,
    DQ_MIN_FRESHNESS_MINUTES,
    DQ_MIN_SUCCESS_RATE,
    DQ_REQUIRED_FIELDS_POSITION,
    SPEED_MAX_REASONABLE,
    SPEED_MIN_OPERATIONAL,
)
from .exceptions import (
    DataFreshnessException,
    DataValidationException,
    DuplicateRecordsException,
    SchemaValidationException,
)
from .logging_config import get_logger

logger = get_logger(__name__)


# =============================================================================
# Schema Validators
# =============================================================================


def validate_schema(
    df: DataFrame, required_columns: List[str], stage: str = "unknown"
) -> Tuple[bool, List[str]]:
    """
    Valida se o DataFrame contém todas as colunas obrigatórias.

    Args:
        df: DataFrame Spark
        required_columns: Lista de colunas obrigatórias
        stage: Estágio do pipeline (para logging)

    Returns:
        Tuple (is_valid, missing_columns)

    Raises:
        SchemaValidationException: Se colunas obrigatórias estiverem faltando
    """
    actual_columns = set(df.columns)
    required_set = set(required_columns)
    missing_columns = list(required_set - actual_columns)

    if missing_columns:
        logger.error(
            f"Schema validation failed in {stage}",
            extra={
                "missing_columns": missing_columns,
                "required_columns": required_columns,
                "actual_columns": list(actual_columns),
            },
        )
        raise SchemaValidationException(
            expected_schema=str(required_columns),
            actual_schema=str(list(actual_columns)),
        )

    logger.info(
        f"Schema validation passed in {stage}",
        extra={"columns_count": len(actual_columns)},
    )
    return True, []


def validate_data_types(
    df: DataFrame, expected_types: Dict[str, str], stage: str = "unknown"
) -> Tuple[bool, List[str]]:
    """
    Valida se as colunas têm os tipos de dados esperados.

    Args:
        df: DataFrame Spark
        expected_types: Dicionário {coluna: tipo_esperado}
        stage: Estágio do pipeline

    Returns:
        Tuple (is_valid, type_mismatches)
    """
    type_mismatches = []

    for col_name, expected_type in expected_types.items():
        if col_name not in df.columns:
            continue

        actual_type = dict(df.dtypes)[col_name]
        if actual_type != expected_type:
            type_mismatches.append(f"{col_name}: expected {expected_type}, got {actual_type}")

    if type_mismatches:
        logger.warning(
            f"Data type mismatches in {stage}",
            extra={"mismatches": type_mismatches},
        )
        return False, type_mismatches

    logger.info(f"Data type validation passed in {stage}")
    return True, []


# =============================================================================
# Null Validators
# =============================================================================


def validate_null_rate(
    df: DataFrame,
    columns: Optional[List[str]] = None,
    max_null_rate: float = DQ_MAX_NULL_RATE,
    stage: str = "unknown",
) -> Dict[str, float]:
    """
    Valida taxa de nulos em colunas específicas.

    Args:
        df: DataFrame Spark
        columns: Colunas para validar (None = todas)
        max_null_rate: Taxa máxima aceitável de nulos
        stage: Estágio do pipeline

    Returns:
        Dicionário {coluna: null_rate}

    Raises:
        DataValidationException: Se taxa de nulos exceder threshold
    """
    if columns is None:
        columns = df.columns

    total_count = df.count()
    null_rates = {}
    failed_columns = []

    for col in columns:
        null_count = df.filter(F.col(col).isNull()).count()
        null_rate = null_count / total_count if total_count > 0 else 0
        null_rates[col] = null_rate

        if null_rate > max_null_rate:
            failed_columns.append(f"{col} ({null_rate:.2%})")

    if failed_columns:
        logger.error(
            f"Null rate validation failed in {stage}",
            extra={
                "failed_columns": failed_columns,
                "threshold": max_null_rate,
            },
        )
        raise DataValidationException(
            validation_rule="null_rate",
            failed_records=len(failed_columns),
            total_records=len(columns),
        )

    logger.info(
        f"Null rate validation passed in {stage}",
        extra={"max_null_rate": max(null_rates.values()) if null_rates else 0},
    )
    return null_rates


def validate_required_fields(
    df: DataFrame, required_fields: List[str] = DQ_REQUIRED_FIELDS_POSITION, stage: str = "unknown"
) -> int:
    """
    Valida se campos obrigatórios não são nulos.

    Args:
        df: DataFrame Spark
        required_fields: Lista de campos obrigatórios
        stage: Estágio do pipeline

    Returns:
        Número de registros válidos

    Raises:
        DataValidationException: Se muitos registros tiverem campos nulos
    """
    total_count = df.count()

    # Cria condição para verificar nulos em campos obrigatórios
    null_condition = F.col(required_fields[0]).isNull()
    for field in required_fields[1:]:
        null_condition = null_condition | F.col(field).isNull()

    invalid_count = df.filter(null_condition).count()
    valid_count = total_count - invalid_count
    valid_rate = valid_count / total_count if total_count > 0 else 0

    if valid_rate < DQ_MIN_SUCCESS_RATE:
        logger.error(
            f"Required fields validation failed in {stage}",
            extra={
                "valid_rate": valid_rate,
                "invalid_count": invalid_count,
                "total_count": total_count,
                "threshold": DQ_MIN_SUCCESS_RATE,
            },
        )
        raise DataValidationException(
            validation_rule="required_fields",
            failed_records=invalid_count,
            total_records=total_count,
        )

    logger.info(
        f"Required fields validation passed in {stage}",
        extra={"valid_count": valid_count, "valid_rate": valid_rate},
    )
    return valid_count


# =============================================================================
# Range Validators
# =============================================================================


def validate_coordinate_range(
    df: DataFrame,
    lat_col: str = "latitude",
    lon_col: str = "longitude",
    lat_range: Tuple[float, float] = DQ_LATITUDE_RANGE,
    lon_range: Tuple[float, float] = DQ_LONGITUDE_RANGE,
    stage: str = "unknown",
) -> int:
    """
    Valida se coordenadas estão dentro do range esperado (São Paulo).

    Args:
        df: DataFrame Spark
        lat_col: Nome da coluna de latitude
        lon_col: Nome da coluna de longitude
        lat_range: Range válido de latitude (min, max)
        lon_range: Range válido de longitude (min, max)
        stage: Estágio do pipeline

    Returns:
        Número de registros com coordenadas válidas

    Raises:
        DataValidationException: Se muitos registros tiverem coordenadas inválidas
    """
    total_count = df.count()

    # Filtra coordenadas válidas
    valid_df = df.filter(
        (F.col(lat_col).between(lat_range[0], lat_range[1]))
        & (F.col(lon_col).between(lon_range[0], lon_range[1]))
    )

    valid_count = valid_df.count()
    invalid_count = total_count - valid_count
    valid_rate = valid_count / total_count if total_count > 0 else 0

    if valid_rate < DQ_MIN_SUCCESS_RATE:
        logger.error(
            f"Coordinate range validation failed in {stage}",
            extra={
                "valid_rate": valid_rate,
                "invalid_count": invalid_count,
                "total_count": total_count,
                "lat_range": lat_range,
                "lon_range": lon_range,
            },
        )
        raise DataValidationException(
            validation_rule="coordinate_range",
            failed_records=invalid_count,
            total_records=total_count,
        )

    logger.info(
        f"Coordinate range validation passed in {stage}",
        extra={"valid_count": valid_count, "valid_rate": valid_rate},
    )
    return valid_count


def validate_speed_range(
    df: DataFrame,
    speed_col: str = "speed",
    min_speed: float = SPEED_MIN_OPERATIONAL,
    max_speed: float = SPEED_MAX_REASONABLE,
    stage: str = "unknown",
) -> int:
    """
    Valida se velocidade está dentro do range razoável.

    Args:
        df: DataFrame Spark
        speed_col: Nome da coluna de velocidade
        min_speed: Velocidade mínima operacional (km/h)
        max_speed: Velocidade máxima razoável (km/h)
        stage: Estágio do pipeline

    Returns:
        Número de registros com velocidade válida
    """
    total_count = df.count()

    valid_count = df.filter(
        (F.col(speed_col) >= min_speed) & (F.col(speed_col) <= max_speed)
    ).count()

    invalid_count = total_count - valid_count
    valid_rate = valid_count / total_count if total_count > 0 else 0

    if valid_rate < DQ_MIN_SUCCESS_RATE:
        logger.warning(
            f"Speed range validation warning in {stage}",
            extra={
                "valid_rate": valid_rate,
                "invalid_count": invalid_count,
                "speed_range": (min_speed, max_speed),
            },
        )

    logger.info(
        f"Speed range validation completed in {stage}",
        extra={"valid_count": valid_count, "valid_rate": valid_rate},
    )
    return valid_count


# =============================================================================
# Duplicate Validators
# =============================================================================


def validate_duplicates(
    df: DataFrame,
    key_columns: List[str],
    max_duplicate_rate: float = DQ_MAX_DUPLICATE_RATE,
    stage: str = "unknown",
) -> Tuple[int, int]:
    """
    Valida taxa de registros duplicados.

    Args:
        df: DataFrame Spark
        key_columns: Colunas que definem duplicatas
        max_duplicate_rate: Taxa máxima aceitável de duplicados
        stage: Estágio do pipeline

    Returns:
        Tuple (unique_count, duplicate_count)

    Raises:
        DuplicateRecordsException: Se taxa de duplicados exceder threshold
    """
    total_count = df.count()
    unique_count = df.dropDuplicates(key_columns).count()
    duplicate_count = total_count - unique_count
    duplicate_rate = duplicate_count / total_count if total_count > 0 else 0

    if duplicate_rate > max_duplicate_rate:
        logger.error(
            f"Duplicate validation failed in {stage}",
            extra={
                "duplicate_rate": duplicate_rate,
                "duplicate_count": duplicate_count,
                "total_count": total_count,
                "threshold": max_duplicate_rate,
                "key_columns": key_columns,
            },
        )
        raise DuplicateRecordsException(
            duplicate_count=duplicate_count,
            total_count=total_count,
            threshold=max_duplicate_rate,
        )

    logger.info(
        f"Duplicate validation passed in {stage}",
        extra={
            "unique_count": unique_count,
            "duplicate_count": duplicate_count,
            "duplicate_rate": duplicate_rate,
        },
    )
    return unique_count, duplicate_count


# =============================================================================
# Freshness Validators
# =============================================================================


def validate_data_freshness(
    df: DataFrame,
    timestamp_col: str = "timestamp",
    max_delay_minutes: int = DQ_MIN_FRESHNESS_MINUTES,
    stage: str = "unknown",
) -> datetime:
    """
    Valida se os dados estão atualizados (freshness).

    Args:
        df: DataFrame Spark
        timestamp_col: Coluna de timestamp
        max_delay_minutes: Delay máximo aceitável em minutos
        stage: Estágio do pipeline

    Returns:
        Timestamp mais recente nos dados

    Raises:
        DataFreshnessException: Se dados estiverem desatualizados
    """
    max_timestamp = df.agg(F.max(timestamp_col)).collect()[0][0]

    if max_timestamp is None:
        logger.error(f"No timestamp found in {stage}")
        raise DataFreshnessException(
            last_update_minutes=999999, threshold_minutes=max_delay_minutes
        )

    current_time = datetime.now()
    delay = current_time - max_timestamp
    delay_minutes = int(delay.total_seconds() / 60)

    if delay_minutes > max_delay_minutes:
        logger.error(
            f"Data freshness validation failed in {stage}",
            extra={
                "delay_minutes": delay_minutes,
                "max_timestamp": max_timestamp.isoformat(),
                "current_time": current_time.isoformat(),
                "threshold_minutes": max_delay_minutes,
            },
        )
        raise DataFreshnessException(
            last_update_minutes=delay_minutes, threshold_minutes=max_delay_minutes
        )

    logger.info(
        f"Data freshness validation passed in {stage}",
        extra={
            "delay_minutes": delay_minutes,
            "max_timestamp": max_timestamp.isoformat(),
        },
    )
    return max_timestamp


# =============================================================================
# Completeness Validators
# =============================================================================


def validate_record_count(
    df: DataFrame,
    min_records: int,
    max_records: Optional[int] = None,
    stage: str = "unknown",
) -> int:
    """
    Valida se o número de registros está dentro do esperado.

    Args:
        df: DataFrame Spark
        min_records: Número mínimo de registros esperado
        max_records: Número máximo de registros esperado (opcional)
        stage: Estágio do pipeline

    Returns:
        Número de registros

    Raises:
        DataValidationException: Se contagem estiver fora do range
    """
    count = df.count()

    if count < min_records:
        logger.error(
            f"Record count validation failed in {stage} - too few records",
            extra={
                "actual_count": count,
                "min_records": min_records,
            },
        )
        raise DataValidationException(
            validation_rule="min_record_count",
            failed_records=min_records - count,
            total_records=min_records,
        )

    if max_records and count > max_records:
        logger.error(
            f"Record count validation failed in {stage} - too many records",
            extra={
                "actual_count": count,
                "max_records": max_records,
            },
        )
        raise DataValidationException(
            validation_rule="max_record_count",
            failed_records=count - max_records,
            total_records=max_records,
        )

    logger.info(
        f"Record count validation passed in {stage}",
        extra={
            "count": count,
            "min_records": min_records,
            "max_records": max_records,
        },
    )
    return count


# =============================================================================
# Composite Validators
# =============================================================================


def run_all_validations(
    df: DataFrame,
    stage: str,
    required_columns: List[str] = DQ_REQUIRED_FIELDS_POSITION,
    key_columns: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Executa todas as validações em sequência.

    Args:
        df: DataFrame Spark
        stage: Estágio do pipeline
        required_columns: Colunas obrigatórias
        key_columns: Colunas para detecção de duplicatas

    Returns:
        Dicionário com resultados de todas as validações
    """
    results = {
        "stage": stage,
        "timestamp": datetime.now().isoformat(),
        "validations": {},
    }

    try:
        # Schema
        validate_schema(df, required_columns, stage)
        results["validations"]["schema"] = "passed"

        # Required fields
        valid_count = validate_required_fields(df, required_columns, stage)
        results["validations"]["required_fields"] = {
            "status": "passed",
            "valid_count": valid_count,
        }

        # Coordinates
        coord_valid = validate_coordinate_range(df, stage=stage)
        results["validations"]["coordinates"] = {
            "status": "passed",
            "valid_count": coord_valid,
        }

        # Duplicates (se key_columns fornecido)
        if key_columns:
            unique, dupes = validate_duplicates(df, key_columns, stage=stage)
            results["validations"]["duplicates"] = {
                "status": "passed",
                "unique_count": unique,
                "duplicate_count": dupes,
            }

        # Freshness
        max_ts = validate_data_freshness(df, stage=stage)
        results["validations"]["freshness"] = {
            "status": "passed",
            "max_timestamp": max_ts.isoformat(),
        }

        results["overall_status"] = "passed"
        logger.info(f"All validations passed in {stage}")

    except Exception as e:
        results["overall_status"] = "failed"
        results["error"] = str(e)
        logger.error(f"Validation failed in {stage}: {e}")
        raise

    return results


# Exemplo de uso
if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("ValidatorsTest").getOrCreate()

    # Criar DataFrame de teste
    data = [
        (1, "2024-01-01 10:00:00", -23.5505, -46.6333, 20.0),
        (2, "2024-01-01 10:01:00", -23.5600, -46.6400, 25.0),
    ]
    df = spark.createDataFrame(
        data, ["vehicle_id", "timestamp", "latitude", "longitude", "speed"]
    )

    # Executar validações
    results = run_all_validations(
        df,
        stage="bronze",
        required_columns=["vehicle_id", "timestamp", "latitude", "longitude"],
        key_columns=["vehicle_id", "timestamp"],
    )

    print(results)
