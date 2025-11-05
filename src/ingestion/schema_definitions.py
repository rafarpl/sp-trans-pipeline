"""
Definições de Schemas Spark para SPTrans Pipeline.

Define schemas para validação de dados em cada camada
do pipeline (Bronze, Silver, Gold) e dados GTFS.
"""

from typing import Dict, List, Optional

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ..common.logging_config import get_logger

logger = get_logger(__name__)


# =============================================================================
# Bronze Layer Schemas (Raw Data)
# =============================================================================


def get_bronze_positions_schema() -> StructType:
    """
    Schema para posições de veículos na camada Bronze (dados brutos da API).

    Returns:
        StructType do schema
    """
    return StructType(
        [
            # Metadados da API
            StructField("hr", StringType(), True),  # Horário da requisição
            # Lista de linhas
            StructField(
                "l",
                ArrayType(
                    StructType(
                        [
                            StructField("cl", IntegerType(), True),  # Código linha
                            StructField("sl", IntegerType(), True),  # Sentido linha
                            StructField("lt0", StringType(), True),  # Letreiro completo
                            StructField("lt1", StringType(), True),  # Letreiro destino
                            StructField("qv", IntegerType(), True),  # Quantidade veículos
                            # Lista de veículos
                            StructField(
                                "vs",
                                ArrayType(
                                    StructType(
                                        [
                                            StructField("p", IntegerType(), True),  # Prefixo
                                            StructField("a", BooleanType(), True),  # Acessibilidade
                                            StructField("ta", StringType(), True),  # Timestamp atualização
                                            StructField("py", DoubleType(), True),  # Latitude
                                            StructField("px", DoubleType(), True),  # Longitude
                                        ]
                                    )
                                ),
                                True,
                            ),
                        ]
                    )
                ),
                True,
            ),
            # Metadados de ingestão
            StructField("ingestion_timestamp", TimestampType(), False),
            StructField("ingestion_date", StringType(), False),
            StructField("year", IntegerType(), False),
            StructField("month", IntegerType(), False),
            StructField("day", IntegerType(), False),
            StructField("hour", IntegerType(), False),
        ]
    )


def get_bronze_api_raw_schema() -> StructType:
    """
    Schema genérico para dados brutos da API (JSON completo).

    Returns:
        StructType do schema
    """
    return StructType(
        [
            StructField("raw_json", StringType(), False),  # JSON completo como string
            StructField("endpoint", StringType(), False),  # Endpoint da API
            StructField("request_timestamp", TimestampType(), False),
            StructField("response_status", IntegerType(), True),
            StructField("year", IntegerType(), False),
            StructField("month", IntegerType(), False),
            StructField("day", IntegerType(), False),
            StructField("hour", IntegerType(), False),
        ]
    )


# =============================================================================
# Silver Layer Schemas (Cleaned & Normalized)
# =============================================================================


def get_silver_vehicle_positions_schema() -> StructType:
    """
    Schema para posições de veículos na camada Silver (dados limpos e normalizados).

    Returns:
        StructType do schema
    """
    return StructType(
        [
            # Identificadores
            StructField("vehicle_id", StringType(), False),  # Prefixo do veículo
            StructField("line_id", IntegerType(), False),  # Código da linha
            StructField("line_direction", IntegerType(), True),  # Sentido (0/1)
            # Informações da linha
            StructField("line_name", StringType(), True),  # Nome/letreiro da linha
            StructField("line_destination", StringType(), True),  # Destino
            # Posição
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
            # Características do veículo
            StructField("accessible", BooleanType(), True),  # Acessibilidade
            # Timestamps
            StructField("position_timestamp", TimestampType(), False),  # Timestamp da posição
            StructField("processed_timestamp", TimestampType(), False),  # Timestamp processamento
            # Particionamento
            StructField("year", IntegerType(), False),
            StructField("month", IntegerType(), False),
            StructField("day", IntegerType(), False),
            # Metadados de qualidade
            StructField("data_quality_score", DoubleType(), True),  # Score 0-1
            StructField("is_duplicate", BooleanType(), False),  # Flag de duplicata
        ]
    )


def get_silver_vehicle_telemetry_schema() -> StructType:
    """
    Schema para telemetria de veículos (velocidade, aceleração, etc).

    Returns:
        StructType do schema
    """
    return StructType(
        [
            # Identificadores
            StructField("vehicle_id", StringType(), False),
            StructField("line_id", IntegerType(), False),
            # Telemetria
            StructField("speed_kmh", DoubleType(), True),  # Velocidade em km/h
            StructField("heading", DoubleType(), True),  # Direção em graus
            StructField("acceleration", DoubleType(), True),  # Aceleração m/s²
            # Distância percorrida
            StructField("distance_from_previous_m", DoubleType(), True),  # Metros
            StructField("time_from_previous_s", DoubleType(), True),  # Segundos
            # Timestamps
            StructField("telemetry_timestamp", TimestampType(), False),
            # Particionamento
            StructField("year", IntegerType(), False),
            StructField("month", IntegerType(), False),
            StructField("day", IntegerType(), False),
        ]
    )


# =============================================================================
# Gold Layer Schemas (Aggregated & Business Logic)
# =============================================================================


def get_gold_hourly_metrics_schema() -> StructType:
    """
    Schema para métricas horárias agregadas.

    Returns:
        StructType do schema
    """
    return StructType(
        [
            # Dimensões
            StructField("line_id", IntegerType(), False),
            StructField("line_name", StringType(), True),
            StructField("hour_timestamp", TimestampType(), False),
            # Métricas de frota
            StructField("vehicles_active", IntegerType(), True),  # Veículos ativos
            StructField("vehicles_accessible", IntegerType(), True),  # Veículos acessíveis
            # Métricas de performance
            StructField("avg_speed_kmh", DoubleType(), True),  # Velocidade média
            StructField("min_speed_kmh", DoubleType(), True),
            StructField("max_speed_kmh", DoubleType(), True),
            StructField("std_speed_kmh", DoubleType(), True),  # Desvio padrão
            # Métricas de distância
            StructField("total_distance_km", DoubleType(), True),  # Distância total
            StructField("avg_distance_per_vehicle_km", DoubleType(), True),
            # Métricas de qualidade
            StructField("data_quality_score", DoubleType(), True),
            StructField("total_records", LongType(), True),
            StructField("valid_records", LongType(), True),
            # Particionamento
            StructField("year", IntegerType(), False),
            StructField("month", IntegerType(), False),
        ]
    )


def get_gold_daily_summary_schema() -> StructType:
    """
    Schema para sumarização diária.

    Returns:
        StructType do schema
    """
    return StructType(
        [
            # Dimensões
            StructField("date", StringType(), False),  # YYYY-MM-DD
            # Métricas gerais
            StructField("total_vehicles", IntegerType(), True),
            StructField("total_lines", IntegerType(), True),
            StructField("total_trips", LongType(), True),
            # Performance
            StructField("avg_speed_kmh", DoubleType(), True),
            StructField("total_distance_km", DoubleType(), True),
            # Operação
            StructField("peak_hour_vehicles", IntegerType(), True),
            StructField("peak_hour", IntegerType(), True),
            StructField("off_peak_avg_vehicles", DoubleType(), True),
            # Qualidade
            StructField("data_quality_score", DoubleType(), True),
            StructField("uptime_percentage", DoubleType(), True),
            # Particionamento
            StructField("year", IntegerType(), False),
            StructField("month", IntegerType(), False),
        ]
    )


def get_gold_line_performance_schema() -> StructType:
    """
    Schema para performance por linha.

    Returns:
        StructType do schema
    """
    return StructType(
        [
            # Dimensões
            StructField("line_id", IntegerType(), False),
            StructField("line_name", StringType(), True),
            StructField("analysis_period", StringType(), False),  # hourly/daily/weekly
            StructField("period_start", TimestampType(), False),
            StructField("period_end", TimestampType(), False),
            # KPIs operacionais
            StructField("avg_headway_minutes", DoubleType(), True),  # Intervalo entre ônibus
            StructField("std_headway_minutes", DoubleType(), True),
            StructField("reliability_score", DoubleType(), True),  # 0-100
            # KPIs de velocidade
            StructField("avg_speed_kmh", DoubleType(), True),
            StructField("congestion_index", DoubleType(), True),  # 0-100 (100=muito congestionado)
            # KPIs de frota
            StructField("avg_vehicles_operating", DoubleType(), True),
            StructField("utilization_rate", DoubleType(), True),  # 0-1
            # Particionamento
            StructField("year", IntegerType(), False),
            StructField("month", IntegerType(), False),
        ]
    )


# =============================================================================
# GTFS Schemas
# =============================================================================


def get_gtfs_stops_schema() -> StructType:
    """
    Schema para stops.txt (GTFS).

    Returns:
        StructType do schema
    """
    return StructType(
        [
            StructField("stop_id", StringType(), False),
            StructField("stop_code", StringType(), True),
            StructField("stop_name", StringType(), False),
            StructField("stop_desc", StringType(), True),
            StructField("stop_lat", DoubleType(), False),
            StructField("stop_lon", DoubleType(), False),
            StructField("zone_id", StringType(), True),
            StructField("stop_url", StringType(), True),
            StructField("location_type", IntegerType(), True),
            StructField("parent_station", StringType(), True),
            StructField("stop_timezone", StringType(), True),
            StructField("wheelchair_boarding", IntegerType(), True),
        ]
    )


def get_gtfs_routes_schema() -> StructType:
    """
    Schema para routes.txt (GTFS).

    Returns:
        StructType do schema
    """
    return StructType(
        [
            StructField("route_id", StringType(), False),
            StructField("agency_id", StringType(), True),
            StructField("route_short_name", StringType(), True),
            StructField("route_long_name", StringType(), False),
            StructField("route_desc", StringType(), True),
            StructField("route_type", IntegerType(), False),  # 3 = Bus
            StructField("route_url", StringType(), True),
            StructField("route_color", StringType(), True),
            StructField("route_text_color", StringType(), True),
        ]
    )


def get_gtfs_trips_schema() -> StructType:
    """
    Schema para trips.txt (GTFS).

    Returns:
        StructType do schema
    """
    return StructType(
        [
            StructField("route_id", StringType(), False),
            StructField("service_id", StringType(), False),
            StructField("trip_id", StringType(), False),
            StructField("trip_headsign", StringType(), True),
            StructField("trip_short_name", StringType(), True),
            StructField("direction_id", IntegerType(), True),
            StructField("block_id", StringType(), True),
            StructField("shape_id", StringType(), True),
            StructField("wheelchair_accessible", IntegerType(), True),
            StructField("bikes_allowed", IntegerType(), True),
        ]
    )


def get_gtfs_stop_times_schema() -> StructType:
    """
    Schema para stop_times.txt (GTFS).

    Returns:
        StructType do schema
    """
    return StructType(
        [
            StructField("trip_id", StringType(), False),
            StructField("arrival_time", StringType(), False),
            StructField("departure_time", StringType(), False),
            StructField("stop_id", StringType(), False),
            StructField("stop_sequence", IntegerType(), False),
            StructField("stop_headsign", StringType(), True),
            StructField("pickup_type", IntegerType(), True),
            StructField("drop_off_type", IntegerType(), True),
            StructField("shape_dist_traveled", DoubleType(), True),
        ]
    )


def get_gtfs_shapes_schema() -> StructType:
    """
    Schema para shapes.txt (GTFS).

    Returns:
        StructType do schema
    """
    return StructType(
        [
            StructField("shape_id", StringType(), False),
            StructField("shape_pt_lat", DoubleType(), False),
            StructField("shape_pt_lon", DoubleType(), False),
            StructField("shape_pt_sequence", IntegerType(), False),
            StructField("shape_dist_traveled", DoubleType(), True),
        ]
    )


# =============================================================================
# Schema Helpers
# =============================================================================


def get_bronze_schema(data_type: str = "positions") -> StructType:
    """
    Retorna schema da camada Bronze.

    Args:
        data_type: Tipo de dado ('positions', 'api_raw')

    Returns:
        StructType do schema
    """
    schemas = {
        "positions": get_bronze_positions_schema(),
        "api_raw": get_bronze_api_raw_schema(),
    }

    if data_type not in schemas:
        raise ValueError(f"Unknown bronze data type: {data_type}")

    return schemas[data_type]


def get_silver_schema(data_type: str = "vehicle_positions") -> StructType:
    """
    Retorna schema da camada Silver.

    Args:
        data_type: Tipo de dado ('vehicle_positions', 'vehicle_telemetry')

    Returns:
        StructType do schema
    """
    schemas = {
        "vehicle_positions": get_silver_vehicle_positions_schema(),
        "vehicle_telemetry": get_silver_vehicle_telemetry_schema(),
    }

    if data_type not in schemas:
        raise ValueError(f"Unknown silver data type: {data_type}")

    return schemas[data_type]


def get_gold_schema(data_type: str = "hourly_metrics") -> StructType:
    """
    Retorna schema da camada Gold.

    Args:
        data_type: Tipo de dado ('hourly_metrics', 'daily_summary', 'line_performance')

    Returns:
        StructType do schema
    """
    schemas = {
        "hourly_metrics": get_gold_hourly_metrics_schema(),
        "daily_summary": get_gold_daily_summary_schema(),
        "line_performance": get_gold_line_performance_schema(),
    }

    if data_type not in schemas:
        raise ValueError(f"Unknown gold data type: {data_type}")

    return schemas[data_type]


def get_gtfs_schema(file_type: str) -> StructType:
    """
    Retorna schema GTFS baseado no tipo de arquivo.

    Args:
        file_type: Tipo de arquivo ('stops', 'routes', 'trips', 'stop_times', 'shapes')

    Returns:
        StructType do schema
    """
    schemas = {
        "stops": get_gtfs_stops_schema(),
        "routes": get_gtfs_routes_schema(),
        "trips": get_gtfs_trips_schema(),
        "stop_times": get_gtfs_stop_times_schema(),
        "shapes": get_gtfs_shapes_schema(),
    }

    if file_type not in schemas:
        raise ValueError(f"Unknown GTFS file type: {file_type}")

    return schemas[file_type]


def validate_schema(df, expected_schema: StructType, stage: str) -> bool:
    """
    Valida se DataFrame tem schema esperado.

    Args:
        df: DataFrame Spark
        expected_schema: Schema esperado
        stage: Estágio do pipeline (para logging)

    Returns:
        True se schema é válido

    Raises:
        ValueError: Se schema não corresponder
    """
    actual_schema = df.schema
    expected_fields = {field.name: field.dataType for field in expected_schema.fields}
    actual_fields = {field.name: field.dataType for field in actual_schema.fields}

    # Verificar campos faltantes
    missing_fields = set(expected_fields.keys()) - set(actual_fields.keys())
    if missing_fields:
        logger.error(
            f"Schema validation failed in {stage}",
            extra={"missing_fields": list(missing_fields)},
        )
        raise ValueError(f"Missing fields in {stage}: {missing_fields}")

    # Verificar tipos incompatíveis
    type_mismatches = []
    for field_name in expected_fields:
        if field_name in actual_fields:
            if expected_fields[field_name] != actual_fields[field_name]:
                type_mismatches.append(
                    f"{field_name}: expected {expected_fields[field_name]}, "
                    f"got {actual_fields[field_name]}"
                )

    if type_mismatches:
        logger.error(
            f"Schema validation failed in {stage}",
            extra={"type_mismatches": type_mismatches},
        )
        raise ValueError(f"Type mismatches in {stage}: {type_mismatches}")

    logger.info(f"Schema validation passed in {stage}")
    return True


# Exemplo de uso
if __name__ == "__main__":
    # Imprimir schemas
    print("Bronze Positions Schema:")
    print(get_bronze_schema("positions").simpleString())

    print("\nSilver Vehicle Positions Schema:")
    print(get_silver_schema("vehicle_positions").simpleString())

    print("\nGold Hourly Metrics Schema:")
    print(get_gold_schema("hourly_metrics").simpleString())

    print("\nGTFS Stops Schema:")
    print(get_gtfs_schema("stops").simpleString())
