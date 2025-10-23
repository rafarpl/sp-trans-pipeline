"""
Spark Job: Ingestão da API SPTrans para Bronze Layer.

Responsabilidades:
- Conectar na API SPTrans
- Coletar posições de todos os veículos
- Transformar JSON em DataFrame Spark
- Salvar no MinIO (Bronze Layer) como Parquet

Execução:
    spark-submit --master spark://spark-master:7077 \
        src/processing/jobs/ingest_api_to_bronze.py \
        --execution-date 2025-10-20T14:30:00
"""
import sys
import argparse
from datetime import datetime
from typing import List, Dict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, explode, to_timestamp
)

# Adicionar src ao path
sys.path.insert(0, '/opt/airflow/src')

from src.common.config import get_config, get_spark_config
from src.common.constants import DataLakePath, PROJECT_VERSION
from src.common.logging_config import get_logger
from src.common.utils import get_partition_path
from src.ingestion.sptrans_api_client import SPTransAPIClient
from src.ingestion.schema_definitions import get_bronze_api_positions_schema


logger = get_logger(__name__, job="ingest_api_to_bronze")


def create_spark_session() -> SparkSession:
    """
    Cria e configura SparkSession.
    
    Returns:
        SparkSession configurada
    """
    logger.info("creating_spark_session")
    
    spark_config = get_spark_config()
    
    spark = SparkSession.builder \
        .appName("SPTrans - API to Bronze") \
        .config("spark.master", spark_config.get('spark.master')) \
        .config("spark.sql.extensions", spark_config.get('spark.sql.extensions')) \
        .config("spark.sql.catalog.spark_catalog", spark_config.get('spark.sql.catalog.spark_catalog')) \
        .config("spark.hadoop.fs.s3a.endpoint", spark_config.get('spark.hadoop.fs.s3a.endpoint')) \
        .config("spark.hadoop.fs.s3a.access.key", spark_config.get('spark.hadoop.fs.s3a.access.key')) \
        .config("spark.hadoop.fs.s3a.secret.key", spark_config.get('spark.hadoop.fs.s3a.secret.key')) \
        .config("spark.hadoop.fs.s3a.path.style.access", spark_config.get('spark.hadoop.fs.s3a.path.style.access')) \
        .config("spark.hadoop.fs.s3a.impl", spark_config.get('spark.hadoop.fs.s3a.impl')) \
        .getOrCreate()
    
    logger.info(
        "spark_session_created",
        app_name=spark.sparkContext.appName,
        spark_version=spark.version
    )
    
    return spark


def fetch_api_data() -> List[Dict]:
    """
    Busca dados da API SPTrans.
    
    Returns:
        Lista de linhas com veículos
    """
    logger.info("fetching_api_data")
    
    with SPTransAPIClient() as client:
        lines = client.get_all_positions()
    
    logger.info(
        "api_data_fetched",
        total_lines=len(lines)
    )
    
    return lines


def transform_api_data_to_dataframe(
    spark: SparkSession,
    api_data: List[Dict],
    execution_timestamp: datetime
) -> DataFrame:
    """
    Transforma dados da API em DataFrame Spark.
    
    Args:
        spark: SparkSession
        api_data: Dados brutos da API
        execution_timestamp: Timestamp de execução
    
    Returns:
        DataFrame com schema do Bronze Layer
    """
    logger.info("transforming_api_data", records=len(api_data))
    
    # Criar DataFrame do JSON
    df_raw = spark.createDataFrame(api_data)
    
    # Explodir array de veículos
    df_exploded = df_raw.select(
        col("c").alias("route_code"),
        col("cl").alias("route_id"),
        col("sl").alias("direction"),
        col("lt0").alias("destination_0"),
        col("lt1").alias("destination_1"),
        explode(col("vs")).alias("vehicle")
    )
    
    # Extrair campos do veículo
    df_vehicles = df_exploded.select(
        col("route_code"),
        col("route_id"),
        col("direction"),
        col("destination_0"),
        col("destination_1"),
        col("vehicle.p").cast("int").alias("vehicle_id"),
        col("vehicle.a").alias("accessible"),
        col("vehicle.py").alias("latitude"),
        col("vehicle.px").alias("longitude"),
        to_timestamp(col("vehicle.ta"), "yyyy-MM-dd HH:mm:ss").alias("timestamp")
    )
    
    # Adicionar metadados
    df_final = df_vehicles \
        .withColumn("ingestion_timestamp", lit(execution_timestamp)) \
        .withColumn("pipeline_version", lit(PROJECT_VERSION)) \
        .withColumn("source_system", lit("sptrans_api"))
    
    # Validar schema
    expected_schema = get_bronze_api_positions_schema()
    
    # Reordenar colunas conforme schema
    df_final = df_final.select([field.name for field in expected_schema.fields])
    
    record_count = df_final.count()
    
    logger.info(
        "data_transformed",
        input_lines=len(api_data),
        output_records=record_count
    )
    
    return df_final


def write_to_bronze(df: DataFrame, execution_timestamp: datetime) -> None:
    """
    Escreve DataFrame no Bronze Layer (MinIO).
    
    Args:
        df: DataFrame para escrever
        execution_timestamp: Timestamp de execução para particionamento
    """
    logger.info("writing_to_bronze", records=df.count())
    
    # Gerar caminho particionado
    output_path = get_partition_path(
        DataLakePath.BRONZE_API_POSITIONS,
        execution_timestamp
    )
    
    logger.info("output_path_generated", path=output_path)
    
    # Escrever como Parquet
    df.write \
        .mode("overwrite") \
        .format("parquet") \
        .option("compression", "snappy") \
        .save(output_path)
    
    logger.info(
        "write_completed",
        path=output_path,
        records=df.count()
    )


def main(execution_date_str: str) -> None:
    """
    Função principal do job.
    
    Args:
        execution_date_str: Data de execução (ISO format)
    """
    # Parse execution date
    execution_timestamp = datetime.fromisoformat(execution_date_str)
    
    logger.info(
        "job_started",
        execution_date=execution_date_str
    )
    
    try:
        # 1. Criar Spark Session
        spark = create_spark_session()
        
        # 2. Buscar dados da API
        api_data = fetch_api_data()
        
        if not api_data:
            logger.warning("no_data_fetched_from_api")
            return
        
        # 3. Transformar em DataFrame
        df = transform_api_data_to_dataframe(spark, api_data, execution_timestamp)
        
        # 4. Escrever no Bronze Layer
        write_to_bronze(df, execution_timestamp)
        
        logger.info(
            "job_completed_successfully",
            execution_date=execution_date_str,
            records_processed=df.count()
        )
    
    except Exception as e:
        logger.error(
            "job_failed",
            execution_date=execution_date_str,
            error=str(e),
            exc_info=True
        )
        raise
    
    finally:
        # Finalizar Spark
        if 'spark' in locals():
            spark.stop()
            logger.info("spark_session_stopped")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest SPTrans API to Bronze Layer')
    parser.add_argument(
        '--execution-date',
        required=True,
        help='Execution date in ISO format (YYYY-MM-DDTHH:MM:SS)'
    )
    
    args = parser.parse_args()
    
    main(args.execution_date)