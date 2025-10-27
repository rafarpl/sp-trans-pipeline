# =============================================================================
# SPARK STREAMING PROCESSOR
# =============================================================================
# Processamento em tempo real com Spark Structured Streaming
# =============================================================================

"""
Spark Streaming Processor

Processa dados de posições em tempo real usando Spark Structured Streaming:
    - Leitura de Kafka
    - Transformações em tempo real
    - Escrita contínua no Data Lake
    - Agregações em janelas de tempo
"""

import logging
from typing import Optional, Dict, Any, Callable
from datetime import datetime

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType,
        TimestampType, IntegerType, BooleanType
    )
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    SparkSession = None
    DataFrame = None

# =============================================================================
# LOGGING
# =============================================================================

logger = logging.getLogger(__name__)

# =============================================================================
# SPARK STREAMING PROCESSOR
# =============================================================================

class SparkStreamingProcessor:
    """Processador de streaming com Spark"""
    
    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        kafka_bootstrap_servers: str = "kafka:9092",
        kafka_topic: str = "sptrans-positions",
        checkpoint_location: str = "/tmp/spark-checkpoints",
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Inicializa processador de streaming.
        
        Args:
            spark: SparkSession (cria nova se None)
            kafka_bootstrap_servers: Servidores Kafka
            kafka_topic: Tópico a consumir
            checkpoint_location: Localização dos checkpoints
            config: Configurações adicionais
        
        Raises:
            ImportError: Se PySpark não estiver instalado
        """
        if not PYSPARK_AVAILABLE:
            raise ImportError(
                "PySpark não instalado. "
                "Instale com: pip install pyspark"
            )
        
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.checkpoint_location = checkpoint_location
        self.config = config or {}
        
        # Criar SparkSession se não fornecida
        if spark is None:
            self.spark = create_streaming_session()
        else:
            self.spark = spark
        
        # Schema para posições
        self.positions_schema = StructType([
            StructField("vehicle_id", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
            StructField("speed", DoubleType(), True),
            StructField("route_code", StringType(), True),
            StructField("direction", IntegerType(), True),
            StructField("accessibility", BooleanType(), True),
        ])
        
        logger.info(f"✓ Spark Streaming Processor inicializado")
        logger.info(f"Kafka: {kafka_bootstrap_servers}, Topic: {kafka_topic}")
    
    def read_from_kafka(self) -> DataFrame:
        """
        Lê stream de dados do Kafka.
        
        Returns:
            DataFrame streaming
        """
        logger.info("Lendo stream do Kafka...")
        
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", self.kafka_topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        logger.info("✓ Stream Kafka conectado")
        
        return df
    
    def parse_kafka_messages(self, df: DataFrame) -> DataFrame:
        """
        Parse mensagens JSON do Kafka.
        
        Args:
            df: DataFrame com mensagens raw do Kafka
        
        Returns:
            DataFrame com mensagens parseadas
        """
        logger.info("Parseando mensagens do Kafka...")
        
        # Converter value de binary para string
        df = df.selectExpr("CAST(value AS STRING) as json_string")
        
        # Parse JSON
        from pyspark.sql.functions import from_json
        
        df_parsed = df.select(
            from_json(F.col("json_string"), self.positions_schema).alias("data")
        ).select("data.*")
        
        # Adicionar timestamp de processamento
        df_parsed = df_parsed.withColumn(
            "processing_time",
            F.current_timestamp()
        )
        
        return df_parsed
    
    def apply_transformations(self, df: DataFrame) -> DataFrame:
        """
        Aplica transformações nos dados streaming.
        
        Args:
            df: DataFrame streaming
        
        Returns:
            DataFrame transformado
        """
        logger.info("Aplicando transformações...")
        
        # Adicionar colunas derivadas
        df = df.withColumn("date", F.to_date("timestamp")) \
               .withColumn("hour", F.hour("timestamp")) \
               .withColumn("minute", F.minute("timestamp"))
        
        # Validar coordenadas
        df = df.withColumn(
            "is_valid_coordinates",
            (F.col("latitude").between(-24.0, -23.3)) &
            (F.col("longitude").between(-46.9, -46.3))
        )
        
        # Filtrar apenas coordenadas válidas
        df = df.filter(F.col("is_valid_coordinates") == True)
        
        # Adicionar flag de movimento
        df = df.withColumn(
            "is_moving",
            F.when(F.col("speed") > 5, True).otherwise(False)
        )
        
        return df
    
    def write_to_lake(
        self,
        df: DataFrame,
        output_path: str,
        output_mode: str = "append",
        trigger_interval: str = "2 minutes",
        partition_by: list = None
    ):
        """
        Escreve stream no Data Lake.
        
        Args:
            df: DataFrame streaming
            output_path: Caminho de saída
            output_mode: Modo de escrita (append/complete/update)
            trigger_interval: Intervalo de trigger
            partition_by: Colunas para particionar
        
        Returns:
            StreamingQuery
        """
        logger.info(f"Configurando escrita para {output_path}...")
        
        partition_by = partition_by or ["date", "hour"]
        
        query = df.writeStream \
            .format("parquet") \
            .outputMode(output_mode) \
            .option("path", output_path) \
            .option("checkpointLocation", f"{self.checkpoint_location}/{datetime.now().strftime('%Y%m%d_%H%M%S')}") \
            .partitionBy(*partition_by) \
            .trigger(processingTime=trigger_interval) \
            .start()
        
        logger.info(f"✓ Stream escrevendo em {output_path}")
        
        return query
    
    def aggregate_windowed(
        self,
        df: DataFrame,
        window_duration: str = "5 minutes",
        slide_duration: str = "1 minute"
    ) -> DataFrame:
        """
        Agrega dados em janelas de tempo.
        
        Args:
            df: DataFrame streaming
            window_duration: Duração da janela
            slide_duration: Intervalo de slide
        
        Returns:
            DataFrame agregado
        """
        logger.info(f"Configurando agregação em janelas ({window_duration})...")
        
        # Definir janela de tempo
        windowed_df = df.groupBy(
            F.window("timestamp", window_duration, slide_duration),
            "route_code"
        ).agg(
            F.countDistinct("vehicle_id").alias("active_vehicles"),
            F.count("*").alias("total_observations"),
            F.avg("speed").alias("avg_speed"),
            F.max("speed").alias("max_speed"),
            F.sum(F.when(F.col("is_moving"), 1).otherwise(0)).alias("moving_count")
        )
        
        # Adicionar timestamp da janela
        windowed_df = windowed_df.withColumn(
            "window_start",
            F.col("window.start")
        ).withColumn(
            "window_end",
            F.col("window.end")
        ).drop("window")
        
        return windowed_df
    
    def write_to_console(
        self,
        df: DataFrame,
        output_mode: str = "append",
        trigger_interval: str = "10 seconds"
    ):
        """
        Escreve stream no console (para debug).
        
        Args:
            df: DataFrame streaming
            output_mode: Modo de saída
            trigger_interval: Intervalo de trigger
        
        Returns:
            StreamingQuery
        """
        logger.info("Configurando saída para console...")
        
        query = df.writeStream \
            .format("console") \
            .outputMode(output_mode) \
            .trigger(processingTime=trigger_interval) \
            .option("truncate", False) \
            .start()
        
        logger.info("✓ Stream escrevendo no console")
        
        return query


# =============================================================================
# FACTORY FUNCTIONS
# =============================================================================

def create_streaming_session(
    app_name: str = "SPTrans-Streaming"
) -> SparkSession:
    """
    Cria SparkSession configurada para streaming.
    
    Args:
        app_name: Nome da aplicação
    
    Returns:
        SparkSession
    """
    import os
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info(f"✓ Spark Streaming Session criada: {app_name}")
    
    return spark


def process_streaming_positions(
    kafka_bootstrap_servers: str = "kafka:9092",
    kafka_topic: str = "sptrans-positions",
    output_path: str = "s3a://sptrans-datalake/bronze/streaming",
    trigger_interval: str = "2 minutes"
):
    """
    Pipeline completo de streaming.
    
    Args:
        kafka_bootstrap_servers: Servidores Kafka
        kafka_topic: Tópico
        output_path: Caminho de saída
        trigger_interval: Intervalo de processamento
    
    Example:
        >>> process_streaming_positions(
        ...     kafka_topic="sptrans-positions",
        ...     output_path="s3a://bucket/bronze/streaming"
        ... )
    """
    logger.info("=== Iniciando pipeline de streaming ===")
    
    # Criar processor
    processor = SparkStreamingProcessor(
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        kafka_topic=kafka_topic
    )
    
    # Ler do Kafka
    raw_stream = processor.read_from_kafka()
    
    # Parse mensagens
    parsed_stream = processor.parse_kafka_messages(raw_stream)
    
    # Aplicar transformações
    transformed_stream = processor.apply_transformations(parsed_stream)
    
    # Escrever no Data Lake
    query = processor.write_to_lake(
        transformed_stream,
        output_path=output_path,
        trigger_interval=trigger_interval
    )
    
    logger.info("Pipeline iniciado. Aguardando término...")
    
    # Aguardar término
    query.awaitTermination()


# =============================================================================
# MAIN (para execução standalone)
# =============================================================================

def main():
    """Função main para execução standalone"""
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(description="Spark Streaming Processor")
    parser.add_argument("--kafka-servers", default="kafka:9092", help="Kafka bootstrap servers")
    parser.add_argument("--kafka-topic", default="sptrans-positions", help="Kafka topic")
    parser.add_argument("--output-path", required=True, help="Output path")
    parser.add_argument("--trigger-interval", default="2 minutes", help="Trigger interval")
    parser.add_argument("--console", action="store_true", help="Output to console (debug)")
    
    args = parser.parse_args()
    
    try:
        if args.console:
            # Modo debug (console)
            logger.info("Modo DEBUG: saída para console")
            
            processor = SparkStreamingProcessor(
                kafka_bootstrap_servers=args.kafka_servers,
                kafka_topic=args.kafka_topic
            )
            
            raw_stream = processor.read_from_kafka()
            parsed_stream = processor.parse_kafka_messages(raw_stream)
            transformed_stream = processor.apply_transformations(parsed_stream)
            
            query = processor.write_to_console(
                transformed_stream,
                trigger_interval=args.trigger_interval
            )
            
            query.awaitTermination()
        
        else:
            # Modo produção (Data Lake)
            process_streaming_positions(
                kafka_bootstrap_servers=args.kafka_servers,
                kafka_topic=args.kafka_topic,
                output_path=args.output_path,
                trigger_interval=args.trigger_interval
            )
    
    except KeyboardInterrupt:
        logger.info("Streaming interrompido pelo usuário")
        sys.exit(0)
    
    except Exception as e:
        logger.error(f"Erro no streaming: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

# =============================================================================
# END
# =============================================================================
