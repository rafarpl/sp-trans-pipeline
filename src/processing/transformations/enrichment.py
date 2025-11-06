"""
Data Enrichment Module

Responsável por enriquecer dados com informações adicionais:
- Integração com GTFS (rotas, linhas, paradas)
- Informações de clima
- Dados de trânsito
- Metadados contextuais
"""

from typing import Dict, List, Optional
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.logging_config import get_logger
from src.common.metrics import MetricsCollector

logger = get_logger(__name__)


class DataEnricher:
    """
    Classe para enriquecimento de dados.
    
    Adiciona informações contextuais aos dados brutos:
    - Informações GTFS (rotas, shapes, stops)
    - Metadados temporais (dia da semana, período do dia)
    - Informações calculadas (distância, velocidade)
    """
    
    def __init__(self, spark: SparkSession):
        """
        Inicializa o Data Enricher.
        
        Args:
            spark: SparkSession ativa
        """
        self.spark = spark
        self.metrics = MetricsCollector()
        self.logger = get_logger(self.__class__.__name__)
    
    def enrich_with_gtfs_routes(
        self,
        df: DataFrame,
        gtfs_routes_path: str
    ) -> DataFrame:
        """
        Enriquece com informações de rotas GTFS.
        
        Args:
            df: DataFrame com posições de veículos
            gtfs_routes_path: Caminho para dados de rotas GTFS
            
        Returns:
            DataFrame enriquecido com informações de rotas
        """
        self.logger.info("Enriching with GTFS routes data")
        
        # Carregar rotas GTFS
        gtfs_routes = self.spark.read.parquet(gtfs_routes_path)
        
        # Join com dados de veículos
        df_enriched = df.join(
            gtfs_routes,
            on="route_id",
            how="left"
        )
        
        # Contar registros enriquecidos
        enriched_count = df_enriched.filter(F.col("route_long_name").isNotNull()).count()
        total_count = df.count()
        
        enrichment_rate = (enriched_count / total_count) * 100 if total_count > 0 else 0
        
        self.logger.info(
            f"Enriched {enriched_count}/{total_count} records with GTFS routes "
            f"({enrichment_rate:.2f}%)"
        )
        self.metrics.gauge("enrichment.gtfs_routes_rate", enrichment_rate)
        
        return df_enriched
    
    def enrich_with_gtfs_stops(
        self,
        df: DataFrame,
        gtfs_stops_path: str,
        distance_threshold_meters: float = 50.0
    ) -> DataFrame:
        """
        Enriquece com informações de paradas GTFS próximas.
        
        Args:
            df: DataFrame com posições de veículos
            gtfs_stops_path: Caminho para dados de paradas GTFS
            distance_threshold_meters: Distância máxima para considerar próximo
            
        Returns:
            DataFrame enriquecido com informação de paradas próximas
        """
        self.logger.info("Enriching with nearby GTFS stops")
        
        # Carregar paradas GTFS
        gtfs_stops = self.spark.read.parquet(gtfs_stops_path)
        
        # Calcular distância usando fórmula de Haversine simplificada
        # Para São Paulo, aproximação funciona bem em distâncias curtas
        df_with_stops = df.crossJoin(
            gtfs_stops.select(
                F.col("stop_id").alias("nearby_stop_id"),
                F.col("stop_name").alias("nearby_stop_name"),
                F.col("stop_lat").alias("stop_latitude"),
                F.col("stop_lon").alias("stop_longitude")
            )
        )
        
        # Calcular distância em metros (aproximação)
        df_with_distance = df_with_stops.withColumn(
            "distance_to_stop",
            F.expr("""
                6371000 * 2 * asin(sqrt(
                    pow(sin((radians(stop_latitude) - radians(latitude)) / 2), 2) +
                    cos(radians(latitude)) * cos(radians(stop_latitude)) *
                    pow(sin((radians(stop_longitude) - radians(longitude)) / 2), 2)
                ))
            """)
        )
        
        # Filtrar apenas paradas próximas
        df_nearby_stops = df_with_distance.filter(
            F.col("distance_to_stop") <= distance_threshold_meters
        )
        
        # Para cada veículo, pegar a parada mais próxima
        window_spec = Window.partitionBy("vehicle_id", "timestamp").orderBy("distance_to_stop")
        
        df_enriched = (
            df_nearby_stops
            .withColumn("rank", F.row_number().over(window_spec))
            .filter(F.col("rank") == 1)
            .drop("rank")
        )
        
        enriched_count = df_enriched.count()
        total_count = df.count()
        
        enrichment_rate = (enriched_count / total_count) * 100 if total_count > 0 else 0
        
        self.logger.info(
            f"Found nearby stops for {enriched_count}/{total_count} records "
            f"({enrichment_rate:.2f}%)"
        )
        self.metrics.gauge("enrichment.nearby_stops_rate", enrichment_rate)
        
        return df_enriched
    
    def add_temporal_features(self, df: DataFrame) -> DataFrame:
        """
        Adiciona features temporais ao DataFrame.
        
        Args:
            df: DataFrame com coluna timestamp
            
        Returns:
            DataFrame com features temporais adicionadas
        """
        self.logger.info("Adding temporal features")
        
        df_enriched = (
            df
            # Data e hora
            .withColumn("date", F.to_date("timestamp"))
            .withColumn("hour", F.hour("timestamp"))
            .withColumn("minute", F.minute("timestamp"))
            
            # Dia da semana (1=Monday, 7=Sunday)
            .withColumn("day_of_week", F.dayofweek("timestamp"))
            .withColumn("day_name", F.date_format("timestamp", "EEEE"))
            
            # Fim de semana
            .withColumn("is_weekend", F.col("day_of_week").isin([1, 7]))
            
            # Período do dia
            .withColumn(
                "time_period",
                F.when((F.col("hour") >= 6) & (F.col("hour") < 12), "morning")
                .when((F.col("hour") >= 12) & (F.col("hour") < 18), "afternoon")
                .when((F.col("hour") >= 18) & (F.col("hour") < 22), "evening")
                .otherwise("night")
            )
            
            # Horário de pico
            .withColumn(
                "is_rush_hour",
                ((F.col("hour").between(7, 9)) | (F.col("hour").between(17, 19))) &
                (~F.col("is_weekend"))
            )
            
            # Semana do ano
            .withColumn("week_of_year", F.weekofyear("timestamp"))
            
            # Mês
            .withColumn("month", F.month("timestamp"))
            .withColumn("month_name", F.date_format("timestamp", "MMMM"))
            
            # Quarter
            .withColumn("quarter", F.quarter("timestamp"))
        )
        
        self.logger.info("Temporal features added successfully")
        
        return df_enriched
    
    def calculate_speed_from_positions(
        self,
        df: DataFrame,
        partition_by: str = "vehicle_id"
    ) -> DataFrame:
        """
        Calcula velocidade baseado em posições consecutivas.
        
        Args:
            df: DataFrame com latitude, longitude, timestamp
            partition_by: Coluna para particionar (geralmente vehicle_id)
            
        Returns:
            DataFrame com velocidade calculada
        """
        self.logger.info("Calculating speed from consecutive positions")
        
        window_spec = Window.partitionBy(partition_by).orderBy("timestamp")
        
        df_with_prev = (
            df
            .withColumn("prev_latitude", F.lag("latitude").over(window_spec))
            .withColumn("prev_longitude", F.lag("longitude").over(window_spec))
            .withColumn("prev_timestamp", F.lag("timestamp").over(window_spec))
        )
        
        # Calcular distância (metros) usando Haversine
        df_with_distance = df_with_prev.withColumn(
            "distance_meters",
            F.when(
                F.col("prev_latitude").isNotNull(),
                F.expr("""
                    6371000 * 2 * asin(sqrt(
                        pow(sin((radians(latitude) - radians(prev_latitude)) / 2), 2) +
                        cos(radians(prev_latitude)) * cos(radians(latitude)) *
                        pow(sin((radians(longitude) - radians(prev_longitude)) / 2), 2)
                    ))
                """)
            )
        )
        
        # Calcular tempo decorrido (segundos)
        df_with_time = df_with_distance.withColumn(
            "time_diff_seconds",
            F.when(
                F.col("prev_timestamp").isNotNull(),
                F.unix_timestamp("timestamp") - F.unix_timestamp("prev_timestamp")
            )
        )
        
        # Calcular velocidade (km/h)
        df_with_speed = df_with_time.withColumn(
            "calculated_speed_kmh",
            F.when(
                (F.col("time_diff_seconds") > 0) & (F.col("distance_meters").isNotNull()),
                (F.col("distance_meters") / F.col("time_diff_seconds")) * 3.6  # m/s para km/h
            )
        )
        
        # Limpar colunas auxiliares
        df_clean = df_with_speed.drop(
            "prev_latitude", "prev_longitude", "prev_timestamp",
            "distance_meters", "time_diff_seconds"
        )
        
        calculated_count = df_clean.filter(F.col("calculated_speed_kmh").isNotNull()).count()
        total_count = df.count()
        
        self.logger.info(
            f"Calculated speed for {calculated_count}/{total_count} records"
        )
        
        return df_clean
    
    def add_trip_metadata(self, df: DataFrame) -> DataFrame:
        """
        Adiciona metadados sobre a viagem/trajeto.
        
        Args:
            df: DataFrame com dados de veículos
            
        Returns:
            DataFrame com metadados de viagem
        """
        self.logger.info("Adding trip metadata")
        
        window_spec = Window.partitionBy("vehicle_id", "trip_id").orderBy("timestamp")
        
        df_with_metadata = (
            df
            # Sequência de pontos na viagem
            .withColumn("trip_sequence", F.row_number().over(window_spec))
            
            # Primeira e última posição da viagem
            .withColumn("is_trip_start", F.col("trip_sequence") == 1)
            .withColumn(
                "is_trip_end",
                F.col("trip_sequence") == F.count("*").over(
                    Window.partitionBy("vehicle_id", "trip_id")
                )
            )
            
            # Duração da viagem até o momento (minutos)
            .withColumn(
                "trip_duration_minutes",
                (
                    F.unix_timestamp("timestamp") -
                    F.unix_timestamp(F.first("timestamp").over(window_spec))
                ) / 60
            )
        )
        
        self.logger.info("Trip metadata added successfully")
        
        return df_with_metadata
    
    def enrich_full(
        self,
        df: DataFrame,
        gtfs_routes_path: Optional[str] = None,
        gtfs_stops_path: Optional[str] = None
    ) -> DataFrame:
        """
        Aplica todos os enriquecimentos disponíveis.
        
        Args:
            df: DataFrame de entrada
            gtfs_routes_path: Caminho para rotas GTFS (opcional)
            gtfs_stops_path: Caminho para paradas GTFS (opcional)
            
        Returns:
            DataFrame totalmente enriquecido
        """
        self.logger.info("Starting full enrichment pipeline")
        
        df_enriched = df
        
        # Temporal features
        df_enriched = self.add_temporal_features(df_enriched)
        
        # Calcular velocidade se não existir
        if "speed" not in df_enriched.columns:
            df_enriched = self.calculate_speed_from_positions(df_enriched)
        
        # GTFS enrichment (se paths fornecidos)
        if gtfs_routes_path:
            df_enriched = self.enrich_with_gtfs_routes(df_enriched, gtfs_routes_path)
        
        if gtfs_stops_path:
            df_enriched = self.enrich_with_gtfs_stops(df_enriched, gtfs_stops_path)
        
        # Trip metadata (se trip_id existir)
        if "trip_id" in df_enriched.columns:
            df_enriched = self.add_trip_metadata(df_enriched)
        
        self.logger.info("Full enrichment pipeline completed")
        
        return df_enriched


def enrich_with_gtfs(
    df: DataFrame,
    gtfs_routes_path: str,
    gtfs_stops_path: Optional[str] = None
) -> DataFrame:
    """
    Função utilitária para enriquecer dados com GTFS.
    
    Args:
        df: DataFrame de entrada
        gtfs_routes_path: Caminho para rotas GTFS
        gtfs_stops_path: Caminho para paradas GTFS (opcional)
        
    Returns:
        DataFrame enriquecido
    """
    spark = df.sparkSession
    enricher = DataEnricher(spark)
    
    df_enriched = enricher.enrich_with_gtfs_routes(df, gtfs_routes_path)
    
    if gtfs_stops_path:
        df_enriched = enricher.enrich_with_gtfs_stops(df_enriched, gtfs_stops_path)
    
    return df_enriched