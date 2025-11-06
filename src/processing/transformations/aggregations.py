"""
Aggregations Module

Responsável por agregações e cálculo de KPIs:
- Fleet coverage (cobertura da frota)
- Average speed (velocidade média)
- Headway (intervalo entre veículos)
- Punctuality (pontualidade)
- Trip statistics
"""

from typing import Dict, List, Optional
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common.logging_config import get_logger
from src.common.metrics import MetricsCollector

logger = get_logger(__name__)


class KPICalculator:
    """
    Classe para cálculo de KPIs (Key Performance Indicators).
    
    Calcula métricas de negócio importantes:
    - Cobertura da frota
    - Velocidade média
    - Headway
    - Pontualidade
    - Estatísticas de viagens
    """
    
    def __init__(self, spark: SparkSession):
        """
        Inicializa o calculador de KPIs.
        
        Args:
            spark: SparkSession ativa
        """
        self.spark = spark
        self.metrics = MetricsCollector()
        self.logger = get_logger(self.__class__.__name__)
    
    def calculate_fleet_coverage(
        self,
        df: DataFrame,
        total_fleet_size: int,
        time_window_minutes: int = 60
    ) -> DataFrame:
        """
        Calcula cobertura da frota (% de veículos ativos).
        
        Args:
            df: DataFrame com dados de veículos
            total_fleet_size: Tamanho total da frota
            time_window_minutes: Janela de tempo para considerar ativo
            
        Returns:
            DataFrame com KPI de cobertura por período
        """
        self.logger.info(f"Calculating fleet coverage (total fleet: {total_fleet_size})")
        
        # Agrupar por hora
        df_hourly = (
            df
            .withColumn("hour", F.date_trunc("hour", "timestamp"))
            .groupBy("hour")
            .agg(
                F.countDistinct("vehicle_id").alias("active_vehicles"),
                F.count("*").alias("total_positions")
            )
            .withColumn("total_fleet_size", F.lit(total_fleet_size))
            .withColumn(
                "coverage_percent",
                (F.col("active_vehicles") / F.col("total_fleet_size")) * 100
            )
            .orderBy("hour")
        )
        
        # Log estatísticas
        avg_coverage = df_hourly.agg(F.avg("coverage_percent")).collect()[0][0]
        self.logger.info(f"Average fleet coverage: {avg_coverage:.2f}%")
        self.metrics.gauge("kpi.fleet_coverage_avg", avg_coverage)
        
        return df_hourly
    
    def calculate_average_speed(
        self,
        df: DataFrame,
        group_by_cols: List[str] = None
    ) -> DataFrame:
        """
        Calcula velocidade média por diferentes dimensões.
        
        Args:
            df: DataFrame com dados de veículos (com coluna speed)
            group_by_cols: Colunas para agrupar (ex: line_id, hour, route_id)
            
        Returns:
            DataFrame com velocidade média agregada
        """
        if group_by_cols is None:
            group_by_cols = ["line_id", "hour"]
        
        self.logger.info(f"Calculating average speed grouped by: {group_by_cols}")
        
        # Preparar dados
        df_with_hour = df.withColumn("hour", F.date_trunc("hour", "timestamp"))
        
        # Calcular médias
        df_avg_speed = (
            df_with_hour
            .filter(F.col("speed").isNotNull())
            .groupBy(*group_by_cols)
            .agg(
                F.avg("speed").alias("avg_speed_kmh"),
                F.stddev("speed").alias("stddev_speed"),
                F.min("speed").alias("min_speed"),
                F.max("speed").alias("max_speed"),
                F.count("*").alias("sample_size")
            )
            .orderBy(*group_by_cols)
        )
        
        # Log estatísticas gerais
        overall_avg = df.filter(F.col("speed").isNotNull()).agg(F.avg("speed")).collect()[0][0]
        self.logger.info(f"Overall average speed: {overall_avg:.2f} km/h")
        self.metrics.gauge("kpi.avg_speed", overall_avg)
        
        return df_avg_speed
    
    def calculate_headway(
        self,
        df: DataFrame,
        stop_id_col: str = "stop_id",
        line_id_col: str = "line_id"
    ) -> DataFrame:
        """
        Calcula headway (intervalo entre veículos consecutivos).
        
        Args:
            df: DataFrame com passagens de veículos em paradas
            stop_id_col: Coluna com ID da parada
            line_id_col: Coluna com ID da linha
            
        Returns:
            DataFrame com headway calculado
        """
        self.logger.info("Calculating headway (interval between vehicles)")
        
        # Window spec: particionar por parada e linha, ordenar por timestamp
        window_spec = Window.partitionBy(stop_id_col, line_id_col).orderBy("timestamp")
        
        # Calcular tempo desde o último veículo
        df_with_headway = (
            df
            .withColumn("prev_timestamp", F.lag("timestamp").over(window_spec))
            .withColumn(
                "headway_minutes",
                (F.unix_timestamp("timestamp") - F.unix_timestamp("prev_timestamp")) / 60
            )
            .filter(F.col("headway_minutes").isNotNull())
        )
        
        # Agregações por parada e linha
        df_headway_summary = (
            df_with_headway
            .groupBy(stop_id_col, line_id_col)
            .agg(
                F.avg("headway_minutes").alias("avg_headway_minutes"),
                F.stddev("headway_minutes").alias("stddev_headway"),
                F.min("headway_minutes").alias("min_headway"),
                F.max("headway_minutes").alias("max_headway"),
                F.count("*").alias("sample_size")
            )
        )
        
        # Classificar qualidade do headway
        df_headway_classified = df_headway_summary.withColumn(
            "headway_quality",
            F.when(F.col("avg_headway_minutes") <= 5, "excellent")
            .when(F.col("avg_headway_minutes") <= 10, "good")
            .when(F.col("avg_headway_minutes") <= 15, "fair")
            .otherwise("poor")
        )
        
        # Log estatísticas
        overall_avg_headway = df_with_headway.agg(F.avg("headway_minutes")).collect()[0][0]
        self.logger.info(f"Overall average headway: {overall_avg_headway:.2f} minutes")
        self.metrics.gauge("kpi.avg_headway_minutes", overall_avg_headway)
        
        return df_headway_classified
    
    def calculate_punctuality(
        self,
        df_actual: DataFrame,
        df_scheduled: DataFrame,
        tolerance_minutes: int = 5
    ) -> DataFrame:
        """
        Calcula pontualidade (aderência aos horários programados).
        
        Args:
            df_actual: DataFrame com horários reais
            df_scheduled: DataFrame com horários programados
            tolerance_minutes: Tolerância em minutos para considerar pontual
            
        Returns:
            DataFrame com métricas de pontualidade
        """
        self.logger.info(f"Calculating punctuality (tolerance: {tolerance_minutes} min)")
        
        # Join entre horários reais e programados
        df_comparison = df_actual.join(
            df_scheduled,
            on=["trip_id", "stop_id"],
            how="inner"
        )
        
        # Calcular diferença em minutos
        df_with_diff = df_comparison.withColumn(
            "delay_minutes",
            (F.unix_timestamp("actual_time") - F.unix_timestamp("scheduled_time")) / 60
        )
        
        # Classificar pontualidade
        df_punctuality = df_with_diff.withColumn(
            "is_on_time",
            F.abs(F.col("delay_minutes")) <= tolerance_minutes
        ).withColumn(
            "punctuality_status",
            F.when(F.col("delay_minutes") < -tolerance_minutes, "early")
            .when(F.col("delay_minutes") > tolerance_minutes, "late")
            .otherwise("on_time")
        )
        
        # Agregar por linha
        df_punctuality_summary = (
            df_punctuality
            .groupBy("line_id")
            .agg(
                F.avg("delay_minutes").alias("avg_delay_minutes"),
                F.stddev("delay_minutes").alias("stddev_delay"),
                (F.sum(F.when(F.col("is_on_time"), 1).otherwise(0)) / F.count("*") * 100)
                    .alias("on_time_percent"),
                F.count("*").alias("total_trips")
            )
        )
        
        # Log estatísticas
        overall_on_time = (
            df_punctuality.filter(F.col("is_on_time")).count() /
            df_punctuality.count() * 100
        )
        self.logger.info(f"Overall on-time performance: {overall_on_time:.2f}%")
        self.metrics.gauge("kpi.on_time_percent", overall_on_time)
        
        return df_punctuality_summary
    
    def calculate_trip_statistics(
        self,
        df: DataFrame
    ) -> DataFrame:
        """
        Calcula estatísticas de viagens.
        
        Args:
            df: DataFrame com dados de viagens
            
        Returns:
            DataFrame com estatísticas agregadas
        """
        self.logger.info("Calculating trip statistics")
        
        # Agregar por viagem
        df_trip_stats = (
            df
            .groupBy("trip_id", "line_id", "vehicle_id")
            .agg(
                F.min("timestamp").alias("trip_start_time"),
                F.max("timestamp").alias("trip_end_time"),
                F.count("*").alias("num_positions"),
                F.avg("speed").alias("avg_trip_speed"),
                F.max("speed").alias("max_trip_speed")
            )
            .withColumn(
                "trip_duration_minutes",
                (F.unix_timestamp("trip_end_time") - F.unix_timestamp("trip_start_time")) / 60
            )
        )
        
        # Estatísticas por linha
        df_line_stats = (
            df_trip_stats
            .groupBy("line_id")
            .agg(
                F.count("trip_id").alias("total_trips"),
                F.avg("trip_duration_minutes").alias("avg_trip_duration_minutes"),
                F.avg("avg_trip_speed").alias("avg_line_speed_kmh"),
                F.countDistinct("vehicle_id").alias("unique_vehicles")
            )
        )
        
        # Log estatísticas
        total_trips = df_trip_stats.count()
        self.logger.info(f"Total trips analyzed: {total_trips}")
        self.metrics.gauge("kpi.total_trips", total_trips)
        
        return df_line_stats
    
    def calculate_daily_summary(
        self,
        df: DataFrame,
        date_col: str = "date"
    ) -> DataFrame:
        """
        Calcula resumo diário de operações.
        
        Args:
            df: DataFrame com dados diários
            date_col: Coluna de data
            
        Returns:
            DataFrame com resumo diário
        """
        self.logger.info("Calculating daily summary")
        
        df_daily = (
            df
            .withColumn("date", F.to_date("timestamp"))
            .groupBy("date")
            .agg(
                # Veículos
                F.countDistinct("vehicle_id").alias("unique_vehicles"),
                F.count("*").alias("total_positions"),
                
                # Velocidade
                F.avg("speed").alias("avg_speed_kmh"),
                F.max("speed").alias("max_speed_kmh"),
                
                # Cobertura temporal
                F.min("timestamp").alias("first_position_time"),
                F.max("timestamp").alias("last_position_time"),
                
                # Linhas
                F.countDistinct("line_id").alias("unique_lines"),
                
                # Viagens
                F.countDistinct("trip_id").alias("total_trips")
            )
            .withColumn(
                "operational_hours",
                (F.unix_timestamp("last_position_time") - 
                 F.unix_timestamp("first_position_time")) / 3600
            )
            .orderBy("date")
        )
        
        return df_daily
    
    def calculate_all_kpis(
        self,
        df: DataFrame,
        total_fleet_size: int,
        df_scheduled: Optional[DataFrame] = None
    ) -> Dict[str, DataFrame]:
        """
        Calcula todos os KPIs disponíveis.
        
        Args:
            df: DataFrame com dados de veículos
            total_fleet_size: Tamanho total da frota
            df_scheduled: DataFrame com horários programados (opcional)
            
        Returns:
            Dicionário com DataFrames de cada KPI
        """
        self.logger.info("Calculating all KPIs")
        
        kpis = {}
        
        # Fleet Coverage
        kpis["fleet_coverage"] = self.calculate_fleet_coverage(df, total_fleet_size)
        
        # Average Speed
        kpis["average_speed"] = self.calculate_average_speed(df)
        
        # Trip Statistics
        kpis["trip_stats"] = self.calculate_trip_statistics(df)
        
        # Daily Summary
        kpis["daily_summary"] = self.calculate_daily_summary(df)
        
        # Punctuality (se dados programados disponíveis)
        if df_scheduled is not None:
            kpis["punctuality"] = self.calculate_punctuality(df, df_scheduled)
        
        self.logger.info(f"Calculated {len(kpis)} KPIs")
        
        return kpis


def calculate_fleet_coverage(
    df: DataFrame,
    total_fleet_size: int
) -> DataFrame:
    """
    Função utilitária para calcular cobertura da frota.
    
    Args:
        df: DataFrame com dados de veículos
        total_fleet_size: Tamanho total da frota
        
    Returns:
        DataFrame com cobertura por hora
    """
    spark = df.sparkSession
    calculator = KPICalculator(spark)
    return calculator.calculate_fleet_coverage(df, total_fleet_size)


def create_kpi_dashboard_data(
    spark: SparkSession,
    vehicle_positions_path: str,
    output_path: str,
    total_fleet_size: int = 15000
) -> None:
    """
    Cria dados agregados para dashboard de KPIs.
    
    Args:
        spark: SparkSession
        vehicle_positions_path: Caminho para dados de posições
        output_path: Caminho para salvar KPIs agregados
        total_fleet_size: Tamanho total da frota
    """
    logger.info("Creating KPI dashboard data")
    
    # Carregar dados
    df = spark.read.parquet(vehicle_positions_path)
    
    # Calcular KPIs
    calculator = KPICalculator(spark)
    kpis = calculator.calculate_all_kpis(df, total_fleet_size)
    
    # Salvar cada KPI
    for kpi_name, kpi_df in kpis.items():
        kpi_path = f"{output_path}/{kpi_name}"
        kpi_df.write.mode("overwrite").parquet(kpi_path)
        logger.info(f"Saved KPI '{kpi_name}' to {kpi_path}")
    
    logger.info("KPI dashboard data creation completed")