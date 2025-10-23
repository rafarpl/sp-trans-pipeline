"""
DAG 04 - Silver to Gold Aggregation
====================================
Agregação de dados da camada Silver para Gold com cálculo de KPIs.

Execução: A cada hora
Processamento:
- KPIs por hora e rota
- Métricas de performance
- Análise de headway
- Estatísticas operacionais

Autor: Rafael - SPTrans Pipeline
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.common.config import Config
from src.common.logging_config import get_logger
from src.processing.jobs.silver_to_gold import SilverToGoldJob
from src.common.metrics import reporter, track_job_execution

logger = get_logger(__name__)


# ===========================
# CONFIGURAÇÕES DO DAG
# ===========================

default_args = {
    'owner': 'sptrans-data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email': ['alerts@sptrans-pipeline.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

dag = DAG(
    'dag_04_silver_to_gold',
    default_args=default_args,
    description='Agregação Silver → Gold com KPIs e métricas',
    schedule_interval='0 * * * *',  # A cada hora no minuto 0
    catchup=False,
    max_active_runs=1,
    tags=['aggregation', 'silver', 'gold', 'kpis', 'analytics'],
)


# ===========================
# FUNÇÕES DAS TASKS
# ===========================

@track_job_execution('validate_silver_data')
def validate_silver_availability(**context):
    """
    Task 1: Valida se dados estão disponíveis na camada Silver.
    """
    logger.info("Validando disponibilidade de dados na Silver")
    
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    config = Config()
    
    spark = SparkSession.builder \
        .appName("Silver_Data_Validation") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()
    
    try:
        silver_path = f"{config.SILVER_LAYER_PATH}/positions_enriched"
        
        # Ler dados
        df = spark.read.parquet(silver_path)
        
        # Filtrar última hora
        recent_data = df.filter(
            F.col('silver_processing_timestamp') >= F.expr("current_timestamp() - interval 1 hour")
        )
        
        count = recent_data.count()
        
        if count == 0:
            logger.warning("Nenhum dado recente na Silver (última hora)")
        else:
            logger.info(f"Dados disponíveis: {count:,} registros na última hora")
        
        # Estatísticas básicas
        stats = {
            'total_records': count,
            'unique_vehicles': recent_data.select('vehicle_id').distinct().count(),
            'unique_routes': recent_data.select('route_code').distinct().count(),
            'date_range': {
                'min': recent_data.agg(F.min('timestamp')).collect()[0][0],
                'max': recent_data.agg(F.max('timestamp')).collect()[0][0]
            }
        }
        
        logger.info(f"Estatísticas: {stats}")
        
        # Push para XCom
        context['task_instance'].xcom_push(key='silver_stats', value=stats)
        
        return stats
    
    finally:
        spark.stop()


@track_job_execution('calculate_hourly_kpis')
def calculate_hourly_kpis(**context):
    """
    Task 2: Calcula KPIs por hora.
    
    KPIs incluem:
    - Total de veículos ativos
    - Veículos em movimento vs parados
    - Velocidade média/máxima
    - Score de qualidade de dados
    """
    logger.info("Calculando KPIs por hora")
    
    execution_date = context['execution_date']
    config = Config()
    
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    
    spark = SparkSession.builder \
        .appName("Hourly_KPIs_Calculation") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()
    
    try:
        # Ler dados da Silver
        silver_path = f"{config.SILVER_LAYER_PATH}/positions_enriched"
        df = spark.read.parquet(silver_path)
        
        # Filtrar última hora
        df_hour = df.filter(
            F.col('silver_processing_timestamp') >= F.expr("current_timestamp() - interval 1 hour")
        )
        
        logger.info(f"Processando {df_hour.count():,} registros")
        
        # Agregações por hora e rota
        kpis_hourly = df_hour.groupBy(
            'date',
            'hour',
            'route_code',
            'route_short_name'
        ).agg(
            F.countDistinct('vehicle_id').alias('total_vehicles'),
            F.sum(F.when(F.col('is_moving'), 1).otherwise(0)).alias('vehicles_moving'),
            F.sum(F.when(~F.col('is_moving'), 1).otherwise(0)).alias('vehicles_stopped'),
            F.avg('speed_kmh').alias('avg_speed_kmh'),
            F.max('speed_kmh').alias('max_speed_kmh'),
            F.count('*').alias('total_records'),
            F.avg('data_quality_score').alias('data_quality_score')
        )
        
        # Adicionar timestamp de processamento
        kpis_hourly = kpis_hourly.withColumn(
            'processing_timestamp',
            F.lit(datetime.now())
        )
        
        # Salvar na Gold
        gold_kpis_path = f"{config.GOLD_LAYER_PATH}/kpis_hourly"
        
        kpis_hourly.write \
            .mode('append') \
            .partitionBy('date') \
            .parquet(gold_kpis_path)
        
        kpi_count = kpis_hourly.count()
        logger.info(f"KPIs calculados: {kpi_count:,} registros")
        
        # Push para XCom
        context['task_instance'].xcom_push(
            key='kpis_count',
            value=kpi_count
        )
        
        return {
            'kpis_generated': kpi_count,
            'output_path': gold_kpis_path
        }
    
    finally:
        spark.stop()


@track_job_execution('calculate_route_metrics')
def calculate_route_metrics(**context):
    """
    Task 3: Calcula métricas por rota.
    
    Métricas incluem:
    - Total de viagens
    - Distância percorrida
    - Horas de serviço
    - Performance operacional
    """
    logger.info("Calculando métricas por rota")
    
    execution_date = context['execution_date']
    config = Config()
    
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    
    spark = SparkSession.builder \
        .appName("Route_Metrics_Calculation") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    try:
        # Ler dados da Silver
        silver_path = f"{config.SILVER_LAYER_PATH}/positions_enriched"
        df = spark.read.parquet(silver_path)
        
        # Filtrar último dia
        df_day = df.filter(
            F.col('date') == F.lit(execution_date.date())
        )
        
        logger.info(f"Processando dados de {execution_date.date()}")
        
        # Calcular distância total por veículo (soma das distâncias entre pontos)
        window_vehicle = Window.partitionBy('vehicle_id', 'date').orderBy('timestamp')
        
        df_with_distance = df_day.withColumn(
            'prev_lat',
            F.lag('latitude', 1).over(window_vehicle)
        ).withColumn(
            'prev_lon',
            F.lag('longitude', 1).over(window_vehicle)
        )
        
        # Calcular distância aproximada (em km)
        df_with_distance = df_with_distance.withColumn(
            'distance_km',
            F.when(
                F.col('prev_lat').isNotNull(),
                F.sqrt(
                    F.pow(F.col('latitude') - F.col('prev_lat'), 2) +
                    F.pow(F.col('longitude') - F.col('prev_lon'), 2)
                ) * 111  # Conversão para km
            ).otherwise(0)
        )
        
        # Agregar por rota
        route_metrics = df_with_distance.groupBy(
            'date',
            'route_code',
            'route_short_name',
            'route_long_name'
        ).agg(
            F.countDistinct('vehicle_id').alias('total_vehicles'),
            F.count('*').alias('total_observations'),
            F.avg('speed_kmh').alias('avg_speed'),
            F.sum('distance_km').alias('distance_traveled_km'),
            F.countDistinct(
                F.concat_ws('_', 'vehicle_id', 'hour')
            ).alias('vehicle_hours'),
            F.avg('data_quality_score').alias('data_quality_score')
        )
        
        # Calcular horas de serviço (aproximado)
        route_metrics = route_metrics.withColumn(
            'service_hours',
            F.col('vehicle_hours') / F.col('total_vehicles')
        )
        
        # Adicionar timestamp
        route_metrics = route_metrics.withColumn(
            'processing_timestamp',
            F.lit(datetime.now())
        )
        
        # Salvar na Gold
        gold_metrics_path = f"{config.GOLD_LAYER_PATH}/metrics_by_route"
        
        route_metrics.write \
            .mode('append') \
            .partitionBy('date') \
            .parquet(gold_metrics_path)
        
        metrics_count = route_metrics.count()
        logger.info(f"Métricas de rota calculadas: {metrics_count:,} rotas")
        
        return {
            'routes_processed': metrics_count,
            'output_path': gold_metrics_path
        }
    
    finally:
        spark.stop()


@track_job_execution('calculate_headway_analysis')
def calculate_headway_analysis(**context):
    """
    Task 4: Análise de headway (intervalo entre ônibus).
    
    Headway é o tempo entre a passagem de dois ônibus consecutivos
    da mesma linha em uma parada.
    """
    logger.info("Calculando análise de headway")
    
    execution_date = context['execution_date']
    config = Config()
    
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    
    spark = SparkSession.builder \
        .appName("Headway_Analysis") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    try:
        # Ler dados da Silver
        silver_path = f"{config.SILVER_LAYER_PATH}/positions_enriched"
        df = spark.read.parquet(silver_path)
        
        # Filtrar último dia
        df_day = df.filter(
            F.col('date') == F.lit(execution_date.date())
        )
        
        # Para cada rota e hora, calcular intervalo entre veículos
        window_headway = Window.partitionBy('route_code', 'hour').orderBy('timestamp')
        
        df_headway = df_day.withColumn(
            'prev_timestamp',
            F.lag('timestamp', 1).over(window_headway)
        ).withColumn(
            'prev_vehicle',
            F.lag('vehicle_id', 1).over(window_headway)
        )
        
        # Calcular headway em minutos
        df_headway = df_headway.withColumn(
            'headway_minutes',
            F.when(
                (F.col('prev_timestamp').isNotNull()) & 
                (F.col('vehicle_id') != F.col('prev_vehicle')),
                (F.unix_timestamp('timestamp') - F.unix_timestamp('prev_timestamp')) / 60.0
            )
        ).filter(F.col('headway_minutes').isNotNull())
        
        # Agregar estatísticas de headway
        headway_stats = df_headway.groupBy(
            'date',
            'hour',
            'route_code',
            'route_short_name'
        ).agg(
            F.avg('headway_minutes').alias('avg_headway_minutes'),
            F.min('headway_minutes').alias('min_headway_minutes'),
            F.max('headway_minutes').alias('max_headway_minutes'),
            F.stddev('headway_minutes').alias('headway_std_dev'),
            F.count('*').alias('observations')
        )
        
        # Calcular variância de headway (regularidade do serviço)
        headway_stats = headway_stats.withColumn(
            'headway_variance',
            F.col('headway_std_dev') / F.col('avg_headway_minutes')
        )
        
        # Adicionar timestamp
        headway_stats = headway_stats.withColumn(
            'processing_timestamp',
            F.lit(datetime.now())
        )
        
        # Salvar na Gold
        gold_headway_path = f"{config.GOLD_LAYER_PATH}/headway_analysis"
        
        headway_stats.write \
            .mode('append') \
            .partitionBy('date') \
            .parquet(gold_headway_path)
        
        headway_count = headway_stats.count()
        logger.info(f"Análise de headway concluída: {headway_count:,} registros")
        
        return {
            'headway_records': headway_count,
            'output_path': gold_headway_path
        }
    
    finally:
        spark.stop()


def generate_summary_statistics(**context):
    """
    Task 5: Gera estatísticas resumidas.
    """
    logger.info("Gerando estatísticas resumidas")
    
    ti = context['task_instance']
    execution_date = context['execution_date']
    
    # Coletar dados das tasks anteriores
    silver_stats = ti.xcom_pull(task_ids='validate_silver_availability', key='silver_stats')
    kpis_count = ti.xcom_pull(task_ids='calculate_hourly_kpis', key='kpis_count')
    
    summary = {
        'execution_date': execution_date.isoformat(),
        'silver_records_processed': silver_stats.get('total_records', 0),
        'unique_vehicles': silver_stats.get('unique_vehicles', 0),
        'unique_routes': silver_stats.get('unique_routes', 0),
        'kpis_generated': kpis_count,
        'timestamp': datetime.now().isoformat()
    }
    
    logger.info(f"Resumo: {summary}")
    
    # Reportar métricas
    reporter.report_processing_stats(
        layer='gold',
        source='aggregations',
        total=kpis_count,
        invalid=0,
        duplicated=0
    )
    
    return summary


def send_completion_notification(**context):
    """
    Task 6: Notificação de conclusão.
    """
    logger.info("Enviando notificação de conclusão")
    
    ti = context['task_instance']
    execution_date = context['execution_date']
    
    silver_stats = ti.xcom_pull(task_ids='validate_silver_availability', key='silver_stats')
    kpis_count = ti.xcom_pull(task_ids='calculate_hourly_kpis', key='kpis_count')
    
    message = f"""
    ✅ DAG Silver → Gold Concluída
    
    Data: {execution_date}
    
    📊 Processamento:
    - Registros Silver: {silver_stats.get('total_records', 0):,}
    - Veículos únicos: {silver_stats.get('unique_vehicles', 0):,}
    - Rotas únicas: {silver_stats.get('unique_routes', 0):,}
    
    📈 Agregações Geradas:
    - KPIs por hora: {kpis_count:,}
    - Métricas de rota: Calculadas
    - Análise de headway: Calculada
    
    Status: SUCCESS ✅
    """
    
    logger.info(message)
    
    return message


# ===========================
# DEFINIÇÃO DAS TASKS
# ===========================

with dag:
    
    start = EmptyOperator(
        task_id='start',
        dag=dag
    )
    
    # Task 1: Validação
    validate_task = PythonOperator(
        task_id='validate_silver_availability',
        python_callable=validate_silver_availability,
        provide_context=True,
        dag=dag
    )
    
    # Task 2: KPIs por hora
    kpis_task = PythonOperator(
        task_id='calculate_hourly_kpis',
        python_callable=calculate_hourly_kpis,
        provide_context=True,
        dag=dag
    )
    
    # Task 3: Métricas por rota
    metrics_task = PythonOperator(
        task_id='calculate_route_metrics',
        python_callable=calculate_route_metrics,
        provide_context=True,
        dag=dag
    )
    
    # Task 4: Análise de headway
    headway_task = PythonOperator(
        task_id='calculate_headway_analysis',
        python_callable=calculate_headway_analysis,
        provide_context=True,
        dag=dag
    )
    
    # Task 5: Estatísticas
    summary_task = PythonOperator(
        task_id='generate_summary_statistics',
        python_callable=generate_summary_statistics,
        provide_context=True,
        dag=dag
    )
    
    # Task 6: Notificação
    notify_task = PythonOperator(
        task_id='send_notification',
        python_callable=send_completion_notification,
        provide_context=True,
        trigger_rule='all_done',
        dag=dag
    )
    
    end = EmptyOperator(
        task_id='end',
        trigger_rule='all_done',
        dag=dag
    )
    
    # ===========================
    # FLUXO
    # ===========================
    
    # Validação primeiro
    start >> validate_task
    
    # Depois executa agregações em paralelo
    validate_task >> [kpis_task, metrics_task, headway_task]
    
    # Estatísticas e notificação
    [kpis_task, metrics_task, headway_task] >> summary_task >> notify_task >> end


# ===========================
# DOCUMENTAÇÃO
# ===========================

dag.doc_md = """
# DAG 04 - Silver to Gold Aggregation

## Objetivo
Agregar dados limpos da camada Silver em KPIs e métricas de negócio na camada Gold.

## Schedule
- **Frequência**: A cada hora (no minuto 0)
- **Dependências**: DAG 03 (Bronze to Silver)

## Agregações Calculadas

### 1. KPIs por Hora
- Total de veículos ativos
- Veículos em movimento vs parados
- Velocidade média e máxima
- Score de qualidade

### 2. Métricas por Rota
- Total de viagens
- Distância percorrida
- Horas de serviço
- Performance operacional

### 3. Análise de Headway
- Intervalo médio entre ônibus
- Regularidade do serviço
- Variância de headway

## Fluxo

```
Start → Validate Silver → [KPIs | Metrics | Headway] → Summary → Notify → End
```

## Uso das Agregações
- Dashboards executivos
- Análises de performance
- Planejamento operacional
- Relatórios gerenciais

## Monitoramento
- Métricas em Prometheus
- Dashboards em Grafana/Superset
- Alertas se processamento falhar
"""
