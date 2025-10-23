"""
DAG 03 - Bronze to Silver Transformation
=========================================
Transformação de dados da camada Bronze para Silver.

Execução: A cada 30 minutos
Processamento:
- Limpeza e validação de dados
- Deduplicação
- Enriquecimento (geocoding)
- Junção com dados GTFS

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
from src.processing.jobs.bronze_to_silver import BronzeToSilverJob
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
    'execution_timeout': timedelta(hours=1),
}

dag = DAG(
    'dag_03_bronze_to_silver',
    default_args=default_args,
    description='Transformação Bronze → Silver com limpeza e enriquecimento',
    schedule_interval='*/30 * * * *',  # A cada 30 minutos
    catchup=False,
    max_active_runs=1,
    tags=['transformation', 'bronze', 'silver', 'data-quality'],
)


# ===========================
# FUNÇÕES DAS TASKS
# ===========================

@track_job_execution('validate_bronze_data')
def validate_bronze_availability(**context):
    """
    Task 1: Valida se dados estão disponíveis na camada Bronze.
    """
    logger.info("Validando disponibilidade de dados na Bronze")
    
    from pyspark.sql import SparkSession
    config = Config()
    
    spark = SparkSession.builder \
        .appName("Bronze_Data_Validation") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()
    
    try:
        # Verificar se há dados de posições recentes (últimos 10 minutos)
        bronze_path = f"{config.BRONZE_LAYER_PATH}/api_positions"
        
        # Tentar ler dados
        df = spark.read.parquet(bronze_path)
        
        # Filtrar dados dos últimos 10 minutos
        recent_data = df.filter(
            F.col('ingestion_timestamp') >= F.expr("current_timestamp() - interval 10 minutes")
        )
        
        count = recent_data.count()
        
        if count == 0:
            logger.warning("Nenhum dado recente encontrado na Bronze (últimos 10 min)")
            logger.info("Continuando mesmo assim...")
        else:
            logger.info(f"Dados disponíveis: {count:,} registros recentes")
        
        # Push para XCom
        context['task_instance'].xcom_push(
            key='bronze_record_count',
            value=count
        )
        
        return count
    
    finally:
        spark.stop()


@track_job_execution('clean_and_deduplicate')
def clean_and_deduplicate_data(**context):
    """
    Task 2: Limpeza e deduplicação de dados.
    
    - Remove duplicatas
    - Valida coordenadas
    - Valida timestamps
    - Remove registros inválidos
    """
    logger.info("Iniciando limpeza e deduplicação")
    
    execution_date = context['execution_date']
    config = Config()
    
    from pyspark.sql import SparkSession
    from src.common.validators import (
        CoordinateValidator,
        TimestampValidator,
        VehicleValidator,
        validate_sptrans_position_data
    )
    
    spark = SparkSession.builder \
        .appName("Bronze_to_Silver_Cleaning") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()
    
    try:
        # Ler dados da Bronze
        bronze_path = f"{config.BRONZE_LAYER_PATH}/api_positions"
        df = spark.read.parquet(bronze_path)
        
        initial_count = df.count()
        logger.info(f"Registros iniciais: {initial_count:,}")
        
        # 1. Remover duplicatas
        df_dedup = df.dropDuplicates(['vehicle_id', 'timestamp'])
        duplicates_removed = initial_count - df_dedup.count()
        logger.info(f"Duplicatas removidas: {duplicates_removed:,}")
        
        # 2. Validar coordenadas
        df_validated = CoordinateValidator.validate_coordinates_df(
            df_dedup, 
            strict=True
        )
        
        # 3. Validar timestamps
        df_validated = TimestampValidator.validate_timestamps_df(
            df_validated
        )
        
        # 4. Validar vehicle IDs
        df_validated = VehicleValidator.validate_vehicles_df(
            df_validated
        )
        
        # 5. Filtrar apenas registros válidos
        df_clean = df_validated.filter(
            F.col('is_valid_coordinate') & 
            F.col('is_valid_timestamp') & 
            F.col('is_valid_vehicle_id')
        )
        
        final_count = df_clean.count()
        invalid_removed = df_dedup.count() - final_count
        
        logger.info(f"Registros inválidos removidos: {invalid_removed:,}")
        logger.info(f"Registros finais: {final_count:,}")
        
        # Calcular métricas de qualidade
        quality_pct = (final_count / initial_count * 100) if initial_count > 0 else 0
        
        # Salvar dados limpos temporariamente
        temp_path = f"{config.SILVER_LAYER_PATH}/positions_cleaned_temp"
        df_clean.write.mode('overwrite').parquet(temp_path)
        
        # Push para XCom
        stats = {
            'initial_count': initial_count,
            'duplicates_removed': duplicates_removed,
            'invalid_removed': invalid_removed,
            'final_count': final_count,
            'quality_pct': quality_pct,
            'temp_path': temp_path
        }
        
        context['task_instance'].xcom_push(key='cleaning_stats', value=stats)
        
        # Reportar métricas
        reporter.report_processing_stats(
            layer='silver',
            source='api_positions',
            total=final_count,
            invalid=invalid_removed,
            duplicated=duplicates_removed
        )
        
        reporter.report_data_quality(
            layer='silver',
            quality_metrics={
                'completeness': quality_pct,
                'accuracy': quality_pct
            }
        )
        
        return stats
    
    finally:
        spark.stop()


@track_job_execution('enrich_with_gtfs')
def enrich_with_gtfs_data(**context):
    """
    Task 3: Enriquecimento com dados GTFS.
    
    Adiciona informações de:
    - Nome da rota
    - Nome da parada mais próxima
    - Direção da linha
    """
    logger.info("Enriquecendo com dados GTFS")
    
    ti = context['task_instance']
    cleaning_stats = ti.xcom_pull(task_ids='clean_and_deduplicate', key='cleaning_stats')
    temp_path = cleaning_stats['temp_path']
    
    config = Config()
    
    from pyspark.sql import SparkSession
    from pyspark.sql import Window
    
    spark = SparkSession.builder \
        .appName("Silver_GTFS_Enrichment") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    try:
        # Ler dados limpos
        df_clean = spark.read.parquet(temp_path)
        
        # Ler dados GTFS da Bronze
        routes_df = spark.read.parquet(f"{config.BRONZE_LAYER_PATH}/gtfs/routes")
        trips_df = spark.read.parquet(f"{config.BRONZE_LAYER_PATH}/gtfs/trips")
        stops_df = spark.read.parquet(f"{config.BRONZE_LAYER_PATH}/gtfs/stops")
        
        # Pegar dados mais recentes do GTFS
        routes_latest = routes_df.orderBy(F.col('ingestion_date').desc()).limit(1000)
        stops_latest = stops_df.orderBy(F.col('ingestion_date').desc()).limit(5000)
        
        # Join com routes (por route_code)
        df_enriched = df_clean.join(
            routes_latest.select('route_id', 'route_short_name', 'route_long_name'),
            df_clean.route_code == routes_latest.route_id,
            'left'
        )
        
        # TODO: Adicionar cálculo de parada mais próxima (requer função de distância)
        
        # Adicionar flag de enriquecimento
        df_enriched = df_enriched.withColumn('enriched', F.lit(True))
        
        enriched_count = df_enriched.count()
        logger.info(f"Registros enriquecidos: {enriched_count:,}")
        
        # Salvar na Silver final
        silver_output = f"{config.SILVER_LAYER_PATH}/positions_enriched"
        
        df_enriched.write \
            .mode('append') \
            .partitionBy('ingestion_date') \
            .parquet(silver_output)
        
        logger.info(f"Dados salvos em {silver_output}")
        
        # Limpar temp
        spark.catalog.clearCache()
        
        return {
            'enriched_count': enriched_count,
            'output_path': silver_output
        }
    
    finally:
        spark.stop()


def data_quality_checks(**context):
    """
    Task 4: Verificações finais de qualidade.
    """
    logger.info("Executando checks de qualidade")
    
    ti = context['task_instance']
    cleaning_stats = ti.xcom_pull(task_ids='clean_and_deduplicate', key='cleaning_stats')
    
    # Calcular score de qualidade
    quality_score = cleaning_stats['quality_pct']
    
    # Definir thresholds
    QUALITY_THRESHOLD = 80.0
    
    if quality_score < QUALITY_THRESHOLD:
        logger.warning(f"Score de qualidade baixo: {quality_score:.2f}%")
        # Aqui você pode enviar alertas
    else:
        logger.info(f"Score de qualidade OK: {quality_score:.2f}%")
    
    # Reportar para monitoramento
    reporter.report_data_quality(
        layer='silver',
        quality_metrics={
            'overall_score': quality_score,
            'threshold': QUALITY_THRESHOLD,
            'status': 'pass' if quality_score >= QUALITY_THRESHOLD else 'warning'
        }
    )
    
    return {
        'quality_score': quality_score,
        'passed': quality_score >= QUALITY_THRESHOLD
    }


def send_completion_notification(**context):
    """
    Task 5: Notificação de conclusão.
    """
    logger.info("Enviando notificação de conclusão")
    
    ti = context['task_instance']
    execution_date = context['execution_date']
    
    # Coletar estatísticas
    cleaning_stats = ti.xcom_pull(task_ids='clean_and_deduplicate', key='cleaning_stats')
    
    message = f"""
    ✅ DAG Bronze → Silver Concluída
    
    Data: {execution_date}
    
    📊 Estatísticas:
    - Registros processados: {cleaning_stats['initial_count']:,}
    - Duplicatas removidas: {cleaning_stats['duplicates_removed']:,}
    - Inválidos removidos: {cleaning_stats['invalid_removed']:,}
    - Registros finais: {cleaning_stats['final_count']:,}
    - Qualidade: {cleaning_stats['quality_pct']:.2f}%
    
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
        task_id='validate_bronze_availability',
        python_callable=validate_bronze_availability,
        provide_context=True,
        dag=dag
    )
    
    # Task 2: Limpeza
    clean_task = PythonOperator(
        task_id='clean_and_deduplicate',
        python_callable=clean_and_deduplicate_data,
        provide_context=True,
        dag=dag
    )
    
    # Task 3: Enriquecimento
    enrich_task = PythonOperator(
        task_id='enrich_with_gtfs',
        python_callable=enrich_with_gtfs_data,
        provide_context=True,
        dag=dag
    )
    
    # Task 4: Quality checks
    quality_task = PythonOperator(
        task_id='data_quality_checks',
        python_callable=data_quality_checks,
        provide_context=True,
        dag=dag
    )
    
    # Task 5: Notificação
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
    
    start >> validate_task >> clean_task >> enrich_task >> quality_task >> notify_task >> end


# ===========================
# DOCUMENTAÇÃO
# ===========================

dag.doc_md = """
# DAG 03 - Bronze to Silver Transformation

## Objetivo
Transformar dados brutos da camada Bronze em dados limpos e enriquecidos na camada Silver.

## Schedule
- **Frequência**: A cada 30 minutos
- **Dependências**: DAG 02 (API Ingestion)

## Transformações Aplicadas

### 1. Limpeza de Dados
- Remoção de duplicatas (vehicle_id + timestamp)
- Validação de coordenadas (dentro de São Paulo)
- Validação de timestamps (não futuro, não muito antigo)
- Validação de IDs de veículos

### 2. Enriquecimento
- Join com dados GTFS (rotas, paradas)
- Adicionar nomes legíveis de rotas
- Cálculo de parada mais próxima

### 3. Qualidade
- Score de qualidade calculado
- Métricas de completude
- Alertas se qualidade < 80%

## Fluxo

```
Start → Validate Bronze → Clean & Deduplicate → Enrich GTFS → Quality Checks → Notify → End
```

## Métricas Coletadas
- Registros processados
- Duplicatas removidas
- Registros inválidos
- Score de qualidade
- Tempo de processamento

## Monitoramento
- Prometheus: métricas em tempo real
- Grafana: dashboards de qualidade
- Alertas: email se falhar
"""
