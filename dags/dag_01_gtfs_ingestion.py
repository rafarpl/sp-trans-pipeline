"""
DAG 01 - GTFS Data Ingestion
=============================
Ingestão diária dos arquivos GTFS (General Transit Feed Specification) da SPTrans.

Execução: Diariamente às 3:00 AM
Dados: routes, trips, stops, stop_times, shapes

Autor: Rafael - SPTrans Pipeline
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

import sys
import os

# Adicionar src ao path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.common.config import Config
from src.common.logging_config import get_logger
from src.ingestion.gtfs_downloader import GTFSDownloader
from src.processing.jobs.ingest_gtfs_to_bronze import GTFSToBronzeJob
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
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

dag = DAG(
    'dag_01_gtfs_ingestion',
    default_args=default_args,
    description='Ingestão diária de dados GTFS da SPTrans',
    schedule_interval='0 3 * * *',  # Diariamente às 3:00 AM
    catchup=False,
    max_active_runs=1,
    tags=['gtfs', 'ingestion', 'bronze', 'daily'],
)


# ===========================
# FUNÇÕES DAS TASKS
# ===========================

@track_job_execution('gtfs_download')
def download_gtfs_data(**context):
    """
    Task 1: Download dos arquivos GTFS do portal SPTrans.
    
    Returns:
        Dict com informações dos arquivos baixados
    """
    logger.info("Iniciando download dos dados GTFS")
    
    execution_date = context['execution_date']
    config = Config()
    
    downloader = GTFSDownloader(
        download_dir=config.GTFS_LOCAL_PATH,
        gtfs_url=config.GTFS_URL
    )
    
    # Download do arquivo ZIP
    zip_path = downloader.download()
    
    # Extrair arquivos
    extracted_files = downloader.extract(zip_path)
    
    logger.info(f"GTFS baixado com sucesso: {len(extracted_files)} arquivos")
    
    # Validar arquivos obrigatórios
    required_files = ['routes.txt', 'trips.txt', 'stops.txt', 'stop_times.txt', 'shapes.txt']
    missing_files = [f for f in required_files if f not in extracted_files]
    
    if missing_files:
        raise ValueError(f"Arquivos obrigatórios faltando: {missing_files}")
    
    # Push para XCom
    context['task_instance'].xcom_push(
        key='gtfs_files',
        value={
            'extracted_files': extracted_files,
            'download_date': execution_date.isoformat(),
            'file_count': len(extracted_files)
        }
    )
    
    # Reportar métricas
    reporter.report_processing_stats(
        layer='gtfs_download',
        source='sptrans',
        total=len(extracted_files),
        invalid=0,
        duplicated=0
    )
    
    return extracted_files


def validate_gtfs_files(**context):
    """
    Task 2: Validação básica dos arquivos GTFS.
    
    Verifica:
    - Formato dos arquivos
    - Campos obrigatórios
    - Integridade dos dados
    """
    logger.info("Validando arquivos GTFS")
    
    # Pegar informações do XCom
    ti = context['task_instance']
    gtfs_info = ti.xcom_pull(task_ids='download_gtfs', key='gtfs_files')
    
    config = Config()
    
    from src.common.validators import GTFSValidator
    from pyspark.sql import SparkSession
    
    # Inicializar Spark
    spark = SparkSession.builder \
        .appName("GTFS_Validation") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    validation_results = {}
    
    try:
        # Validar routes
        routes_df = spark.read.csv(
            f"{config.GTFS_LOCAL_PATH}/routes.txt",
            header=True,
            inferSchema=True
        )
        validation_results['routes'] = GTFSValidator.validate_gtfs_routes(routes_df)
        logger.info(f"Routes validado: {validation_results['routes']}")
        
        # Validar stops
        stops_df = spark.read.csv(
            f"{config.GTFS_LOCAL_PATH}/stops.txt",
            header=True,
            inferSchema=True
        )
        validation_results['stops'] = GTFSValidator.validate_gtfs_stops(stops_df)
        logger.info(f"Stops validado: {validation_results['stops']}")
        
        # Verificar se completude está aceitável (>90%)
        for file_type, results in validation_results.items():
            if results['completeness_pct'] < 90:
                raise ValueError(
                    f"Completude baixa para {file_type}: {results['completeness_pct']:.2f}%"
                )
        
        # Push para XCom
        ti.xcom_push(key='validation_results', value=validation_results)
        
        logger.info("Validação concluída com sucesso")
        
    finally:
        spark.stop()
    
    return validation_results


def ingest_to_bronze(**context):
    """
    Task 3: Ingestão dos dados GTFS para a camada Bronze (MinIO).
    
    Converte arquivos CSV para Parquet e salva no Data Lake.
    """
    logger.info("Iniciando ingestão GTFS para Bronze")
    
    execution_date = context['execution_date']
    config = Config()
    
    # Inicializar job
    job = GTFSToBronzeJob(
        input_path=config.GTFS_LOCAL_PATH,
        output_path=f"{config.BRONZE_LAYER_PATH}/gtfs",
        execution_date=execution_date
    )
    
    # Executar ingestão
    result = job.run()
    
    logger.info(f"Ingestão concluída: {result}")
    
    # Push para XCom
    context['task_instance'].xcom_push(key='ingestion_result', value=result)
    
    # Reportar métricas
    reporter.report_processing_stats(
        layer='bronze',
        source='gtfs',
        total=result.get('total_records', 0),
        invalid=0,
        duplicated=0
    )
    
    return result


def cleanup_temp_files(**context):
    """
    Task 4: Limpeza de arquivos temporários.
    """
    logger.info("Limpando arquivos temporários")
    
    config = Config()
    import shutil
    
    # Remover arquivos temporários (manter apenas últimos 7 dias)
    temp_dir = config.GTFS_LOCAL_PATH
    
    if os.path.exists(temp_dir):
        # Lista arquivos
        files = os.listdir(temp_dir)
        logger.info(f"Arquivos temporários encontrados: {len(files)}")
        
        # Aqui você pode implementar lógica de cleanup mais sofisticada
        # Por exemplo, manter apenas os últimos 7 dias
        
    logger.info("Cleanup concluído")


def send_success_notification(**context):
    """
    Task 5: Enviar notificação de sucesso.
    """
    logger.info("Enviando notificação de sucesso")
    
    ti = context['task_instance']
    execution_date = context['execution_date']
    
    # Coletar informações
    gtfs_info = ti.xcom_pull(task_ids='download_gtfs', key='gtfs_files')
    validation = ti.xcom_pull(task_ids='validate_gtfs', key='validation_results')
    ingestion = ti.xcom_pull(task_ids='ingest_to_bronze', key='ingestion_result')
    
    message = f"""
    ✅ DAG GTFS Ingestion Concluída com Sucesso
    
    Data de Execução: {execution_date}
    
    📥 Download:
    - Arquivos baixados: {gtfs_info.get('file_count', 0)}
    
    ✔️ Validação:
    - Routes: {validation.get('routes', {}).get('completeness_pct', 0):.2f}% completo
    - Stops: {validation.get('stops', {}).get('completeness_pct', 0):.2f}% completo
    
    💾 Ingestão Bronze:
    - Total de registros: {ingestion.get('total_records', 0)}
    - Arquivos Parquet criados: {ingestion.get('files_created', 0)}
    
    Status: SUCCESS ✅
    """
    
    logger.info(message)
    
    # Aqui você pode adicionar envio de email, Slack, etc.
    # from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
    
    return message


# ===========================
# DEFINIÇÃO DAS TASKS
# ===========================

with dag:
    
    # Task de início
    start = EmptyOperator(
        task_id='start',
        dag=dag
    )
    
    # Task 1: Download GTFS
    download_task = PythonOperator(
        task_id='download_gtfs',
        python_callable=download_gtfs_data,
        provide_context=True,
        dag=dag
    )
    
    # Task 2: Validação
    validate_task = PythonOperator(
        task_id='validate_gtfs',
        python_callable=validate_gtfs_files,
        provide_context=True,
        dag=dag
    )
    
    # Task 3: Ingestão para Bronze
    ingest_task = PythonOperator(
        task_id='ingest_to_bronze',
        python_callable=ingest_to_bronze,
        provide_context=True,
        dag=dag
    )
    
    # Task 4: Cleanup
    cleanup_task = PythonOperator(
        task_id='cleanup_temp_files',
        python_callable=cleanup_temp_files,
        provide_context=True,
        dag=dag
    )
    
    # Task 5: Notificação
    notify_task = PythonOperator(
        task_id='send_notification',
        python_callable=send_success_notification,
        provide_context=True,
        trigger_rule='all_success',
        dag=dag
    )
    
    # Task de fim
    end = EmptyOperator(
        task_id='end',
        trigger_rule='all_done',
        dag=dag
    )
    
    # ===========================
    # DEFINIÇÃO DO FLUXO
    # ===========================
    
    start >> download_task >> validate_task >> ingest_task >> cleanup_task >> notify_task >> end


# ===========================
# DOCUMENTAÇÃO DO DAG
# ===========================

dag.doc_md = """
# DAG 01 - GTFS Data Ingestion

## Objetivo
Realizar a ingestão diária dos dados GTFS (General Transit Feed Specification) da SPTrans 
para a camada Bronze do Data Lake.

## Schedule
- **Frequência**: Diária
- **Horário**: 3:00 AM (horário de menor tráfego)
- **Timezone**: America/Sao_Paulo

## Arquivos GTFS Processados
1. **routes.txt**: Informações sobre as linhas de ônibus
2. **trips.txt**: Viagens programadas para cada linha
3. **stops.txt**: Paradas de ônibus (com coordenadas geográficas)
4. **stop_times.txt**: Horários de chegada/saída em cada parada
5. **shapes.txt**: Traçado geográfico das rotas

## Fluxo de Execução

```
Start → Download GTFS → Validate → Ingest to Bronze → Cleanup → Notify → End
```

## Tasks

### 1. download_gtfs
- Download do arquivo ZIP do portal SPTrans
- Extração dos arquivos CSV
- Validação de arquivos obrigatórios

### 2. validate_gtfs
- Validação de formato e schema
- Verificação de campos obrigatórios
- Check de integridade (IDs, coordenadas)

### 3. ingest_to_bronze
- Conversão CSV → Parquet
- Particionamento por data
- Upload para MinIO (S3-compatible)

### 4. cleanup_temp_files
- Remoção de arquivos temporários
- Manutenção de diretórios

### 5. send_notification
- Envio de notificação de sucesso/falha
- Métricas de execução

## Monitoramento
- Métricas exportadas para Prometheus
- Logs detalhados em `/logs/airflow/`
- Alertas configurados para falhas

## Contato
- Equipe: SPTrans Data Team
- Email: alerts@sptrans-pipeline.com
"""
