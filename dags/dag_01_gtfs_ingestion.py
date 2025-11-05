"""
Airflow DAG: Ingestão GTFS (Diária).

Executa download e ingestão dos dados estáticos GTFS
da SPTrans para a camada Bronze.

Schedule: Diário às 02:00 (horário que SPTrans atualiza GTFS)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.task_group import TaskGroup

# Imports do projeto
import sys
sys.path.append('/opt/airflow/src')

from src.processing.jobs.ingest_gtfs_to_bronze import run_gtfs_to_bronze_job
from src.common.logging_config import setup_logging

# Setup logging
setup_logging(log_level="INFO", log_format="json")

# Configuração padrão
default_args = {
    'owner': 'sptrans-pipeline',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['alerts@sptrans-pipeline.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

# DAG
dag = DAG(
    'sptrans_01_gtfs_ingestion',
    default_args=default_args,
    description='Ingestão diária de dados GTFS estáticos',
    schedule_interval='0 2 * * *',  # Diário às 02:00
    catchup=False,
    max_active_runs=1,
    tags=['sptrans', 'bronze', 'gtfs', 'daily'],
)


def check_gtfs_availability(**context):
    """Verifica se GTFS está disponível para download."""
    import requests
    from src.common.constants import SPTRANS_GTFS_URL
    
    try:
        response = requests.head(SPTRANS_GTFS_URL, timeout=10)
        if response.status_code == 200:
            print("✅ GTFS available for download")
            return True
        else:
            raise Exception(f"GTFS not available: {response.status_code}")
    except Exception as e:
        print(f"❌ Error checking GTFS availability: {e}")
        raise


def ingest_gtfs_to_bronze(**context):
    """Executa ingestão GTFS para Bronze."""
    # Forçar download apenas às segundas-feiras (dados atualizam semanalmente)
    execution_date = context['execution_date']
    force_download = execution_date.weekday() == 0  # Segunda-feira = 0
    
    print(f"🔄 Starting GTFS ingestion (force_download={force_download})")
    
    stats = run_gtfs_to_bronze_job(force_download=force_download)
    
    print(f"✅ GTFS ingestion completed successfully")
    print(f"📊 Stats: {stats}")
    
    # Salvar stats no XCom para próximas tasks
    context['task_instance'].xcom_push(key='gtfs_stats', value=stats)
    
    return stats


def validate_gtfs_data(**context):
    """Valida dados GTFS ingeridos."""
    stats = context['task_instance'].xcom_pull(
        task_ids='ingest_gtfs', 
        key='gtfs_stats'
    )
    
    print("🔍 Validating GTFS data")
    
    # Validações básicas
    if stats['failed_files'] > 0:
        raise Exception(f"❌ {stats['failed_files']} files failed to process")
    
    if stats['total_records'] == 0:
        raise Exception("❌ No records ingested")
    
    # Verificar arquivos mínimos obrigatórios
    required_files = ['stops', 'routes', 'trips', 'stop_times']
    results = stats.get('results_by_file', {})
    
    for req_file in required_files:
        if req_file not in results:
            raise Exception(f"❌ Required file missing: {req_file}")
        
        if results[req_file].get('status') != 'success':
            raise Exception(f"❌ File {req_file} failed to process")
        
        if results[req_file].get('record_count', 0) == 0:
            raise Exception(f"❌ File {req_file} has zero records")
    
    print("✅ GTFS data validation passed")
    return True


def send_success_notification(**context):
    """Envia notificação de sucesso."""
    stats = context['task_instance'].xcom_pull(
        task_ids='ingest_gtfs',
        key='gtfs_stats'
    )
    
    message = f"""
    ✅ *GTFS Ingestion Successful*
    
    📊 *Statistics:*
    • Files processed: {stats['total_files_processed']}
    • Total records: {stats['total_records']:,}
    • Success: {stats['successful_files']}
    • Failed: {stats['failed_files']}
    
    🕐 *Execution:* {context['execution_date'].strftime('%Y-%m-%d %H:%M:%S')}
    """
    
    print(message)
    return message


def send_failure_notification(**context):
    """Envia notificação de falha."""
    exception = context.get('exception')
    
    message = f"""
    ❌ *GTFS Ingestion Failed*
    
    🔥 *Error:* {str(exception)}
    
    🕐 *Execution:* {context['execution_date'].strftime('%Y-%m-%d %H:%M:%S')}
    
    📋 *DAG:* {context['dag'].dag_id}
    📝 *Task:* {context['task'].task_id}
    """
    
    print(message)
    return message


# Tasks
with dag:
    # 1. Verificar disponibilidade
    check_availability = PythonOperator(
        task_id='check_gtfs_availability',
        python_callable=check_gtfs_availability,
        provide_context=True,
    )
    
    # 2. Ingerir GTFS
    ingest_gtfs = PythonOperator(
        task_id='ingest_gtfs',
        python_callable=ingest_gtfs_to_bronze,
        provide_context=True,
    )
    
    # 3. Validar dados
    validate_data = PythonOperator(
        task_id='validate_gtfs_data',
        python_callable=validate_gtfs_data,
        provide_context=True,
    )
    
    # 4. Notificação de sucesso
    success_notification = PythonOperator(
        task_id='send_success_notification',
        python_callable=send_success_notification,
        provide_context=True,
        trigger_rule='all_success',
    )
    
    # 5. Notificação de falha
    failure_notification = PythonOperator(
        task_id='send_failure_notification',
        python_callable=send_failure_notification,
        provide_context=True,
        trigger_rule='one_failed',
    )
    
    # Fluxo
    check_availability >> ingest_gtfs >> validate_data >> success_notification
    [check_availability, ingest_gtfs, validate_data] >> failure_notification
