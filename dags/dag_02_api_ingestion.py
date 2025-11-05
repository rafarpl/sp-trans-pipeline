"""
Airflow DAG: Ingestão API SPTrans (Near Real-Time).

Executa ingestão de posições de veículos da API Olho Vivo
para a camada Bronze.

Schedule: A cada 3 minutos (requisito: near real-time ~2min)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup

import sys
sys.path.append('/opt/airflow/src')

from src.processing.jobs.ingest_api_to_bronze import run_api_to_bronze_job
from src.common.logging_config import setup_logging

setup_logging(log_level="INFO", log_format="json")

# Configuração
default_args = {
    'owner': 'sptrans-pipeline',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['alerts@sptrans-pipeline.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
    'execution_timeout': timedelta(minutes=2),
}

dag = DAG(
    'sptrans_02_api_ingestion',
    default_args=default_args,
    description='Ingestão near real-time de posições de veículos',
    schedule_interval='*/3 * * * *',  # A cada 3 minutos
    catchup=False,
    max_active_runs=1,  # Evitar sobreposição
    tags=['sptrans', 'bronze', 'api', 'realtime'],
)


def check_api_health(**context):
    """Verifica saúde da API antes de ingerir."""
    from src.ingestion.sptrans_api_client import SPTransAPIClient
    
    print("🏥 Checking API health")
    
    try:
        with SPTransAPIClient() as client:
            # Tentar autenticar
            client.authenticate()
            print("✅ API is healthy and authenticated")
            return True
    except Exception as e:
        print(f"❌ API health check failed: {e}")
        raise


def ingest_positions_to_bronze(**context):
    """Ingere posições de veículos para Bronze."""
    print("🚌 Starting API ingestion (all vehicles)")
    
    stats = run_api_to_bronze_job(
        line_id=None,  # Todas as linhas
        save_mode='append'
    )
    
    print(f"✅ API ingestion completed")
    print(f"📊 Records ingested: {stats['record_count']:,}")
    
    # Salvar stats
    context['task_instance'].xcom_push(key='api_stats', value=stats)
    
    return stats


def monitor_ingestion_quality(**context):
    """Monitora qualidade da ingestão."""
    stats = context['task_instance'].xcom_pull(
        task_ids='ingest_positions',
        key='api_stats'
    )
    
    print("🔍 Monitoring ingestion quality")
    
    record_count = stats.get('record_count', 0)
    
    # Alertas
    if record_count == 0:
        print("⚠️  WARNING: Zero records ingested!")
        # Não falhar - API pode estar temporariamente vazia
    elif record_count < 100:
        print(f"⚠️  WARNING: Low record count ({record_count})")
    else:
        print(f"✅ Quality check passed ({record_count:,} records)")
    
    return True


def calculate_pipeline_metrics(**context):
    """Calcula métricas do pipeline."""
    from datetime import datetime, timedelta
    
    stats = context['task_instance'].xcom_pull(
        task_ids='ingest_positions',
        key='api_stats'
    )
    
    execution_date = context['execution_date']
    
    # Métricas
    metrics = {
        'timestamp': datetime.now().isoformat(),
        'execution_date': execution_date.isoformat(),
        'records_ingested': stats.get('record_count', 0),
        'output_path': stats.get('output_path'),
        'format': stats.get('format'),
    }
    
    print(f"📊 Pipeline metrics: {metrics}")
    
    # Salvar para dashboard
    context['task_instance'].xcom_push(key='pipeline_metrics', value=metrics)
    
    return metrics


def trigger_downstream_if_needed(**context):
    """Trigger pipeline downstream se necessário."""
    stats = context['task_instance'].xcom_pull(
        task_ids='ingest_positions',
        key='api_stats'
    )
    
    record_count = stats.get('record_count', 0)
    
    # Trigger Bronze → Silver se houver dados suficientes
    # (implementar TriggerDagRunOperator se necessário)
    
    if record_count > 0:
        print(f"✅ {record_count:,} records ready for downstream processing")
        return True
    else:
        print("⏭️  Skipping downstream trigger (no records)")
        return False


# Tasks
with dag:
    # 1. Health check
    health_check = PythonOperator(
        task_id='check_api_health',
        python_callable=check_api_health,
        provide_context=True,
    )
    
    # 2. Ingestão
    ingest_positions = PythonOperator(
        task_id='ingest_positions',
        python_callable=ingest_positions_to_bronze,
        provide_context=True,
    )
    
    # 3. Monitoramento de qualidade
    quality_monitor = PythonOperator(
        task_id='monitor_quality',
        python_callable=monitor_ingestion_quality,
        provide_context=True,
    )
    
    # 4. Métricas
    calculate_metrics = PythonOperator(
        task_id='calculate_metrics',
        python_callable=calculate_pipeline_metrics,
        provide_context=True,
    )
    
    # 5. Trigger downstream
    trigger_downstream = PythonOperator(
        task_id='trigger_downstream',
        python_callable=trigger_downstream_if_needed,
        provide_context=True,
    )
    
    # Fluxo
    health_check >> ingest_positions >> [quality_monitor, calculate_metrics]
    [quality_monitor, calculate_metrics] >> trigger_downstream
