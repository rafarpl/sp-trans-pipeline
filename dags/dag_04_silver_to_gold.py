"""
Airflow DAG: Agregações Silver → Gold.

Calcula KPIs e agregações de negócio dos dados Silver
para a camada Gold (métricas horárias, diárias, performance).

Schedule: A cada 15 minutos
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

import sys
sys.path.append('/opt/airflow/src')

from src.processing.jobs.silver_to_gold import run_silver_to_gold_job
from src.common.logging_config import setup_logging

setup_logging(log_level="INFO", log_format="json")

default_args = {
    'owner': 'sptrans-pipeline',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['alerts@sptrans-pipeline.com'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(minutes=15),
}

dag = DAG(
    'sptrans_04_silver_to_gold',
    default_args=default_args,
    description='Agregações e KPIs Silver → Gold',
    schedule_interval='*/15 * * * *',  # A cada 15 minutos
    catchup=False,
    max_active_runs=1,
    tags=['sptrans', 'gold', 'aggregation', 'kpi'],
)


def aggregate_hourly_metrics(**context):
    """Agrega métricas horárias."""
    execution_date = context['execution_date']
    date = execution_date.strftime('%Y-%m-%d')
    
    print(f"📊 Aggregating hourly metrics for {date}")
    
    stats = run_silver_to_gold_job(
        date=date,
        aggregation_type='hourly'
    )
    
    print(f"✅ Hourly aggregation completed: {stats['output_count']:,} records")
    context['task_instance'].xcom_push(key='hourly_stats', value=stats)
    
    return stats


def aggregate_line_performance(**context):
    """Agrega performance por linha."""
    execution_date = context['execution_date']
    date = execution_date.strftime('%Y-%m-%d')
    
    print(f"📈 Aggregating line performance for {date}")
    
    stats = run_silver_to_gold_job(
        date=date,
        aggregation_type='line_performance'
    )
    
    print(f"✅ Line performance aggregation completed: {stats['output_count']:,} records")
    context['task_instance'].xcom_push(key='performance_stats', value=stats)
    
    return stats


def validate_gold_metrics(**context):
    """Valida métricas Gold."""
    hourly_stats = context['task_instance'].xcom_pull(
        task_ids='aggregate_hourly_metrics',
        key='hourly_stats'
    )
    
    print("🔍 Validating Gold metrics")
    
    if hourly_stats and hourly_stats['output_count'] == 0:
        print("⚠️  WARNING: No hourly metrics generated")
    else:
        print(f"✅ Validation passed")
    
    return True


# Tasks
with dag:
    with TaskGroup('aggregation_tasks', tooltip='Parallel aggregations') as aggregation_group:
        hourly = PythonOperator(
            task_id='aggregate_hourly_metrics',
            python_callable=aggregate_hourly_metrics,
            provide_context=True,
        )
        
        performance = PythonOperator(
            task_id='aggregate_line_performance',
            python_callable=aggregate_line_performance,
            provide_context=True,
        )
    
    validate = PythonOperator(
        task_id='validate_metrics',
        python_callable=validate_gold_metrics,
        provide_context=True,
    )
    
    aggregation_group >> validate
