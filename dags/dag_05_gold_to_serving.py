"""
Airflow DAG: Load Gold → Serving (PostgreSQL).

Carrega dados agregados da camada Gold para PostgreSQL
que serve como serving layer para dashboards.

Schedule: A cada 10 minutos
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

import sys
sys.path.append('/opt/airflow/src')

from src.processing.jobs.gold_to_postgres import run_gold_to_postgres_job
from src.common.logging_config import setup_logging

setup_logging(log_level="INFO", log_format="json")

default_args = {
    'owner': 'sptrans-pipeline',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['alerts@sptrans-pipeline.com'],
    'email_on_failure': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=10),
}

dag = DAG(
    'sptrans_05_gold_to_serving',
    default_args=default_args,
    description='Load Gold → PostgreSQL Serving Layer',
    schedule_interval='*/10 * * * *',  # A cada 10 minutos
    catchup=False,
    max_active_runs=1,
    tags=['sptrans', 'serving', 'postgres', 'load'],
)


def load_hourly_metrics(**context):
    """Carrega métricas horárias no PostgreSQL."""
    execution_date = context['execution_date']
    date = execution_date.strftime('%Y-%m')  # Year-month
    
    print(f"💾 Loading hourly metrics to PostgreSQL ({date})")
    
    stats = run_gold_to_postgres_job(
        data_type='hourly_metrics',
        date=date,
        mode='append'
    )
    
    print(f"✅ Loaded {stats['input_count']:,} hourly records")
    context['task_instance'].xcom_push(key='hourly_load_stats', value=stats)
    
    return stats


def load_daily_summary(**context):
    """Carrega sumário diário no PostgreSQL."""
    execution_date = context['execution_date']
    date = execution_date.strftime('%Y-%m')
    
    print(f"💾 Loading daily summary to PostgreSQL ({date})")
    
    stats = run_gold_to_postgres_job(
        data_type='daily_summary',
        date=date,
        mode='append'
    )
    
    print(f"✅ Loaded {stats['input_count']:,} daily records")
    context['task_instance'].xcom_push(key='daily_load_stats', value=stats)
    
    return stats


def load_line_performance(**context):
    """Carrega performance de linhas no PostgreSQL."""
    execution_date = context['execution_date']
    date = execution_date.strftime('%Y-%m')
    
    print(f"💾 Loading line performance to PostgreSQL ({date})")
    
    stats = run_gold_to_postgres_job(
        data_type='line_performance',
        date=date,
        mode='append'
    )
    
    print(f"✅ Loaded {stats['input_count']:,} performance records")
    context['task_instance'].xcom_push(key='performance_load_stats', value=stats)
    
    return stats


def refresh_materialized_views(**context):
    """Atualiza materialized views no PostgreSQL."""
    import psycopg2
    from src.common.config import Config
    
    config = Config()
    
    print("🔄 Refreshing materialized views")
    
    conn = psycopg2.connect(
        host=config.postgres.host,
        port=config.postgres.port,
        database=config.postgres.database,
        user=config.postgres.user,
        password=config.postgres.password
    )
    
    try:
        cur = conn.cursor()
        
        # Lista de views a atualizar
        views = [
            'serving.mv_lines_current',
            'serving.mv_fleet_summary',
        ]
        
        for view in views:
            print(f"  Refreshing {view}...")
            cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}")
            conn.commit()
        
        cur.close()
        conn.close()
        
        print(f"✅ Refreshed {len(views)} materialized views")
        
    except Exception as e:
        conn.close()
        print(f"❌ Error refreshing views: {e}")
        raise


def validate_serving_data(**context):
    """Valida dados no serving layer."""
    import psycopg2
    from src.common.config import Config
    
    config = Config()
    
    print("🔍 Validating serving layer data")
    
    conn = psycopg2.connect(
        host=config.postgres.host,
        port=config.postgres.port,
        database=config.postgres.database,
        user=config.postgres.user,
        password=config.postgres.password
    )
    
    try:
        cur = conn.cursor()
        
        # Check record counts
        tables = [
            'serving.hourly_aggregates',
            'serving.daily_aggregates',
            'serving.lines_metrics',
        ]
        
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            print(f"  {table}: {count:,} records")
            
            if count == 0:
                print(f"⚠️  WARNING: {table} is empty")
        
        cur.close()
        conn.close()
        
        print("✅ Serving layer validation complete")
        
    except Exception as e:
        conn.close()
        print(f"❌ Validation error: {e}")
        raise


# Tasks
with dag:
    with TaskGroup('load_tasks', tooltip='Parallel data loads') as load_group:
        load_hourly = PythonOperator(
            task_id='load_hourly_metrics',
            python_callable=load_hourly_metrics,
            provide_context=True,
        )
        
        load_daily = PythonOperator(
            task_id='load_daily_summary',
            python_callable=load_daily_summary,
            provide_context=True,
        )
        
        load_performance = PythonOperator(
            task_id='load_line_performance',
            python_callable=load_line_performance,
            provide_context=True,
        )
    
    refresh_views = PythonOperator(
        task_id='refresh_materialized_views',
        python_callable=refresh_materialized_views,
        provide_context=True,
    )
    
    validate = PythonOperator(
        task_id='validate_serving_data',
        python_callable=validate_serving_data,
        provide_context=True,
    )
    
    load_group >> refresh_views >> validate
