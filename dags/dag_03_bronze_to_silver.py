"""
Airflow DAG: Transformação Bronze → Silver.

Processa dados brutos da camada Bronze, aplicando limpeza,
normalização e validações para a camada Silver.

Schedule: A cada 5 minutos (após ingestão API)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor

import sys
sys.path.append('/opt/airflow/src')

from src.processing.jobs.bronze_to_silver import run_bronze_to_silver_job
from src.common.logging_config import setup_logging

setup_logging(log_level="INFO", log_format="json")

default_args = {
    'owner': 'sptrans-pipeline',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['alerts@sptrans-pipeline.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=10),
}

dag = DAG(
    'sptrans_03_bronze_to_silver',
    default_args=default_args,
    description='Limpeza e normalização Bronze → Silver',
    schedule_interval='*/5 * * * *',  # A cada 5 minutos
    catchup=False,
    max_active_runs=2,  # Permitir paralelização limitada
    tags=['sptrans', 'silver', 'transformation', 'cleaning'],
)


def check_bronze_data_availability(**context):
    """Verifica se há dados no Bronze para processar."""
    from pyspark.sql import SparkSession
    from src.common.utils import get_s3_path
    from src.common.constants import BUCKET_BRONZE, PREFIX_API_POSITIONS
    
    print("🔍 Checking Bronze data availability")
    
    # Criar Spark session
    spark = SparkSession.builder.appName("Bronze_Check").getOrCreate()
    
    try:
        bronze_path = get_s3_path(BUCKET_BRONZE, PREFIX_API_POSITIONS)
        
        # Tentar ler última hora
        current_hour = context['execution_date'].hour
        
        df = spark.read.parquet(bronze_path).filter(
            f"hour = {current_hour}"
        )
        
        count = df.count()
        
        spark.stop()
        
        if count > 0:
            print(f"✅ Found {count:,} records in Bronze")
            context['task_instance'].xcom_push(key='bronze_count', value=count)
            return 'transform_bronze_to_silver'
        else:
            print("⏭️  No data in Bronze, skipping")
            return 'skip_transformation'
            
    except Exception as e:
        print(f"⚠️  Error checking Bronze: {e}")
        spark.stop()
        return 'skip_transformation'


def transform_bronze_to_silver(**context):
    """Executa transformação Bronze → Silver."""
    execution_date = context['execution_date']
    date = execution_date.strftime('%Y-%m-%d')
    hour = execution_date.hour
    
    print(f"🔄 Starting Bronze → Silver transformation")
    print(f"📅 Date: {date}, Hour: {hour}")
    
    stats = run_bronze_to_silver_job(
        date=date,
        hour=hour
    )
    
    print(f"✅ Transformation completed")
    print(f"📊 Stats: {stats}")
    
    # Salvar stats
    context['task_instance'].xcom_push(key='transform_stats', value=stats)
    
    return stats


def validate_silver_quality(**context):
    """Valida qualidade dos dados Silver."""
    stats = context['task_instance'].xcom_pull(
        task_ids='transform_bronze_to_silver',
        key='transform_stats'
    )
    
    print("🔍 Validating Silver data quality")
    
    # Validações
    input_count = stats.get('input_count', 0)
    output_count = stats.get('output_count', 0)
    dq_results = stats.get('dq_results', {})
    
    # Check 1: Records processed
    if output_count == 0:
        raise Exception("❌ Zero records in output")
    
    # Check 2: Data loss check (não deve perder mais de 50%)
    if output_count < input_count * 0.5:
        print(f"⚠️  WARNING: Significant data loss ({output_count}/{input_count})")
    
    # Check 3: Data quality
    dq_status = dq_results.get('overall_status', 'unknown')
    if dq_status == 'failed':
        raise Exception("❌ Data quality validation failed")
    elif dq_status == 'warning':
        print(f"⚠️  WARNING: Data quality issues detected")
    
    print(f"✅ Quality validation passed")
    print(f"   Input: {input_count:,}")
    print(f"   Output: {output_count:,}")
    print(f"   Retention: {(output_count/input_count*100):.1f}%")
    print(f"   DQ Status: {dq_status}")
    
    return True


def calculate_transformation_metrics(**context):
    """Calcula métricas da transformação."""
    stats = context['task_instance'].xcom_pull(
        task_ids='transform_bronze_to_silver',
        key='transform_stats'
    )
    
    if not stats:
        print("⏭️  No stats available")
        return None
    
    input_count = stats.get('input_count', 0)
    output_count = stats.get('output_count', 0)
    dq_results = stats.get('dq_results', {})
    
    metrics = {
        'timestamp': datetime.now().isoformat(),
        'input_records': input_count,
        'output_records': output_count,
        'retention_rate': output_count / input_count if input_count > 0 else 0,
        'dq_status': dq_results.get('overall_status', 'unknown'),
        'validations': len(dq_results.get('validations', {})),
    }
    
    print(f"📊 Transformation metrics: {metrics}")
    
    context['task_instance'].xcom_push(key='metrics', value=metrics)
    
    return metrics


def update_silver_statistics(**context):
    """Atualiza estatísticas da camada Silver."""
    print("📈 Updating Silver layer statistics")
    
    # Aqui poderíamos:
    # - Atualizar tabela de controle no PostgreSQL
    # - Enviar métricas para Prometheus Pushgateway
    # - Atualizar dashboard em tempo real
    
    stats = context['task_instance'].xcom_pull(
        task_ids='transform_bronze_to_silver',
        key='transform_stats'
    )
    
    if stats:
        print(f"✅ Statistics updated for Silver layer")
    
    return True


# Tasks
with dag:
    # 1. Verificar disponibilidade de dados
    check_data = BranchPythonOperator(
        task_id='check_bronze_availability',
        python_callable=check_bronze_data_availability,
        provide_context=True,
    )
    
    # 2a. Transformar (se houver dados)
    transform = PythonOperator(
        task_id='transform_bronze_to_silver',
        python_callable=transform_bronze_to_silver,
        provide_context=True,
    )
    
    # 2b. Skip (se não houver dados)
    skip = DummyOperator(
        task_id='skip_transformation',
    )
    
    # 3. Validar qualidade
    validate_quality = PythonOperator(
        task_id='validate_quality',
        python_callable=validate_silver_quality,
        provide_context=True,
        trigger_rule='none_failed',
    )
    
    # 4. Calcular métricas
    calculate_metrics = PythonOperator(
        task_id='calculate_metrics',
        python_callable=calculate_transformation_metrics,
        provide_context=True,
        trigger_rule='none_failed',
    )
    
    # 5. Atualizar estatísticas
    update_stats = PythonOperator(
        task_id='update_statistics',
        python_callable=update_silver_statistics,
        provide_context=True,
        trigger_rule='none_failed',
    )
    
    # Fluxo
    check_data >> [transform, skip]
    transform >> validate_quality >> calculate_metrics >> update_stats
    skip >> update_stats
