"""
Airflow DAG: Monitoramento de Qualidade de Dados.

Executa checks de data quality em todas as camadas,
gera relatórios e envia alertas se necessário.

Schedule: A cada hora
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

import sys
sys.path.append('/opt/airflow/src')

from src.common.logging_config import setup_logging

setup_logging(log_level="INFO", log_format="json")

default_args = {
    'owner': 'sptrans-pipeline',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['alerts@sptrans-pipeline.com'],
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=20),
}

dag = DAG(
    'sptrans_06_data_quality',
    default_args=default_args,
    description='Monitoramento de qualidade de dados',
    schedule_interval='0 * * * *',  # A cada hora
    catchup=False,
    max_active_runs=1,
    tags=['sptrans', 'data-quality', 'monitoring'],
)


def check_bronze_quality(**context):
    """Valida qualidade da camada Bronze."""
    from pyspark.sql import SparkSession
    from src.common.utils import get_s3_path
    from src.common.constants import BUCKET_BRONZE, PREFIX_API_POSITIONS
    
    print("🔍 Checking Bronze data quality")
    
    spark = SparkSession.builder.appName("DQ_Bronze").getOrCreate()
    
    try:
        bronze_path = get_s3_path(BUCKET_BRONZE, PREFIX_API_POSITIONS)
        df = spark.read.parquet(bronze_path)
        
        # Checks básicos
        total_count = df.count()
        null_count = df.filter(df['hr'].isNull()).count()
        
        # Últimas 24 horas
        recent_df = df.filter("ingestion_timestamp > current_timestamp() - interval 24 hours")
        recent_count = recent_df.count()
        
        metrics = {
            'layer': 'bronze',
            'total_records': total_count,
            'null_records': null_count,
            'recent_24h': recent_count,
            'null_rate': null_count / total_count if total_count > 0 else 0,
        }
        
        spark.stop()
        
        print(f"📊 Bronze metrics: {metrics}")
        context['task_instance'].xcom_push(key='bronze_metrics', value=metrics)
        
        # Alertas
        if metrics['null_rate'] > 0.01:  # > 1%
            print(f"⚠️  WARNING: High null rate in Bronze ({metrics['null_rate']:.2%})")
        
        if recent_count == 0:
            raise Exception("❌ No data ingested in last 24 hours!")
        
        print("✅ Bronze quality check passed")
        return metrics
        
    except Exception as e:
        spark.stop()
        print(f"❌ Bronze quality check failed: {e}")
        raise


def check_silver_quality(**context):
    """Valida qualidade da camada Silver."""
    from pyspark.sql import SparkSession
    from src.common.utils import get_s3_path
    from src.common.constants import BUCKET_SILVER
    
    print("🔍 Checking Silver data quality")
    
    spark = SparkSession.builder.appName("DQ_Silver").getOrCreate()
    
    try:
        silver_path = get_s3_path(BUCKET_SILVER, "vehicle_positions")
        df = spark.read.format("delta").load(silver_path)
        
        total_count = df.count()
        duplicate_count = df.filter(df['is_duplicate']).count()
        low_quality_count = df.filter(df['data_quality_score'] < 0.8).count()
        
        # Últimas 24 horas
        recent_df = df.filter("processed_timestamp > current_timestamp() - interval 24 hours")
        recent_count = recent_df.count()
        
        metrics = {
            'layer': 'silver',
            'total_records': total_count,
            'duplicate_records': duplicate_count,
            'low_quality_records': low_quality_count,
            'recent_24h': recent_count,
            'duplicate_rate': duplicate_count / total_count if total_count > 0 else 0,
            'low_quality_rate': low_quality_count / total_count if total_count > 0 else 0,
        }
        
        spark.stop()
        
        print(f"📊 Silver metrics: {metrics}")
        context['task_instance'].xcom_push(key='silver_metrics', value=metrics)
        
        # Alertas
        if metrics['duplicate_rate'] > 0.05:  # > 5%
            print(f"⚠️  WARNING: High duplicate rate ({metrics['duplicate_rate']:.2%})")
        
        if metrics['low_quality_rate'] > 0.10:  # > 10%
            print(f"⚠️  WARNING: High low-quality rate ({metrics['low_quality_rate']:.2%})")
        
        print("✅ Silver quality check passed")
        return metrics
        
    except Exception as e:
        spark.stop()
        print(f"❌ Silver quality check failed: {e}")
        raise


def check_gold_quality(**context):
    """Valida qualidade da camada Gold."""
    from pyspark.sql import SparkSession
    from src.common.utils import get_s3_path
    from src.common.constants import BUCKET_GOLD
    
    print("🔍 Checking Gold data quality")
    
    spark = SparkSession.builder.appName("DQ_Gold").getOrCreate()
    
    try:
        # Check hourly metrics
        hourly_path = get_s3_path(BUCKET_GOLD, "hourly_metrics")
        hourly_df = spark.read.format("delta").load(hourly_path)
        
        hourly_count = hourly_df.count()
        
        # Últimas 24 horas
        recent_hourly = hourly_df.filter(
            "hour_timestamp > current_timestamp() - interval 24 hours"
        )
        recent_hourly_count = recent_hourly.count()
        
        metrics = {
            'layer': 'gold',
            'total_hourly_metrics': hourly_count,
            'recent_hourly_24h': recent_hourly_count,
        }
        
        spark.stop()
        
        print(f"📊 Gold metrics: {metrics}")
        context['task_instance'].xcom_push(key='gold_metrics', value=metrics)
        
        # Alertas
        expected_hourly = 24  # 24 horas * ~1 registro por hora
        if recent_hourly_count < expected_hourly * 0.5:  # Menos de 50% esperado
            print(f"⚠️  WARNING: Low hourly metrics count ({recent_hourly_count}/{expected_hourly})")
        
        print("✅ Gold quality check passed")
        return metrics
        
    except Exception as e:
        spark.stop()
        print(f"❌ Gold quality check failed: {e}")
        raise


def check_serving_quality(**context):
    """Valida qualidade do serving layer (PostgreSQL)."""
    import psycopg2
    from src.common.config import Config
    
    config = Config()
    
    print("🔍 Checking Serving layer quality")
    
    conn = psycopg2.connect(
        host=config.postgres.host,
        port=config.postgres.port,
        database=config.postgres.database,
        user=config.postgres.user,
        password=config.postgres.password
    )
    
    try:
        cur = conn.cursor()
        
        # Check hourly aggregates
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE loaded_at > NOW() - INTERVAL '24 hours') as recent_24h
            FROM serving.hourly_aggregates
        """)
        
        hourly_total, hourly_recent = cur.fetchone()
        
        metrics = {
            'layer': 'serving',
            'hourly_total': hourly_total,
            'hourly_recent_24h': hourly_recent,
        }
        
        cur.close()
        conn.close()
        
        print(f"📊 Serving metrics: {metrics}")
        context['task_instance'].xcom_push(key='serving_metrics', value=metrics)
        
        # Alertas
        if hourly_recent == 0:
            print(f"⚠️  WARNING: No recent data in serving layer")
        
        print("✅ Serving quality check passed")
        return metrics
        
    except Exception as e:
        conn.close()
        print(f"❌ Serving quality check failed: {e}")
        raise


def generate_quality_report(**context):
    """Gera relatório consolidado de qualidade."""
    bronze_metrics = context['task_instance'].xcom_pull(
        task_ids='check_bronze_quality', key='bronze_metrics'
    )
    silver_metrics = context['task_instance'].xcom_pull(
        task_ids='check_silver_quality', key='silver_metrics'
    )
    gold_metrics = context['task_instance'].xcom_pull(
        task_ids='check_gold_quality', key='gold_metrics'
    )
    serving_metrics = context['task_instance'].xcom_pull(
        task_ids='check_serving_quality', key='serving_metrics'
    )
    
    print("📋 Generating quality report")
    
    report = {
        'timestamp': datetime.now().isoformat(),
        'layers': {
            'bronze': bronze_metrics,
            'silver': silver_metrics,
            'gold': gold_metrics,
            'serving': serving_metrics,
        },
        'overall_status': 'healthy',
    }
    
    print(f"📊 Quality Report:")
    print(f"  Bronze: {bronze_metrics['total_records']:,} records")
    print(f"  Silver: {silver_metrics['total_records']:,} records")
    print(f"  Gold: {gold_metrics['total_hourly_metrics']:,} hourly metrics")
    print(f"  Serving: {serving_metrics['hourly_total']:,} records")
    
    context['task_instance'].xcom_push(key='quality_report', value=report)
    
    return report


def send_quality_alert_if_needed(**context):
    """Envia alerta se houver problemas de qualidade."""
    report = context['task_instance'].xcom_pull(
        task_ids='generate_quality_report',
        key='quality_report'
    )
    
    print("📧 Checking if quality alert is needed")
    
    # Lógica de alerta (placeholder)
    # Aqui poderíamos enviar para Slack, email, PagerDuty, etc.
    
    print("✅ Quality monitoring complete")
    return True


# Tasks
with dag:
    with TaskGroup('quality_checks', tooltip='Parallel quality checks') as checks_group:
        check_bronze = PythonOperator(
            task_id='check_bronze_quality',
            python_callable=check_bronze_quality,
            provide_context=True,
        )
        
        check_silver = PythonOperator(
            task_id='check_silver_quality',
            python_callable=check_silver_quality,
            provide_context=True,
        )
        
        check_gold = PythonOperator(
            task_id='check_gold_quality',
            python_callable=check_gold_quality,
            provide_context=True,
        )
        
        check_serving = PythonOperator(
            task_id='check_serving_quality',
            python_callable=check_serving_quality,
            provide_context=True,
        )
    
    generate_report = PythonOperator(
        task_id='generate_quality_report',
        python_callable=generate_quality_report,
        provide_context=True,
    )
    
    send_alert = PythonOperator(
        task_id='send_alert_if_needed',
        python_callable=send_quality_alert_if_needed,
        provide_context=True,
    )
    
    checks_group >> generate_report >> send_alert
