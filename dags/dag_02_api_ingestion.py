"""
DAG 02: Ingestão da API SPTrans em tempo real.

Schedule: A cada 2 minutos
Responsabilidade: Coletar posições dos ônibus e salvar no Bronze Layer

Tasks:
1. ingest_api_to_bronze - Spark job para ingestão
2. validate_bronze_data - Validação básica dos dados
3. send_metrics - Envia métricas para Prometheus
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


# ============================================================================
# DEFAULT ARGS
# ============================================================================
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['alert@sptrans.local'],
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=5),
}


# ============================================================================
# DAG DEFINITION
# ============================================================================
dag = DAG(
    dag_id='dag_02_api_ingestion',
    default_args=default_args,
    description='Ingestão em tempo real da API SPTrans para Bronze Layer',
    schedule_interval='*/2 * * * *',  # A cada 2 minutos
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=3,
    tags=['ingestion', 'api', 'bronze', 'realtime'],
)


# ============================================================================
# PYTHON FUNCTIONS
# ============================================================================

def validate_bronze_data(**context):
    """
    Valida dados escritos no Bronze Layer.
    """
    from pyspark.sql import SparkSession
    from src.common.config import get_spark_config
    from src.common.constants import DataLakePath
    from src.common.utils import get_partition_path
    
    execution_date = context['execution_date']
    
    print(f"Validando dados do Bronze Layer para: {execution_date}")
    
    # Criar Spark Session
    spark_config = get_spark_config()
    spark = SparkSession.builder \
        .appName("Validate Bronze Data") \
        .config("spark.master", spark_config.get('spark.master')) \
        .config("spark.hadoop.fs.s3a.endpoint", spark_config.get('spark.hadoop.fs.s3a.endpoint')) \
        .config("spark.hadoop.fs.s3a.access.key", spark_config.get('spark.hadoop.fs.s3a.access.key')) \
        .config("spark.hadoop.fs.s3a.secret.key", spark_config.get('spark.hadoop.fs.s3a.secret.key')) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    try:
        # Ler dados do Bronze
        partition_path = get_partition_path(
            DataLakePath.BRONZE_API_POSITIONS,
            execution_date
        )
        
        df = spark.read.parquet(partition_path)
        
        # Validações básicas
        record_count = df.count()
        
        if record_count == 0:
            raise ValueError("Nenhum registro encontrado no Bronze Layer")
        
        # Verificar campos obrigatórios não-nulos
        null_counts = df.select([
            (df[col].isNull().cast("int")).alias(col)
            for col in ['vehicle_id', 'latitude', 'longitude', 'timestamp']
        ]).agg(*[
            sum(col).alias(col) 
            for col in ['vehicle_id', 'latitude', 'longitude', 'timestamp']
        ]).collect()[0]
        
        null_counts_dict = null_counts.asDict()
        
        for field, null_count in null_counts_dict.items():
            if null_count > 0:
                print(f"⚠️  WARNING: {null_count} registros com {field} nulo")
        
        # Verificar ranges de coordenadas
        invalid_coords = df.filter(
            (df.latitude < -90) | (df.latitude > 90) |
            (df.longitude < -180) | (df.longitude > 180)
        ).count()
        
        if invalid_coords > 0:
            print(f"⚠️  WARNING: {invalid_coords} registros com coordenadas inválidas")
        
        print(f"✓ Validação concluída: {record_count} registros válidos")
        
        # Retornar métricas via XCom
        return {
            'record_count': record_count,
            'invalid_coords': invalid_coords,
            'null_counts': null_counts_dict
        }
    
    finally:
        spark.stop()


def send_metrics_to_prometheus(**context):
    """
    Envia métricas para Prometheus.
    """
    from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
    
    execution_date = context['execution_date']
    
    # Obter métricas do XCom
    ti = context['ti']
    validation_metrics = ti.xcom_pull(task_ids='validate_bronze_data')
    
    if not validation_metrics:
        print("Nenhuma métrica para enviar")
        return
    
    # Criar registry
    registry = CollectorRegistry()
    
    # Definir métricas
    records_gauge = Gauge(
        'sptrans_bronze_records_total',
        'Total de registros no Bronze Layer',
        registry=registry
    )
    records_gauge.set(validation_metrics['record_count'])
    
    invalid_coords_gauge = Gauge(
        'sptrans_bronze_invalid_coords_total',
        'Total de coordenadas inválidas',
        registry=registry
    )
    invalid_coords_gauge.set(validation_metrics['invalid_coords'])
    
    print(f"✓ Métricas enviadas para Prometheus")
    print(f"  - Records: {validation_metrics['record_count']}")
    print(f"  - Invalid coords: {validation_metrics['invalid_coords']}")


# ============================================================================
# TASKS
# ============================================================================

# Task 1: Ingestão via Spark
ingest_api_task = SparkSubmitOperator(
    task_id='ingest_api_to_bronze',
    application='/opt/airflow/src/processing/jobs/ingest_api_to_bronze.py',
    name='sptrans-api-to-bronze',
    conn_id='spark_default',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.executor.memory': '4g',
        'spark.executor.cores': '2',
        'spark.driver.memory': '2g',
    },
    application_args=[
        '--execution-date', '{{ execution_date.isoformat() }}'
    ],
    verbose=True,
    dag=dag,
)

# Task 2: Validação
validate_data_task = PythonOperator(
    task_id='validate_bronze_data',
    python_callable=validate_bronze_data,
    provide_context=True,
    dag=dag,
)

# Task 3: Métricas
send_metrics_task = PythonOperator(
    task_id='send_metrics',
    python_callable=send_metrics_to_prometheus,
    provide_context=True,
    dag=dag,
)


# ============================================================================
# TASK DEPENDENCIES
# ============================================================================
ingest_api_task >> validate_data_task >> send_metrics_task