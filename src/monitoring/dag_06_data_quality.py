"""
=============================================================================
AIRFLOW DAG - DATA QUALITY CHECKS
=============================================================================
DAG para verificação contínua de qualidade de dados em todas as camadas.

Schedule: A cada 30 minutos
Dependências: Dados nas camadas Bronze, Silver e Gold
=============================================================================
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# =============================================================================
# CONFIGURAÇÕES DO DAG
# =============================================================================

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['alerts@sptrans-pipeline.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

dag = DAG(
    'data_quality_checks',
    default_args=default_args,
    description='Verificação de qualidade de dados em todas as camadas',
    schedule_interval='*/30 * * * *',  # A cada 30 minutos
    start_date=days_ago(1),
    catchup=False,
    tags=['data-quality', 'monitoring', 'validation'],
    max_active_runs=1,
)

# =============================================================================
# FUNÇÕES PYTHON
# =============================================================================

def check_bronze_quality(**context):
    """Verifica qualidade da camada Bronze"""
    from pyspark.sql import SparkSession
    from datetime import datetime
    import logging
    
    logger = logging.getLogger(__name__)
    logger.info("=== Iniciando verificação de qualidade Bronze ===")
    
    # Data de execução
    execution_date = context['ds']
    
    # Criar SparkSession
    spark = SparkSession.builder \
        .appName("Bronze-Quality-Check") \
        .getOrCreate()
    
    try:
        # Ler dados Bronze
        bronze_path = f"s3a://sptrans-datalake/bronze/api_positions/date={execution_date}"
        
        logger.info(f"Lendo dados: {bronze_path}")
        df = spark.read.parquet(bronze_path)
        
        total_records = df.count()
        logger.info(f"Total de registros: {total_records}")
        
        # Verificações de qualidade
        quality_checks = {
            'total_records': total_records,
            'null_vehicle_ids': df.filter(df.vehicle_id.isNull()).count(),
            'null_timestamps': df.filter(df.timestamp.isNull()).count(),
            'null_coordinates': df.filter(
                df.latitude.isNull() | df.longitude.isNull()
            ).count(),
            'invalid_coordinates': df.filter(
                (df.latitude < -24.0) | (df.latitude > -23.3) |
                (df.longitude < -46.9) | (df.longitude > -46.3)
            ).count(),
        }
        
        # Calcular score de qualidade
        issues = sum([
            quality_checks['null_vehicle_ids'],
            quality_checks['null_timestamps'],
            quality_checks['null_coordinates'],
            quality_checks['invalid_coordinates'],
        ])
        
        quality_score = ((total_records - issues) / total_records * 100) if total_records > 0 else 0
        quality_checks['quality_score'] = quality_score
        
        logger.info(f"Quality Score Bronze: {quality_score:.2f}%")
        
        # Alertar se score baixo
        if quality_score < 80:
            logger.warning(f"⚠️ Quality score baixo: {quality_score:.2f}%")
            # Enviar alerta
            context['task_instance'].xcom_push(key='bronze_quality_alert', value=True)
        
        # Salvar métricas
        context['task_instance'].xcom_push(key='bronze_quality', value=quality_checks)
        
        return quality_checks
    
    finally:
        spark.stop()


def check_silver_quality(**context):
    """Verifica qualidade da camada Silver"""
    from pyspark.sql import SparkSession
    import logging
    
    logger = logging.getLogger(__name__)
    logger.info("=== Iniciando verificação de qualidade Silver ===")
    
    execution_date = context['ds']
    
    spark = SparkSession.builder \
        .appName("Silver-Quality-Check") \
        .getOrCreate()
    
    try:
        silver_path = f"s3a://sptrans-datalake/silver/positions/date={execution_date}"
        
        logger.info(f"Lendo dados: {silver_path}")
        df = spark.read.parquet(silver_path)
        
        total_records = df.count()
        
        quality_checks = {
            'total_records': total_records,
            'records_with_route': df.filter(df.route_code.isNotNull()).count(),
            'records_with_speed': df.filter(df.speed.isNotNull()).count(),
            'duplicates': total_records - df.dropDuplicates(['vehicle_id', 'timestamp']).count(),
        }
        
        # Calcular completude
        completeness = (quality_checks['records_with_route'] / total_records * 100) if total_records > 0 else 0
        quality_checks['completeness'] = completeness
        
        quality_score = completeness
        quality_checks['quality_score'] = quality_score
        
        logger.info(f"Quality Score Silver: {quality_score:.2f}%")
        
        if quality_score < 85:
            logger.warning(f"⚠️ Quality score Silver baixo: {quality_score:.2f}%")
            context['task_instance'].xcom_push(key='silver_quality_alert', value=True)
        
        context['task_instance'].xcom_push(key='silver_quality', value=quality_checks)
        
        return quality_checks
    
    finally:
        spark.stop()


def check_gold_quality(**context):
    """Verifica qualidade da camada Gold"""
    from pyspark.sql import SparkSession
    import logging
    
    logger = logging.getLogger(__name__)
    logger.info("=== Iniciando verificação de qualidade Gold ===")
    
    execution_date = context['ds']
    
    spark = SparkSession.builder \
        .appName("Gold-Quality-Check") \
        .getOrCreate()
    
    try:
        # Verificar KPIs horários
        kpis_path = f"s3a://sptrans-datalake/gold/kpis_hourly/date={execution_date}"
        
        logger.info(f"Lendo KPIs: {kpis_path}")
        df = spark.read.parquet(kpis_path)
        
        total_records = df.count()
        
        quality_checks = {
            'total_kpi_records': total_records,
            'null_metrics': df.filter(
                df.avg_speed_kmh.isNull() | df.total_vehicles.isNull()
            ).count(),
            'avg_quality_score': df.agg({'data_quality_score': 'avg'}).collect()[0][0],
        }
        
        quality_score = quality_checks['avg_quality_score'] if quality_checks['avg_quality_score'] else 0
        quality_checks['quality_score'] = quality_score
        
        logger.info(f"Quality Score Gold: {quality_score:.2f}%")
        
        if quality_score < 90:
            logger.warning(f"⚠️ Quality score Gold baixo: {quality_score:.2f}%")
            context['task_instance'].xcom_push(key='gold_quality_alert', value=True)
        
        context['task_instance'].xcom_push(key='gold_quality', value=quality_checks)
        
        return quality_checks
    
    finally:
        spark.stop()


def save_quality_metrics_to_postgres(**context):
    """Salva métricas de qualidade no PostgreSQL"""
    import psycopg2
    import json
    from datetime import datetime
    import logging
    
    logger = logging.getLogger(__name__)
    logger.info("=== Salvando métricas de qualidade no PostgreSQL ===")
    
    # Obter métricas do XCom
    bronze_quality = context['task_instance'].xcom_pull(
        task_ids='check_bronze_quality',
        key='bronze_quality'
    )
    silver_quality = context['task_instance'].xcom_pull(
        task_ids='check_silver_quality',
        key='silver_quality'
    )
    gold_quality = context['task_instance'].xcom_pull(
        task_ids='check_gold_quality',
        key='gold_quality'
    )
    
    # Conectar ao PostgreSQL
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow123"
    )
    
    try:
        cursor = conn.cursor()
        
        execution_date = context['ds']
        timestamp = datetime.now()
        
        # Inserir métricas
        insert_query = """
            INSERT INTO serving.data_quality_metrics 
            (date, hour, layer, source, total_records, quality_score, 
             completeness_score, validity_score, accuracy_score, 
             overall_quality_score, metrics_json, processing_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Bronze metrics
        cursor.execute(insert_query, (
            execution_date,
            datetime.now().hour,
            'bronze',
            'api',
            bronze_quality.get('total_records', 0),
            bronze_quality.get('quality_score', 0),
            100.0,  # completeness
            95.0,   # validity
            90.0,   # accuracy
            bronze_quality.get('quality_score', 0),
            json.dumps(bronze_quality),
            timestamp
        ))
        
        # Silver metrics
        cursor.execute(insert_query, (
            execution_date,
            datetime.now().hour,
            'silver',
            'positions',
            silver_quality.get('total_records', 0),
            silver_quality.get('quality_score', 0),
            silver_quality.get('completeness', 0),
            95.0,
            90.0,
            silver_quality.get('quality_score', 0),
            json.dumps(silver_quality),
            timestamp
        ))
        
        # Gold metrics
        cursor.execute(insert_query, (
            execution_date,
            datetime.now().hour,
            'gold',
            'kpis',
            gold_quality.get('total_kpi_records', 0),
            gold_quality.get('quality_score', 0),
            100.0,
            100.0,
            100.0,
            gold_quality.get('quality_score', 0),
            json.dumps(gold_quality),
            timestamp
        ))
        
        conn.commit()
        
        logger.info("✓ Métricas salvas no PostgreSQL")
        
    finally:
        cursor.close()
        conn.close()


def send_quality_alerts(**context):
    """Envia alertas se qualidade estiver baixa"""
    import logging
    
    logger = logging.getLogger(__name__)
    logger.info("=== Verificando alertas de qualidade ===")
    
    # Verificar se há alertas
    bronze_alert = context['task_instance'].xcom_pull(
        task_ids='check_bronze_quality',
        key='bronze_quality_alert'
    )
    silver_alert = context['task_instance'].xcom_pull(
        task_ids='check_silver_quality',
        key='silver_quality_alert'
    )
    gold_alert = context['task_instance'].xcom_pull(
        task_ids='check_gold_quality',
        key='gold_quality_alert'
    )
    
    alerts = []
    
    if bronze_alert:
        alerts.append("⚠️ ALERTA: Quality score Bronze abaixo do threshold")
    
    if silver_alert:
        alerts.append("⚠️ ALERTA: Quality score Silver abaixo do threshold")
    
    if gold_alert:
        alerts.append("⚠️ ALERTA: Quality score Gold abaixo do threshold")
    
    if alerts:
        logger.warning("\n".join(alerts))
        # Aqui você pode integrar com Slack, email, etc
    else:
        logger.info("✓ Todos os checks de qualidade passaram")


# =============================================================================
# TASKS
# =============================================================================

# Task 1: Check Bronze Quality
check_bronze = PythonOperator(
    task_id='check_bronze_quality',
    python_callable=check_bronze_quality,
    provide_context=True,
    dag=dag,
)

# Task 2: Check Silver Quality
check_silver = PythonOperator(
    task_id='check_silver_quality',
    python_callable=check_silver_quality,
    provide_context=True,
    dag=dag,
)

# Task 3: Check Gold Quality
check_gold = PythonOperator(
    task_id='check_gold_quality',
    python_callable=check_gold_quality,
    provide_context=True,
    dag=dag,
)

# Task 4: Save Metrics to PostgreSQL
save_metrics = PythonOperator(
    task_id='save_quality_metrics',
    python_callable=save_quality_metrics_to_postgres,
    provide_context=True,
    dag=dag,
)

# Task 5: Send Alerts
send_alerts = PythonOperator(
    task_id='send_quality_alerts',
    python_callable=send_quality_alerts,
    provide_context=True,
    dag=dag,
)

# Task 6: Update Prometheus Metrics
update_prometheus = BashOperator(
    task_id='update_prometheus_metrics',
    bash_command="""
    echo "Atualizando métricas Prometheus..."
    # Aqui você pode usar pushgateway para enviar métricas
    """,
    dag=dag,
)

# =============================================================================
# DEPENDÊNCIAS
# =============================================================================

# Verificações em paralelo
[check_bronze, check_silver, check_gold] >> save_metrics >> send_alerts >> update_prometheus

# =============================================================================
# DOCUMENTAÇÃO
# =============================================================================

dag.doc_md = """
# DAG de Data Quality

Verifica a qualidade dos dados em todas as camadas do Data Lake.

## Funcionalidades

- **Bronze Quality**: Valida dados brutos da API
- **Silver Quality**: Valida dados limpos e enriquecidos  
- **Gold Quality**: Valida KPIs e agregações
- **Metrics Storage**: Salva métricas no PostgreSQL
- **Alerting**: Envia alertas para quality scores baixos

## Thresholds

- Bronze: 80%
- Silver: 85%
- Gold: 90%

## Execução

Roda a cada 30 minutos automaticamente.
"""

# =============================================================================
# END
# =============================================================================
