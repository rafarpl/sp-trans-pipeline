"""
DAG 02 - API Real-Time Ingestion
=================================
Ingestão contínua de posições dos ônibus da API SPTrans.

Execução: A cada 2 minutos
Destino: Bronze Layer (MinIO)

Autor: Rafael - SPTrans Pipeline
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.common.config import Config
from src.common.logging_config import get_logger
from src.ingestion.sptrans_api_client import SPTransClient
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
    'retry_delay': timedelta(seconds=30),
    'execution_timeout': timedelta(minutes=1),
}

dag = DAG(
    'dag_02_api_ingestion',
    default_args=default_args,
    description='Ingestão em tempo real da API SPTrans (a cada 2 min)',
    schedule_interval='*/2 * * * *',  # A cada 2 minutos
    catchup=False,
    max_active_runs=3,  # Permitir 3 execuções simultâneas
    tags=['ingestion', 'api', 'real-time', 'bronze'],
)


# ===========================
# FUNÇÕES DAS TASKS
# ===========================

@track_job_execution('authenticate_api')
def authenticate_sptrans_api(**context):
    """
    Task 1: Autentica na API SPTrans.
    """
    logger.info("Autenticando na API SPTrans")
    
    config = Config()
    client = SPTransClient(token=config.SPTRANS_API_TOKEN)
    
    # Autenticar
    success = client.authenticate()
    
    if not success:
        raise Exception("Falha na autenticação da API SPTrans")
    
    logger.info("✅ Autenticação bem-sucedida")
    
    # Salvar client ID no XCom (apenas para tracking)
    context['task_instance'].xcom_push(
        key='auth_timestamp',
        value=datetime.now().isoformat()
    )
    
    return {'authenticated': True, 'timestamp': datetime.now().isoformat()}


@track_job_execution('fetch_positions')
def fetch_vehicle_positions(**context):
    """
    Task 2: Busca posições atuais dos veículos.
    """
    logger.info("Buscando posições dos veículos")
    
    config = Config()
    client = SPTransClient(token=config.SPTRANS_API_TOKEN)
    
    # Autenticar novamente (cookie pode ter expirado)
    client.authenticate()
    
    # Buscar posições
    positions_data = client.get_all_positions()
    
    if not positions_data or 'l' not in positions_data:
        logger.warning("Nenhuma posição retornada pela API")
        return {'total_vehicles': 0, 'total_routes': 0}
    
    # Contar veículos
    total_vehicles = sum(len(line.get('vs', [])) for line in positions_data.get('l', []))
    total_routes = len(positions_data.get('l', []))
    
    logger.info(f"Posições obtidas: {total_vehicles:,} veículos em {total_routes:,} rotas")
    
    # Salvar dados no XCom
    context['task_instance'].xcom_push(
        key='positions_data',
        value=positions_data
    )
    
    context['task_instance'].xcom_push(
        key='vehicle_count',
        value=total_vehicles
    )
    
    # Reportar métricas
    reporter.report_api_call(
        endpoint='positions',
        status_code=200,
        response_time_ms=0,  # Client não retorna tempo
        records_count=total_vehicles
    )
    
    return {
        'total_vehicles': total_vehicles,
        'total_routes': total_routes,
        'timestamp': datetime.now().isoformat()
    }


@track_job_execution('process_and_save')
def process_and_save_to_bronze(**context):
    """
    Task 3: Processa e salva dados na Bronze Layer.
    """
    logger.info("Processando e salvando dados na Bronze")
    
    import json
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import *
    
    config = Config()
    execution_date = context['execution_date']
    
    # Recuperar dados do XCom
    ti = context['task_instance']
    positions_data = ti.xcom_pull(task_ids='fetch_vehicle_positions', key='positions_data')
    
    if not positions_data:
        logger.warning("Nenhum dado para processar")
        return {'records_saved': 0}
    
    # Criar sessão Spark
    spark = SparkSession.builder \
        .appName("API_to_Bronze_Ingestion") \
        .config("spark.driver.memory", "2g") \
        .config("spark.hadoop.fs.s3a.endpoint", config.MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", config.MINIO_ROOT_USER) \
        .config("spark.hadoop.fs.s3a.secret.key", config.MINIO_ROOT_PASSWORD) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    
    try:
        # Processar dados em lista de registros
        records = []
        ingestion_timestamp = datetime.now()
        
        for line in positions_data.get('l', []):
            route_code = line.get('c')
            route_name = line.get('lt0', '')
            route_direction = line.get('sl', 0)
            
            for vehicle in line.get('vs', []):
                records.append({
                    'vehicle_id': vehicle.get('p'),
                    'route_code': route_code,
                    'route_name': route_name,
                    'direction': route_direction,
                    'latitude': vehicle.get('py'),
                    'longitude': vehicle.get('px'),
                    'timestamp': vehicle.get('ta'),
                    'has_accessibility': vehicle.get('a', False),
                    'timestamp_capture': ingestion_timestamp.isoformat(),
                    'ingestion_timestamp': ingestion_timestamp,
                    'ingestion_date': ingestion_timestamp.date()
                })
        
        if not records:
            logger.warning("Nenhum registro para salvar")
            spark.stop()
            return {'records_saved': 0}
        
        # Criar DataFrame
        df = spark.createDataFrame(records)
        
        # Adicionar metadados da Bronze Layer
        df = df.withColumn('layer', F.lit('bronze')) \
               .withColumn('source', F.lit('sptrans_api')) \
               .withColumn('raw_data', F.lit(json.dumps(positions_data)))
        
        # Salvar na Bronze (particionado por data)
        bronze_path = f"{config.BRONZE_LAYER_PATH}/api_positions"
        
        df.write \
            .mode('append') \
            .partitionBy('ingestion_date') \
            .parquet(bronze_path)
        
        record_count = len(records)
        
        logger.info(f"✅ {record_count:,} registros salvos na Bronze")
        
        # Reportar métricas
        reporter.report_processing_stats(
            layer='bronze',
            source='api_positions',
            total=record_count,
            invalid=0,
            duplicated=0
        )
        
        return {
            'records_saved': record_count,
            'output_path': bronze_path,
            'timestamp': datetime.now().isoformat()
        }
    
    except Exception as e:
        logger.error(f"Erro ao processar dados: {e}")
        raise
    
    finally:
        spark.stop()


def validate_data_quality(**context):
    """
    Task 4: Validação básica de qualidade dos dados.
    """
    logger.info("Validando qualidade dos dados")
    
    ti = context['task_instance']
    vehicle_count = ti.xcom_pull(task_ids='fetch_vehicle_positions', key='vehicle_count')
    
    # Validações básicas
    quality_checks = {
        'has_vehicles': vehicle_count > 0,
        'reasonable_count': 100 <= vehicle_count <= 20000,
        'timestamp': datetime.now().isoformat()
    }
    
    if not quality_checks['has_vehicles']:
        logger.warning("⚠️ Nenhum veículo encontrado!")
    
    if not quality_checks['reasonable_count']:
        logger.warning(f"⚠️ Contagem suspeita de veículos: {vehicle_count}")
    
    all_checks_passed = all([
        quality_checks['has_vehicles'],
        quality_checks['reasonable_count']
    ])
    
    if all_checks_passed:
        logger.info("✅ Todos os checks de qualidade passaram")
    else:
        logger.warning("⚠️ Alguns checks de qualidade falharam")
    
    # Reportar métricas de qualidade
    reporter.report_data_quality(
        layer='bronze',
        quality_metrics={
            'completeness': 100.0 if quality_checks['has_vehicles'] else 0.0,
            'validity': 100.0 if quality_checks['reasonable_count'] else 50.0
        }
    )
    
    return quality_checks


def send_completion_notification(**context):
    """
    Task 5: Notificação de conclusão (apenas em caso de sucesso).
    """
    ti = context['task_instance']
    execution_date = context['execution_date']
    
    vehicle_count = ti.xcom_pull(task_ids='fetch_vehicle_positions', key='vehicle_count')
    
    # Log simples (em produção, poderia enviar para Slack, email, etc)
    message = f"""
    ✅ Ingestão API Concluída
    
    Execução: {execution_date}
    Veículos: {vehicle_count:,}
    Status: SUCCESS
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
    
    # Task 1: Autenticação
    auth_task = PythonOperator(
        task_id='authenticate_api',
        python_callable=authenticate_sptrans_api,
        provide_context=True,
        dag=dag
    )
    
    # Task 2: Buscar posições
    fetch_task = PythonOperator(
        task_id='fetch_vehicle_positions',
        python_callable=fetch_vehicle_positions,
        provide_context=True,
        dag=dag
    )
    
    # Task 3: Processar e salvar
    save_task = PythonOperator(
        task_id='process_and_save_to_bronze',
        python_callable=process_and_save_to_bronze,
        provide_context=True,
        dag=dag
    )
    
    # Task 4: Validação
    validate_task = PythonOperator(
        task_id='validate_data_quality',
        python_callable=validate_data_quality,
        provide_context=True,
        dag=dag
    )
    
    # Task 5: Notificação
    notify_task = PythonOperator(
        task_id='send_notification',
        python_callable=send_completion_notification,
        provide_context=True,
        trigger_rule='all_success',
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
    
    start >> auth_task >> fetch_task >> save_task >> validate_task >> notify_task >> end


# ===========================
# DOCUMENTAÇÃO
# ===========================

dag.doc_md = """
# DAG 02 - API Real-Time Ingestion

## Objetivo
Coletar posições dos ônibus da API SPTrans em tempo real e armazenar na Bronze Layer.

## Schedule
- **Frequência**: A cada 2 minutos
- **Primeira execução**: 2025-01-01
- **Catchup**: Desabilitado

## Fluxo de Execução

```
Start → Auth → Fetch → Save → Validate → Notify → End
```

### Tasks

1. **authenticate_api**: Autentica na API SPTrans
2. **fetch_vehicle_positions**: Busca posições atuais dos veículos
3. **process_and_save_to_bronze**: Processa e salva no Data Lake (Bronze)
4. **validate_data_quality**: Validação básica de qualidade
5. **send_notification**: Notificação de conclusão

## Dados Coletados

- **vehicle_id**: Prefixo do veículo
- **route_code**: Código da linha
- **latitude/longitude**: Coordenadas
- **timestamp**: Momento da captura
- **has_accessibility**: Acessibilidade

## Destino
MinIO (Bronze Layer): `s3a://sptrans-datalake/bronze/api_positions/`

Particionado por: `ingestion_date`

## Monitoramento
- Métricas exportadas para Prometheus
- Alertas configurados se falhar por 3 tentativas consecutivas
- Logs centralizados

## Dependências
- API SPTrans (externa)
- MinIO (storage)
- Spark (processamento)

## SLA
- Tempo esperado: < 1 minuto
- Taxa de sucesso esperada: > 99%
"""
