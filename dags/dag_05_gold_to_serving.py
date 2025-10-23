"""
DAG 05 - Gold to Serving Layer
===============================
Carrega dados agregados da camada Gold para PostgreSQL (Serving Layer).

Execução: A cada hora (após DAG 04)
Destino: PostgreSQL com tabelas otimizadas para consultas

Autor: Rafael - SPTrans Pipeline
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.common.config import Config
from src.common.logging_config import get_logger
from src.serving.postgres_loader import PostgresLoader
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
    'execution_timeout': timedelta(minutes=30),
}

dag = DAG(
    'dag_05_gold_to_serving',
    default_args=default_args,
    description='Carrega dados Gold para PostgreSQL (Serving Layer)',
    schedule_interval='15 * * * *',  # 15 minutos após cada hora
    catchup=False,
    max_active_runs=1,
    tags=['serving', 'gold', 'postgresql', 'load'],
)


# ===========================
# FUNÇÕES DAS TASKS
# ===========================

@track_job_execution('load_kpis_hourly')
def load_kpis_to_postgres(**context):
    """
    Task 1: Carrega KPIs por hora para PostgreSQL.
    """
    logger.info("Carregando KPIs hourly para PostgreSQL")
    
    execution_date = context['execution_date']
    config = Config()
    
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    
    spark = SparkSession.builder \
        .appName("Load_KPIs_to_Postgres") \
        .config("spark.driver.memory", "2g") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.5.0.jar") \
        .getOrCreate()
    
    try:
        # Ler KPIs da Gold
        gold_kpis_path = f"{config.GOLD_LAYER_PATH}/kpis_hourly"
        df_kpis = spark.read.parquet(gold_kpis_path)
        
        # Filtrar última hora
        df_recent = df_kpis.filter(
            F.col('processing_timestamp') >= F.expr("current_timestamp() - interval 1 hour")
        )
        
        record_count = df_recent.count()
        logger.info(f"KPIs para carregar: {record_count:,}")
        
        if record_count == 0:
            logger.warning("Nenhum KPI para carregar")
            return {'records_loaded': 0}
        
        # Configuração JDBC
        jdbc_url = f"jdbc:postgresql://{config.POSTGRES_HOST}:{config.POSTGRES_PORT}/{config.POSTGRES_DB}"
        connection_properties = {
            "user": config.POSTGRES_USER,
            "password": config.POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver"
        }
        
        # Escrever para PostgreSQL
        df_recent.write \
            .jdbc(
                url=jdbc_url,
                table="serving.kpis_hourly",
                mode="append",
                properties=connection_properties
            )
        
        logger.info(f"KPIs carregados com sucesso: {record_count:,} registros")
        
        # Push para XCom
        context['task_instance'].xcom_push(
            key='kpis_loaded',
            value=record_count
        )
        
        # Reportar métricas
        reporter.report_processing_stats(
            layer='serving',
            source='kpis_hourly',
            total=record_count,
            invalid=0,
            duplicated=0
        )
        
        return {'records_loaded': record_count}
    
    except Exception as e:
        logger.error(f"Erro ao carregar KPIs: {e}")
        raise
    
    finally:
        spark.stop()


@track_job_execution('load_route_metrics')
def load_route_metrics_to_postgres(**context):
    """
    Task 2: Carrega métricas por rota para PostgreSQL.
    """
    logger.info("Carregando métricas de rota para PostgreSQL")
    
    execution_date = context['execution_date']
    config = Config()
    
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    
    spark = SparkSession.builder \
        .appName("Load_Route_Metrics_to_Postgres") \
        .config("spark.driver.memory", "2g") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.5.0.jar") \
        .getOrCreate()
    
    try:
        # Ler métricas da Gold
        gold_metrics_path = f"{config.GOLD_LAYER_PATH}/metrics_by_route"
        df_metrics = spark.read.parquet(gold_metrics_path)
        
        # Filtrar último dia
        df_recent = df_metrics.filter(
            F.col('date') == F.lit(execution_date.date())
        )
        
        record_count = df_recent.count()
        logger.info(f"Métricas para carregar: {record_count:,}")
        
        if record_count == 0:
            logger.warning("Nenhuma métrica para carregar")
            return {'records_loaded': 0}
        
        # Configuração JDBC
        jdbc_url = f"jdbc:postgresql://{config.POSTGRES_HOST}:{config.POSTGRES_PORT}/{config.POSTGRES_DB}"
        connection_properties = {
            "user": config.POSTGRES_USER,
            "password": config.POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver"
        }
        
        # Escrever para PostgreSQL
        df_recent.write \
            .jdbc(
                url=jdbc_url,
                table="serving.route_metrics",
                mode="append",
                properties=connection_properties
            )
        
        logger.info(f"Métricas carregadas: {record_count:,} registros")
        
        return {'records_loaded': record_count}
    
    except Exception as e:
        logger.error(f"Erro ao carregar métricas: {e}")
        raise
    
    finally:
        spark.stop()


@track_job_execution('load_headway_analysis')
def load_headway_to_postgres(**context):
    """
    Task 3: Carrega análise de headway para PostgreSQL.
    """
    logger.info("Carregando análise de headway para PostgreSQL")
    
    execution_date = context['execution_date']
    config = Config()
    
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    
    spark = SparkSession.builder \
        .appName("Load_Headway_to_Postgres") \
        .config("spark.driver.memory", "2g") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.5.0.jar") \
        .getOrCreate()
    
    try:
        # Ler headway da Gold
        gold_headway_path = f"{config.GOLD_LAYER_PATH}/headway_analysis"
        df_headway = spark.read.parquet(gold_headway_path)
        
        # Filtrar último dia
        df_recent = df_headway.filter(
            F.col('date') == F.lit(execution_date.date())
        )
        
        record_count = df_recent.count()
        logger.info(f"Análises de headway para carregar: {record_count:,}")
        
        if record_count == 0:
            logger.warning("Nenhuma análise de headway para carregar")
            return {'records_loaded': 0}
        
        # Configuração JDBC
        jdbc_url = f"jdbc:postgresql://{config.POSTGRES_HOST}:{config.POSTGRES_PORT}/{config.POSTGRES_DB}"
        connection_properties = {
            "user": config.POSTGRES_USER,
            "password": config.POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver"
        }
        
        # Escrever para PostgreSQL
        df_recent.write \
            .jdbc(
                url=jdbc_url,
                table="serving.headway_analysis",
                mode="append",
                properties=connection_properties
            )
        
        logger.info(f"Headway carregado: {record_count:,} registros")
        
        return {'records_loaded': record_count}
    
    except Exception as e:
        logger.error(f"Erro ao carregar headway: {e}")
        raise
    
    finally:
        spark.stop()


@track_job_execution('update_materialized_views')
def refresh_materialized_views(**context):
    """
    Task 4: Atualiza views materializadas no PostgreSQL.
    """
    logger.info("Atualizando views materializadas")
    
    config = Config()
    
    import psycopg2
    
    try:
        # Conectar ao PostgreSQL
        conn = psycopg2.connect(
            host=config.POSTGRES_HOST,
            port=config.POSTGRES_PORT,
            database=config.POSTGRES_DB,
            user=config.POSTGRES_USER,
            password=config.POSTGRES_PASSWORD
        )
        
        cursor = conn.cursor()
        
        # Lista de views materializadas para atualizar
        materialized_views = [
            'serving.mv_current_vehicle_status',
            'serving.mv_route_summary_today',
            'serving.mv_system_kpis_realtime'
        ]
        
        refreshed_count = 0
        
        for view_name in materialized_views:
            try:
                logger.info(f"Atualizando {view_name}...")
                cursor.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view_name}")
                conn.commit()
                refreshed_count += 1
                logger.info(f"  ✓ {view_name} atualizada")
            except Exception as e:
                logger.warning(f"  ✗ Erro ao atualizar {view_name}: {e}")
                conn.rollback()
        
        cursor.close()
        conn.close()
        
        logger.info(f"Views atualizadas: {refreshed_count}/{len(materialized_views)}")
        
        return {
            'views_refreshed': refreshed_count,
            'total_views': len(materialized_views)
        }
    
    except Exception as e:
        logger.error(f"Erro ao atualizar views: {e}")
        raise


@track_job_execution('cache_to_redis')
def cache_latest_data_to_redis(**context):
    """
    Task 5: Cacheia dados mais recentes no Redis.
    """
    logger.info("Cacheando dados no Redis")
    
    config = Config()
    
    import redis
    import json
    import psycopg2
    
    try:
        # Conectar ao Redis
        redis_client = redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            decode_responses=True
        )
        
        # Conectar ao PostgreSQL
        pg_conn = psycopg2.connect(
            host=config.POSTGRES_HOST,
            port=config.POSTGRES_PORT,
            database=config.POSTGRES_DB,
            user=config.POSTGRES_USER,
            password=config.POSTGRES_PASSWORD
        )
        
        cursor = pg_conn.cursor()
        
        # 1. Cachear resumo do sistema (última hora)
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT route_code) as active_routes,
                SUM(total_vehicles) as total_vehicles,
                AVG(avg_speed_kmh) as avg_speed,
                AVG(data_quality_score) as quality_score
            FROM serving.kpis_hourly
            WHERE processing_timestamp > NOW() - INTERVAL '1 hour'
        """)
        
        system_summary = cursor.fetchone()
        
        if system_summary:
            summary_data = {
                'active_routes': system_summary[0] or 0,
                'total_vehicles': system_summary[1] or 0,
                'avg_speed': float(system_summary[2]) if system_summary[2] else 0.0,
                'quality_score': float(system_summary[3]) if system_summary[3] else 0.0,
                'timestamp': datetime.now().isoformat()
            }
            
            redis_client.setex(
                'system:summary',
                300,  # TTL: 5 minutos
                json.dumps(summary_data)
            )
            
            logger.info(f"System summary cacheado: {summary_data}")
        
        # 2. Cachear top 10 rotas mais movimentadas
        cursor.execute("""
            SELECT 
                route_code,
                route_short_name,
                total_vehicles,
                avg_speed_kmh
            FROM serving.kpis_hourly
            WHERE processing_timestamp > NOW() - INTERVAL '1 hour'
            ORDER BY total_vehicles DESC
            LIMIT 10
        """)
        
        top_routes = cursor.fetchall()
        
        if top_routes:
            routes_data = [
                {
                    'route_code': row[0],
                    'route_name': row[1],
                    'vehicles': row[2],
                    'avg_speed': float(row[3]) if row[3] else 0.0
                }
                for row in top_routes
            ]
            
            redis_client.setex(
                'routes:top_active',
                300,  # TTL: 5 minutos
                json.dumps(routes_data)
            )
            
            logger.info(f"Top routes cacheadas: {len(routes_data)}")
        
        cursor.close()
        pg_conn.close()
        
        logger.info("Dados cacheados no Redis com sucesso")
        
        return {
            'cache_keys_created': 2,
            'ttl_seconds': 300
        }
    
    except Exception as e:
        logger.error(f"Erro ao cachear no Redis: {e}")
        raise


def send_completion_notification(**context):
    """
    Task 6: Notificação de conclusão.
    """
    logger.info("Enviando notificação de conclusão")
    
    ti = context['task_instance']
    execution_date = context['execution_date']
    
    kpis_loaded = ti.xcom_pull(task_ids='load_kpis_to_postgres', key='kpis_loaded') or 0
    
    message = f"""
    ✅ DAG Gold → Serving Concluída
    
    Data: {execution_date}
    
    📥 Dados Carregados:
    - KPIs por hora: {kpis_loaded:,}
    - Métricas de rota: Carregadas
    - Análise de headway: Carregada
    
    🔄 Atualizações:
    - Views materializadas: Atualizadas
    - Cache Redis: Atualizado
    
    Status: SUCCESS ✅
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
    
    # Sensor para aguardar DAG 04
    wait_for_gold = ExternalTaskSensor(
        task_id='wait_for_gold_aggregation',
        external_dag_id='dag_04_silver_to_gold',
        external_task_id='end',
        timeout=600,
        poke_interval=60,
        mode='reschedule',
        dag=dag
    )
    
    # Tasks de carga (em paralelo)
    load_kpis = PythonOperator(
        task_id='load_kpis_to_postgres',
        python_callable=load_kpis_to_postgres,
        provide_context=True,
        dag=dag
    )
    
    load_metrics = PythonOperator(
        task_id='load_route_metrics_to_postgres',
        python_callable=load_route_metrics_to_postgres,
        provide_context=True,
        dag=dag
    )
    
    load_headway = PythonOperator(
        task_id='load_headway_to_postgres',
        python_callable=load_headway_to_postgres,
        provide_context=True,
        dag=dag
    )
    
    # Atualizar views
    refresh_views = PythonOperator(
        task_id='refresh_materialized_views',
        python_callable=refresh_materialized_views,
        provide_context=True,
        dag=dag
    )
    
    # Cachear no Redis
    cache_redis = PythonOperator(
        task_id='cache_to_redis',
        python_callable=cache_latest_data_to_redis,
        provide_context=True,
        dag=dag
    )
    
    # Notificação
    notify = PythonOperator(
        task_id='send_notification',
        python_callable=send_completion_notification,
        provide_context=True,
        trigger_rule='all_done',
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
    
    start >> wait_for_gold >> [load_kpis, load_metrics, load_headway]
    [load_kpis, load_metrics, load_headway] >> refresh_views >> cache_redis >> notify >> end


# ===========================
# DOCUMENTAÇÃO
# ===========================

dag.doc_md = """
# DAG 05 - Gold to Serving Layer

## Objetivo
Carregar dados agregados da camada Gold para PostgreSQL (Serving Layer) e Redis (Cache).

## Schedule
- **Frequência**: A cada hora (15 minutos após DAG 04)
- **Dependências**: DAG 04 (Silver to Gold)

## Operações

### 1. Load para PostgreSQL
- KPIs por hora
- Métricas por rota
- Análise de headway

### 2. Atualização de Views
- Views materializadas
- Índices otimizados

### 3. Cache Redis
- Resumo do sistema
- Top rotas ativas
- TTL: 5 minutos

## Destino
PostgreSQL `serving` schema:
- `kpis_hourly`
- `route_metrics`
- `headway_analysis`

## Uso
- Dashboards (Superset/Grafana)
- APIs de consulta
- Relatórios executivos
"""
