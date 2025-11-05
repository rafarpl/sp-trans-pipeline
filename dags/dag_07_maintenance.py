"""
Airflow DAG: Manutenção e Otimização.

Executa tarefas de manutenção: limpeza de dados antigos,
otimização de tabelas Delta, vacuum, compaction, etc.

Schedule: Semanal (domingos às 03:00)
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
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=2),
}

dag = DAG(
    'sptrans_07_maintenance',
    default_args=default_args,
    description='Manutenção e otimização do pipeline',
    schedule_interval='0 3 * * 0',  # Domingo às 03:00
    catchup=False,
    max_active_runs=1,
    tags=['sptrans', 'maintenance', 'optimization'],
)


def optimize_delta_tables(**context):
    """Otimiza tabelas Delta (compaction, Z-ordering)."""
    from pyspark.sql import SparkSession
    from src.common.utils import get_s3_path
    from src.common.constants import BUCKET_SILVER, BUCKET_GOLD
    
    print("🔧 Optimizing Delta tables")
    
    spark = SparkSession.builder.appName("Delta_Optimize").getOrCreate()
    
    tables = [
        (BUCKET_SILVER, "vehicle_positions"),
        (BUCKET_GOLD, "hourly_metrics"),
        (BUCKET_GOLD, "line_performance"),
    ]
    
    optimized = []
    
    for bucket, prefix in tables:
        try:
            path = get_s3_path(bucket, prefix)
            print(f"  Optimizing {path}...")
            
            # Optimize (compaction)
            spark.sql(f"OPTIMIZE delta.`{path}`")
            
            # Z-Ordering (clustering) - melhora performance de queries
            # Para Silver: ordenar por vehicle_id, position_timestamp
            if "vehicle_positions" in prefix:
                spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY (vehicle_id, position_timestamp)")
            
            optimized.append(f"{bucket}/{prefix}")
            print(f"  ✅ Optimized {bucket}/{prefix}")
            
        except Exception as e:
            print(f"  ⚠️  Error optimizing {bucket}/{prefix}: {e}")
    
    spark.stop()
    
    print(f"✅ Optimized {len(optimized)} Delta tables")
    return optimized


def vacuum_delta_tables(**context):
    """Executa VACUUM em tabelas Delta (limpa arquivos antigos)."""
    from pyspark.sql import SparkSession
    from src.common.utils import get_s3_path
    from src.common.constants import BUCKET_SILVER, BUCKET_GOLD, DELTA_VACUUM_RETENTION_HOURS
    
    print("🧹 Vacuuming Delta tables")
    
    spark = SparkSession.builder.appName("Delta_Vacuum").getOrCreate()
    
    tables = [
        (BUCKET_SILVER, "vehicle_positions"),
        (BUCKET_GOLD, "hourly_metrics"),
    ]
    
    vacuumed = []
    
    for bucket, prefix in tables:
        try:
            path = get_s3_path(bucket, prefix)
            print(f"  Vacuuming {path} (retention: {DELTA_VACUUM_RETENTION_HOURS}h)...")
            
            # VACUUM remove arquivos não mais referenciados
            spark.sql(f"VACUUM delta.`{path}` RETAIN {DELTA_VACUUM_RETENTION_HOURS} HOURS")
            
            vacuumed.append(f"{bucket}/{prefix}")
            print(f"  ✅ Vacuumed {bucket}/{prefix}")
            
        except Exception as e:
            print(f"  ⚠️  Error vacuuming {bucket}/{prefix}: {e}")
    
    spark.stop()
    
    print(f"✅ Vacuumed {len(vacuumed)} Delta tables")
    return vacuumed


def cleanup_old_bronze_data(**context):
    """Remove dados Bronze antigos (> 30 dias)."""
    from pyspark.sql import SparkSession
    from src.common.utils import get_s3_path
    from src.common.constants import BUCKET_BRONZE, PREFIX_API_POSITIONS
    
    print("🗑️  Cleaning up old Bronze data (> 30 days)")
    
    spark = SparkSession.builder.appName("Bronze_Cleanup").getOrCreate()
    
    try:
        bronze_path = get_s3_path(BUCKET_BRONZE, PREFIX_API_POSITIONS)
        
        # Ler Bronze
        df = spark.read.parquet(bronze_path)
        
        # Contar registros antigos
        old_df = df.filter("ingestion_timestamp < current_timestamp() - interval 30 days")
        old_count = old_df.count()
        
        if old_count > 0:
            print(f"  Found {old_count:,} old records to delete")
            
            # Para Parquet, não podemos deletar in-place facilmente
            # Alternativa: reescrever apenas dados recentes
            recent_df = df.filter("ingestion_timestamp >= current_timestamp() - interval 30 days")
            recent_count = recent_df.count()
            
            print(f"  Keeping {recent_count:,} recent records")
            
            # Reescrever (backup antes em produção!)
            # recent_df.write.mode("overwrite").parquet(bronze_path)
            print(f"  ⚠️  Cleanup skipped (requires manual backup)")
        else:
            print(f"  ✅ No old data to cleanup")
        
        spark.stop()
        
    except Exception as e:
        spark.stop()
        print(f"❌ Bronze cleanup error: {e}")
        raise


def vacuum_postgres_tables(**context):
    """Executa VACUUM ANALYZE no PostgreSQL."""
    import psycopg2
    from src.common.config import Config
    
    config = Config()
    
    print("🧹 Vacuuming PostgreSQL tables")
    
    conn = psycopg2.connect(
        host=config.postgres.host,
        port=config.postgres.port,
        database=config.postgres.database,
        user=config.postgres.user,
        password=config.postgres.password
    )
    
    # VACUUM precisa de autocommit
    conn.set_isolation_level(0)
    
    try:
        cur = conn.cursor()
        
        tables = [
            'serving.hourly_aggregates',
            'serving.daily_aggregates',
            'serving.lines_metrics',
        ]
        
        for table in tables:
            print(f"  Vacuuming {table}...")
            cur.execute(f"VACUUM ANALYZE {table}")
            print(f"  ✅ Vacuumed {table}")
        
        cur.close()
        conn.close()
        
        print(f"✅ Vacuumed {len(tables)} PostgreSQL tables")
        
    except Exception as e:
        conn.close()
        print(f"❌ PostgreSQL vacuum error: {e}")
        raise


def refresh_all_materialized_views(**context):
    """Atualiza todas materialized views."""
    import psycopg2
    from src.common.config import Config
    
    config = Config()
    
    print("🔄 Refreshing all materialized views")
    
    conn = psycopg2.connect(
        host=config.postgres.host,
        port=config.postgres.port,
        database=config.postgres.database,
        user=config.postgres.user,
        password=config.postgres.password
    )
    
    try:
        cur = conn.cursor()
        
        # Listar todas MVs no schema serving
        cur.execute("""
            SELECT schemaname, matviewname 
            FROM pg_matviews 
            WHERE schemaname = 'serving'
        """)
        
        views = cur.fetchall()
        
        for schema, view_name in views:
            full_name = f"{schema}.{view_name}"
            print(f"  Refreshing {full_name}...")
            cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {full_name}")
            conn.commit()
            print(f"  ✅ Refreshed {full_name}")
        
        cur.close()
        conn.close()
        
        print(f"✅ Refreshed {len(views)} materialized views")
        
    except Exception as e:
        conn.close()
        print(f"❌ Refresh views error: {e}")
        raise


def analyze_postgres_statistics(**context):
    """Atualiza estatísticas do PostgreSQL para melhor query planning."""
    import psycopg2
    from src.common.config import Config
    
    config = Config()
    
    print("📊 Analyzing PostgreSQL statistics")
    
    conn = psycopg2.connect(
        host=config.postgres.host,
        port=config.postgres.port,
        database=config.postgres.database,
        user=config.postgres.user,
        password=config.postgres.password
    )
    
    conn.set_isolation_level(0)
    
    try:
        cur = conn.cursor()
        
        # ANALYZE atualiza estatísticas para o query planner
        print("  Running ANALYZE on database...")
        cur.execute("ANALYZE")
        
        cur.close()
        conn.close()
        
        print("✅ PostgreSQL statistics updated")
        
    except Exception as e:
        conn.close()
        print(f"❌ Analyze error: {e}")
        raise


def generate_maintenance_report(**context):
    """Gera relatório de manutenção."""
    execution_date = context['execution_date']
    
    print("📋 Generating maintenance report")
    
    report = {
        'timestamp': datetime.now().isoformat(),
        'execution_date': execution_date.isoformat(),
        'tasks_completed': [
            'delta_optimization',
            'delta_vacuum',
            'postgres_vacuum',
            'materialized_views_refresh',
            'statistics_update',
        ],
        'status': 'success',
    }
    
    print(f"✅ Maintenance report: {report}")
    
    context['task_instance'].xcom_push(key='maintenance_report', value=report)
    
    return report


# Tasks
with dag:
    with TaskGroup('delta_maintenance', tooltip='Delta Lake maintenance') as delta_group:
        optimize = PythonOperator(
            task_id='optimize_delta_tables',
            python_callable=optimize_delta_tables,
            provide_context=True,
        )
        
        vacuum = PythonOperator(
            task_id='vacuum_delta_tables',
            python_callable=vacuum_delta_tables,
            provide_context=True,
        )
        
        optimize >> vacuum
    
    with TaskGroup('postgres_maintenance', tooltip='PostgreSQL maintenance') as postgres_group:
        vacuum_pg = PythonOperator(
            task_id='vacuum_postgres_tables',
            python_callable=vacuum_postgres_tables,
            provide_context=True,
        )
        
        refresh_views = PythonOperator(
            task_id='refresh_materialized_views',
            python_callable=refresh_all_materialized_views,
            provide_context=True,
        )
        
        analyze_stats = PythonOperator(
            task_id='analyze_statistics',
            python_callable=analyze_postgres_statistics,
            provide_context=True,
        )
        
        vacuum_pg >> refresh_views >> analyze_stats
    
    cleanup_bronze = PythonOperator(
        task_id='cleanup_old_bronze_data',
        python_callable=cleanup_old_bronze_data,
        provide_context=True,
    )
    
    generate_report = PythonOperator(
        task_id='generate_maintenance_report',
        python_callable=generate_maintenance_report,
        provide_context=True,
    )
    
    # Fluxo
    [delta_group, postgres_group, cleanup_bronze] >> generate_report
