"""
Testes de Integração: PostgreSQL Loader
Projeto: SPTrans Real-Time Data Pipeline
Autor: Equipe LABDATA/FIA

Testa o carregamento de dados da camada Gold para PostgreSQL:
- Conexão com banco de dados
- Inserção de dados
- Atualização de dados
- Integridade referencial
- Performance
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, BooleanType
)
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import tempfile
import os


@pytest.fixture(scope="module")
def spark_session():
    """Cria uma sessão Spark para testes"""
    spark = SparkSession.builder \
        .appName("test_postgres_loader") \
        .master("local[2]") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()


@pytest.fixture(scope="module")
def db_config():
    """Configuração do banco de dados de teste"""
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "database": os.getenv("POSTGRES_TEST_DB", "sptrans_test"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "postgres")
    }


@pytest.fixture(scope="module")
def db_connection(db_config):
    """Cria conexão com banco de dados de teste"""
    # Conectar ao postgres para criar o banco de teste
    conn = psycopg2.connect(
        host=db_config["host"],
        port=db_config["port"],
        database="postgres",
        user=db_config["user"],
        password=db_config["password"]
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    
    # Criar banco de teste
    try:
        cursor.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(
            sql.Identifier(db_config["database"])
        ))
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(
            sql.Identifier(db_config["database"])
        ))
    except Exception as e:
        print(f"Erro ao criar banco de teste: {e}")
    finally:
        cursor.close()
        conn.close()
    
    # Conectar ao banco de teste
    test_conn = psycopg2.connect(**db_config)
    
    yield test_conn
    
    # Cleanup
    test_conn.close()
    
    # Deletar banco de teste
    conn = psycopg2.connect(
        host=db_config["host"],
        port=db_config["port"],
        database="postgres",
        user=db_config["user"],
        password=db_config["password"]
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    try:
        cursor.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(
            sql.Identifier(db_config["database"])
        ))
    except Exception as e:
        print(f"Erro ao deletar banco de teste: {e}")
    finally:
        cursor.close()
        conn.close()


class TestDatabaseConnection:
    """Testes para conexão com banco de dados"""
    
    def test_connection_successful(self, db_connection):
        """Testa se a conexão com o banco é bem-sucedida"""
        assert db_connection is not None
        assert not db_connection.closed
    
    def test_execute_simple_query(self, db_connection):
        """Testa execução de query simples"""
        cursor = db_connection.cursor()
        cursor.execute("SELECT 1 as test")
        result = cursor.fetchone()
        cursor.close()
        
        assert result[0] == 1
    
    def test_database_version(self, db_connection):
        """Testa obtenção da versão do PostgreSQL"""
        cursor = db_connection.cursor()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()[0]
        cursor.close()
        
        assert "PostgreSQL" in version


class TestSchemaCreation:
    """Testes para criação de schemas e tabelas"""
    
    def test_create_schema(self, db_connection):
        """Testa criação de schema"""
        cursor = db_connection.cursor()
        
        # Criar schema
        cursor.execute("CREATE SCHEMA IF NOT EXISTS gold")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS serving")
        db_connection.commit()
        
        # Verificar schemas existem
        cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name IN ('gold', 'serving')
        """)
        schemas = [row[0] for row in cursor.fetchall()]
        cursor.close()
        
        assert 'gold' in schemas
        assert 'serving' in schemas
    
    def test_create_kpis_hourly_table(self, db_connection):
        """Testa criação da tabela de KPIs horários"""
        cursor = db_connection.cursor()
        
        # Criar schema se não existir
        cursor.execute("CREATE SCHEMA IF NOT EXISTS gold")
        
        # Criar tabela
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS gold.kpis_hourly (
                id SERIAL PRIMARY KEY,
                hour TIMESTAMP NOT NULL,
                route_code VARCHAR(50) NOT NULL,
                num_vehicles INTEGER,
                avg_speed DOUBLE PRECISION,
                total_passengers INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(hour, route_code)
            )
        """)
        db_connection.commit()
        
        # Verificar tabela foi criada
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'gold' AND table_name = 'kpis_hourly'
        """)
        result = cursor.fetchone()
        cursor.close()
        
        assert result is not None
        assert result[0] == 'kpis_hourly'
    
    def test_create_route_summary_table(self, db_connection):
        """Testa criação da tabela de resumo por rota"""
        cursor = db_connection.cursor()
        
        cursor.execute("CREATE SCHEMA IF NOT EXISTS gold")
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS gold.route_summary (
                route_code VARCHAR(50) PRIMARY KEY,
                total_trips INTEGER,
                avg_distance DOUBLE PRECISION,
                avg_duration DOUBLE PRECISION,
                avg_speed DOUBLE PRECISION,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        db_connection.commit()
        
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'gold' AND table_name = 'route_summary'
        """)
        result = cursor.fetchone()
        cursor.close()
        
        assert result is not None


class TestDataInsertion:
    """Testes para inserção de dados"""
    
    @pytest.fixture(autouse=True)
    def setup_tables(self, db_connection):
        """Setup: cria tabelas antes de cada teste"""
        cursor = db_connection.cursor()
        
        cursor.execute("CREATE SCHEMA IF NOT EXISTS gold")
        cursor.execute("DROP TABLE IF EXISTS gold.kpis_hourly CASCADE")
        cursor.execute("""
            CREATE TABLE gold.kpis_hourly (
                id SERIAL PRIMARY KEY,
                hour TIMESTAMP NOT NULL,
                route_code VARCHAR(50) NOT NULL,
                num_vehicles INTEGER,
                avg_speed DOUBLE PRECISION,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(hour, route_code)
            )
        """)
        db_connection.commit()
        cursor.close()
    
    def test_insert_single_record(self, db_connection):
        """Testa inserção de registro único"""
        cursor = db_connection.cursor()
        
        cursor.execute("""
            INSERT INTO gold.kpis_hourly (hour, route_code, num_vehicles, avg_speed)
            VALUES (%s, %s, %s, %s)
        """, (datetime(2025, 10, 29, 8, 0, 0), '8000-10', 10, 42.5))
        
        db_connection.commit()
        
        # Verificar inserção
        cursor.execute("SELECT COUNT(*) FROM gold.kpis_hourly")
        count = cursor.fetchone()[0]
        cursor.close()
        
        assert count == 1
    
    def test_insert_multiple_records(self, db_connection):
        """Testa inserção de múltiplos registros"""
        cursor = db_connection.cursor()
        
        records = [
            (datetime(2025, 10, 29, 8, 0, 0), '8000-10', 10, 42.5),
            (datetime(2025, 10, 29, 8, 0, 0), '8000-20', 8, 38.0),
            (datetime(2025, 10, 29, 9, 0, 0), '8000-10', 12, 45.0),
        ]
        
        cursor.executemany("""
            INSERT INTO gold.kpis_hourly (hour, route_code, num_vehicles, avg_speed)
            VALUES (%s, %s, %s, %s)
        """, records)
        
        db_connection.commit()
        
        cursor.execute("SELECT COUNT(*) FROM gold.kpis_hourly")
        count = cursor.fetchone()[0]
        cursor.close()
        
        assert count == 3
    
    def test_handle_duplicate_key(self, db_connection):
        """Testa tratamento de chave duplicada"""
        cursor = db_connection.cursor()
        
        # Inserir registro
        cursor.execute("""
            INSERT INTO gold.kpis_hourly (hour, route_code, num_vehicles, avg_speed)
            VALUES (%s, %s, %s, %s)
        """, (datetime(2025, 10, 29, 8, 0, 0), '8000-10', 10, 42.5))
        db_connection.commit()
        
        # Tentar inserir duplicata com ON CONFLICT
        cursor.execute("""
            INSERT INTO gold.kpis_hourly (hour, route_code, num_vehicles, avg_speed)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (hour, route_code) 
            DO UPDATE SET 
                num_vehicles = EXCLUDED.num_vehicles,
                avg_speed = EXCLUDED.avg_speed
        """, (datetime(2025, 10, 29, 8, 0, 0), '8000-10', 15, 45.0))
        db_connection.commit()
        
        # Verificar que ainda há apenas 1 registro, mas atualizado
        cursor.execute("""
            SELECT num_vehicles, avg_speed 
            FROM gold.kpis_hourly 
            WHERE hour = %s AND route_code = %s
        """, (datetime(2025, 10, 29, 8, 0, 0), '8000-10'))
        
        result = cursor.fetchone()
        cursor.close()
        
        assert result[0] == 15
        assert result[1] == 45.0


class TestSparkToPostgres:
    """Testes para carregamento de dados do Spark para PostgreSQL"""
    
    @pytest.fixture(autouse=True)
    def setup_table(self, db_connection):
        """Setup: cria tabela antes de cada teste"""
        cursor = db_connection.cursor()
        cursor.execute("CREATE SCHEMA IF NOT EXISTS gold")
        cursor.execute("DROP TABLE IF EXISTS gold.spark_test CASCADE")
        cursor.execute("""
            CREATE TABLE gold.spark_test (
                id SERIAL PRIMARY KEY,
                route_code VARCHAR(50),
                value DOUBLE PRECISION,
                timestamp TIMESTAMP
            )
        """)
        db_connection.commit()
        cursor.close()
    
    def test_spark_write_to_postgres(self, spark_session, db_config):
        """Testa escrita de DataFrame Spark para PostgreSQL"""
        # Criar DataFrame de teste
        schema = StructType([
            StructField("route_code", StringType(), False),
            StructField("value", DoubleType(), False),
            StructField("timestamp", TimestampType(), False),
        ])
        
        test_data = [
            ("8000-10", 42.5, datetime(2025, 10, 29, 8, 0, 0)),
            ("8000-20", 38.0, datetime(2025, 10, 29, 8, 0, 0)),
            ("8100-15", 45.0, datetime(2025, 10, 29, 8, 0, 0)),
        ]
        
        df = spark_session.createDataFrame(test_data, schema)
        
        # Escrever no PostgreSQL
        jdbc_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['database']}"
        
        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "gold.spark_test") \
            .option("user", db_config["user"]) \
            .option("password", db_config["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        # Verificar dados foram inseridos
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM gold.spark_test")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        
        assert count == 3
    
    def test_spark_read_from_postgres(self, spark_session, db_connection, db_config):
        """Testa leitura de dados do PostgreSQL para Spark"""
        # Inserir dados de teste
        cursor = db_connection.cursor()
        cursor.execute("""
            INSERT INTO gold.spark_test (route_code, value, timestamp)
            VALUES 
                ('8000-10', 42.5, %s),
                ('8000-20', 38.0, %s)
        """, (datetime(2025, 10, 29, 8, 0, 0), datetime(2025, 10, 29, 8, 0, 0)))
        db_connection.commit()
        cursor.close()
        
        # Ler com Spark
        jdbc_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['database']}"
        
        df = spark_session.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "gold.spark_test") \
            .option("user", db_config["user"]) \
            .option("password", db_config["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        assert df.count() == 2
        assert "route_code" in df.columns


class TestBulkOperations:
    """Testes para operações em lote"""
    
    def test_bulk_insert_performance(self, db_connection):
        """Testa performance de inserção em lote"""
        cursor = db_connection.cursor()
        
        cursor.execute("CREATE SCHEMA IF NOT EXISTS gold")
        cursor.execute("DROP TABLE IF EXISTS gold.bulk_test CASCADE")
        cursor.execute("""
            CREATE TABLE gold.bulk_test (
                id SERIAL PRIMARY KEY,
                value INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        db_connection.commit()
        
        # Inserir 1000 registros em lote
        import time
        start_time = time.time()
        
        records = [(i,) for i in range(1000)]
        cursor.executemany("INSERT INTO gold.bulk_test (value) VALUES (%s)", records)
        db_connection.commit()
        
        elapsed_time = time.time() - start_time
        
        # Verificar inserção
        cursor.execute("SELECT COUNT(*) FROM gold.bulk_test")
        count = cursor.fetchone()[0]
        cursor.close()
        
        assert count == 1000
        assert elapsed_time < 5.0  # Deve ser rápido (< 5 segundos)
    
    def test_truncate_and_load(self, db_connection):
        """Testa estratégia truncate and load"""
        cursor = db_connection.cursor()
        
        cursor.execute("CREATE SCHEMA IF NOT EXISTS gold")
        cursor.execute("DROP TABLE IF EXISTS gold.truncate_test CASCADE")
        cursor.execute("""
            CREATE TABLE gold.truncate_test (
                route_code VARCHAR(50) PRIMARY KEY,
                value INTEGER
            )
        """)
        
        # Inserir dados iniciais
        cursor.execute("INSERT INTO gold.truncate_test VALUES ('8000-10', 100)")
        db_connection.commit()
        
        # Truncate
        cursor.execute("TRUNCATE TABLE gold.truncate_test")
        
        # Carregar novos dados
        new_data = [('8000-20', 200), ('8000-30', 300)]
        cursor.executemany("INSERT INTO gold.truncate_test VALUES (%s, %s)", new_data)
        db_connection.commit()
        
        # Verificar
        cursor.execute("SELECT COUNT(*) FROM gold.truncate_test")
        count = cursor.fetchone()[0]
        
        cursor.execute("SELECT route_code FROM gold.truncate_test ORDER BY route_code")
        routes = [row[0] for row in cursor.fetchall()]
        cursor.close()
        
        assert count == 2
        assert '8000-10' not in routes
        assert '8000-20' in routes


class TestTransactionHandling:
    """Testes para gerenciamento de transações"""
    
    def test_commit_transaction(self, db_connection):
        """Testa commit de transação"""
        cursor = db_connection.cursor()
        
        cursor.execute("CREATE SCHEMA IF NOT EXISTS gold")
        cursor.execute("DROP TABLE IF EXISTS gold.trans_test CASCADE")
        cursor.execute("CREATE TABLE gold.trans_test (id INTEGER PRIMARY KEY, value TEXT)")
        
        # Inserir e commitar
        cursor.execute("INSERT INTO gold.trans_test VALUES (1, 'test')")
        db_connection.commit()
        
        # Verificar em nova conexão
        cursor.execute("SELECT value FROM gold.trans_test WHERE id = 1")
        result = cursor.fetchone()
        cursor.close()
        
        assert result[0] == 'test'
    
    def test_rollback_transaction(self, db_connection):
        """Testa rollback de transação"""
        cursor = db_connection.cursor()
        
        cursor.execute("CREATE SCHEMA IF NOT EXISTS gold")
        cursor.execute("DROP TABLE IF EXISTS gold.trans_test CASCADE")
        cursor.execute("CREATE TABLE gold.trans_test (id INTEGER PRIMARY KEY, value TEXT)")
        db_connection.commit()
        
        # Inserir mas não commitar
        cursor.execute("INSERT INTO gold.trans_test VALUES (1, 'test')")
        
        # Rollback
        db_connection.rollback()
        
        # Verificar que dado não foi inserido
        cursor.execute("SELECT COUNT(*) FROM gold.trans_test")
        count = cursor.fetchone()[0]
        cursor.close()
        
        assert count == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
