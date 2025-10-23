"""
PostgreSQL Loader Module
========================
Carrega dados do Data Lake para PostgreSQL (Serving Layer).

Otimizado para:
- Bulk inserts
- Upserts (merge)
- Gestão de conexões
- Performance
"""

from typing import List, Dict, Any, Optional
import psycopg2
from psycopg2.extras import execute_batch, execute_values
from psycopg2.extensions import connection
import pandas as pd
from contextlib import contextmanager

from src.common.logging_config import get_logger
from src.common.config import Config
from src.common.metrics import track_storage_operation

logger = get_logger(__name__)


class PostgresLoader:
    """Carregador de dados para PostgreSQL."""
    
    def __init__(self, config: Optional[Config] = None):
        """
        Inicializa loader.
        
        Args:
            config: Configurações (usa Config() se None)
        """
        self.config = config or Config()
        
        self.conn_params = {
            'host': self.config.POSTGRES_HOST,
            'port': self.config.POSTGRES_PORT,
            'database': self.config.POSTGRES_DB,
            'user': self.config.POSTGRES_USER,
            'password': self.config.POSTGRES_PASSWORD
        }
        
        logger.info(f"PostgresLoader inicializado: {self.config.POSTGRES_HOST}:{self.config.POSTGRES_PORT}")
    
    @contextmanager
    def get_connection(self) -> connection:
        """
        Context manager para conexão PostgreSQL.
        
        Yields:
            Conexão PostgreSQL
        """
        conn = None
        try:
            conn = psycopg2.connect(**self.conn_params)
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Erro na conexão PostgreSQL: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    @track_storage_operation('write', 'serving')
    def load_dataframe(self, 
                      df: pd.DataFrame,
                      table_name: str,
                      schema: str = 'serving',
                      if_exists: str = 'append',
                      batch_size: int = 1000) -> int:
        """
        Carrega DataFrame do pandas para PostgreSQL.
        
        Args:
            df: DataFrame para carregar
            table_name: Nome da tabela
            schema: Schema do PostgreSQL
            if_exists: 'append', 'replace' ou 'fail'
            batch_size: Tamanho do batch para inserção
        
        Returns:
            Número de registros carregados
        """
        logger.info(f"Carregando DataFrame para {schema}.{table_name}")
        
        if df.empty:
            logger.warning("DataFrame vazio, nada para carregar")
            return 0
        
        record_count = len(df)
        logger.info(f"Registros para carregar: {record_count:,}")
        
        try:
            # Criar connection string para pandas
            conn_string = (
                f"postgresql://{self.config.POSTGRES_USER}:{self.config.POSTGRES_PASSWORD}"
                f"@{self.config.POSTGRES_HOST}:{self.config.POSTGRES_PORT}/{self.config.POSTGRES_DB}"
            )
            
            # Carregar usando pandas to_sql
            df.to_sql(
                name=table_name,
                con=conn_string,
                schema=schema,
                if_exists=if_exists,
                index=False,
                method='multi',
                chunksize=batch_size
            )
            
            logger.info(f"DataFrame carregado com sucesso: {record_count:,} registros")
            
            return record_count
        
        except Exception as e:
            logger.error(f"Erro ao carregar DataFrame: {e}")
            raise
    
    @track_storage_operation('write', 'serving')
    def bulk_insert(self,
                   table_name: str,
                   columns: List[str],
                   data: List[tuple],
                   schema: str = 'serving',
                   batch_size: int = 1000) -> int:
        """
        Inserção em lote (bulk insert) usando execute_values.
        
        Args:
            table_name: Nome da tabela
            columns: Lista de colunas
            data: Lista de tuplas com dados
            schema: Schema do PostgreSQL
            batch_size: Tamanho do batch
        
        Returns:
            Número de registros inseridos
        """
        logger.info(f"Bulk insert em {schema}.{table_name}")
        
        if not data:
            logger.warning("Nenhum dado para inserir")
            return 0
        
        record_count = len(data)
        logger.info(f"Registros para inserir: {record_count:,}")
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Construir query
                cols = ', '.join(columns)
                placeholders = ', '.join(['%s'] * len(columns))
                query = f"INSERT INTO {schema}.{table_name} ({cols}) VALUES %s"
                
                # Executar em batches
                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    execute_values(cursor, query, batch)
                    
                    if (i + batch_size) % 10000 == 0:
                        logger.info(f"Progresso: {i + batch_size:,}/{record_count:,}")
                
                cursor.close()
            
            logger.info(f"Bulk insert concluído: {record_count:,} registros")
            
            return record_count
        
        except Exception as e:
            logger.error(f"Erro no bulk insert: {e}")
            raise
    
    @track_storage_operation('write', 'serving')
    def upsert(self,
              table_name: str,
              columns: List[str],
              data: List[tuple],
              conflict_columns: List[str],
              update_columns: List[str],
              schema: str = 'serving',
              batch_size: int = 1000) -> int:
        """
        Upsert (INSERT ... ON CONFLICT DO UPDATE).
        
        Args:
            table_name: Nome da tabela
            columns: Lista de todas as colunas
            data: Lista de tuplas com dados
            conflict_columns: Colunas da constraint única
            update_columns: Colunas para atualizar em caso de conflito
            schema: Schema do PostgreSQL
            batch_size: Tamanho do batch
        
        Returns:
            Número de registros processados
        """
        logger.info(f"Upsert em {schema}.{table_name}")
        
        if not data:
            logger.warning("Nenhum dado para upsert")
            return 0
        
        record_count = len(data)
        logger.info(f"Registros para upsert: {record_count:,}")
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Construir query de upsert
                cols = ', '.join(columns)
                placeholders = ', '.join(['%s'] * len(columns))
                conflict_cols = ', '.join(conflict_columns)
                update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
                
                query = f"""
                    INSERT INTO {schema}.{table_name} ({cols})
                    VALUES ({placeholders})
                    ON CONFLICT ({conflict_cols})
                    DO UPDATE SET {update_set}
                """
                
                # Executar em batches
                execute_batch(cursor, query, data, page_size=batch_size)
                
                cursor.close()
            
            logger.info(f"Upsert concluído: {record_count:,} registros")
            
            return record_count
        
        except Exception as e:
            logger.error(f"Erro no upsert: {e}")
            raise
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[tuple]:
        """
        Executa query e retorna resultados.
        
        Args:
            query: SQL query
            params: Parâmetros da query
        
        Returns:
            Lista de tuplas com resultados
        """
        logger.info("Executando query")
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                results = cursor.fetchall()
                cursor.close()
            
            logger.info(f"Query executada: {len(results)} registros retornados")
            
            return results
        
        except Exception as e:
            logger.error(f"Erro ao executar query: {e}")
            raise
    
    def execute_many(self, query: str, data: List[tuple]) -> int:
        """
        Executa query múltiplas vezes (batch).
        
        Args:
            query: SQL query
            data: Lista de tuplas com parâmetros
        
        Returns:
            Número de registros afetados
        """
        logger.info("Executando query em batch")
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.executemany(query, data)
                affected = cursor.rowcount
                cursor.close()
            
            logger.info(f"Batch executado: {affected} registros afetados")
            
            return affected
        
        except Exception as e:
            logger.error(f"Erro ao executar batch: {e}")
            raise
    
    def table_exists(self, table_name: str, schema: str = 'serving') -> bool:
        """
        Verifica se tabela existe.
        
        Args:
            table_name: Nome da tabela
            schema: Schema do PostgreSQL
        
        Returns:
            True se tabela existe
        """
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_name = %s
            )
        """
        
        try:
            results = self.execute_query(query, (schema, table_name))
            return results[0][0] if results else False
        
        except Exception as e:
            logger.error(f"Erro ao verificar tabela: {e}")
            return False
    
    def get_table_count(self, table_name: str, schema: str = 'serving') -> int:
        """
        Retorna número de registros na tabela.
        
        Args:
            table_name: Nome da tabela
            schema: Schema do PostgreSQL
        
        Returns:
            Número de registros
        """
        query = f"SELECT COUNT(*) FROM {schema}.{table_name}"
        
        try:
            results = self.execute_query(query)
            return results[0][0] if results else 0
        
        except Exception as e:
            logger.error(f"Erro ao contar registros: {e}")
            return 0
    
    def truncate_table(self, table_name: str, schema: str = 'serving'):
        """
        Trunca tabela (remove todos os dados).
        
        Args:
            table_name: Nome da tabela
            schema: Schema do PostgreSQL
        """
        logger.warning(f"Truncando tabela {schema}.{table_name}")
        
        query = f"TRUNCATE TABLE {schema}.{table_name} RESTART IDENTITY CASCADE"
        
        try:
            self.execute_query(query)
            logger.info(f"Tabela truncada: {schema}.{table_name}")
        
        except Exception as e:
            logger.error(f"Erro ao truncar tabela: {e}")
            raise
    
    def refresh_materialized_view(self, view_name: str, schema: str = 'serving'):
        """
        Atualiza view materializada.
        
        Args:
            view_name: Nome da view
            schema: Schema do PostgreSQL
        """
        logger.info(f"Atualizando view materializada: {schema}.{view_name}")
        
        query = f"REFRESH MATERIALIZED VIEW CONCURRENTLY {schema}.{view_name}"
        
        try:
            self.execute_query(query)
            logger.info(f"View atualizada: {schema}.{view_name}")
        
        except Exception as e:
            logger.error(f"Erro ao atualizar view: {e}")
            raise
    
    def vacuum_analyze(self, table_name: str, schema: str = 'serving'):
        """
        Executa VACUUM ANALYZE na tabela.
        
        Args:
            table_name: Nome da tabela
            schema: Schema do PostgreSQL
        """
        logger.info(f"Executando VACUUM ANALYZE em {schema}.{table_name}")
        
        try:
            with self.get_connection() as conn:
                # VACUUM requer autocommit
                old_isolation = conn.isolation_level
                conn.set_isolation_level(0)
                
                cursor = conn.cursor()
                cursor.execute(f"VACUUM ANALYZE {schema}.{table_name}")
                cursor.close()
                
                conn.set_isolation_level(old_isolation)
            
            logger.info("VACUUM ANALYZE concluído")
        
        except Exception as e:
            logger.error(f"Erro ao executar VACUUM: {e}")
            raise


if __name__ == "__main__":
    # Teste do módulo
    print("=== Testando PostgresLoader ===\n")
    
    # Criar loader
    loader = PostgresLoader()
    
    print("1. Testando conexão...")
    try:
        with loader.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT version()")
            version = cursor.fetchone()
            print(f"   ✅ Conectado: {version[0][:50]}...")
            cursor.close()
    except Exception as e:
        print(f"   ❌ Erro: {e}")
    
    print("\n2. Testando verificação de tabela...")
    exists = loader.table_exists('kpis_hourly', 'serving')
    print(f"   Tabela serving.kpis_hourly existe: {exists}")
    
    if exists:
        print("\n3. Testando contagem de registros...")
        count = loader.get_table_count('kpis_hourly', 'serving')
        print(f"   ✅ Registros na tabela: {count:,}")
    
    print("\n✅ Testes concluídos!")
