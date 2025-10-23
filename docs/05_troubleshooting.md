# 🔧 Troubleshooting Guide - SPTrans Pipeline

## Guia de Solução de Problemas Comuns

---

## 🚨 Problemas de Infraestrutura

### 1. Docker Compose não inicia

**Sintoma:**
```bash
ERROR: Couldn't connect to Docker daemon
```

**Soluções:**
```bash
# Verificar se Docker está rodando
sudo systemctl status docker

# Iniciar Docker
sudo systemctl start docker

# Adicionar usuário ao grupo docker
sudo usermod -aG docker $USER
newgrp docker

# Testar
docker ps
```

---

### 2. Containers crasham ao iniciar

**Sintoma:**
```bash
Container sptrans-airflow exited with code 1
```

**Diagnóstico:**
```bash
# Ver logs do container
docker logs sptrans-airflow

# Ver logs em tempo real
docker logs -f sptrans-airflow

# Inspecionar container
docker inspect sptrans-airflow
```

**Soluções comuns:**
- **Porta já em uso**: Alterar portas no `docker-compose.yml`
- **Memória insuficiente**: Aumentar memória do Docker (Settings > Resources)
- **Variáveis de ambiente faltando**: Verificar arquivo `.env`

---

### 3. Erro de permissão em volumes

**Sintoma:**
```bash
PermissionError: [Errno 13] Permission denied: '/opt/airflow/logs'
```

**Solução:**
```bash
# Criar diretórios com permissões corretas
mkdir -p logs/airflow logs/spark data/gtfs
chmod -R 777 logs data

# Ou usar o script de setup
./scripts/setup.sh
```

---

## 🌐 Problemas de Rede

### 4. Containers não se comunicam

**Sintoma:**
```
Could not connect to PostgreSQL at postgres:5432
```

**Diagnóstico:**
```bash
# Verificar rede Docker
docker network ls
docker network inspect sptrans-network

# Testar conectividade
docker exec sptrans-airflow ping postgres
docker exec sptrans-airflow nc -zv postgres 5432
```

**Solução:**
```bash
# Recriar rede
docker network rm sptrans-network
docker network create sptrans-network

# Reiniciar containers
docker-compose down
docker-compose up -d
```

---

### 5. API SPTrans não responde

**Sintoma:**
```python
requests.exceptions.ConnectionError: Failed to establish connection
```

**Diagnóstico:**
```bash
# Testar conectividade
curl -X POST "http://api.olhovivo.sptrans.com.br/v2.1/Login/Autenticar?token=SEU_TOKEN"

# Verificar DNS
nslookup api.olhovivo.sptrans.com.br

# Testar com timeout maior
curl --max-time 30 "http://api.olhovivo.sptrans.com.br/v2.1/Posicao"
```

**Soluções:**
1. **Token inválido**: Verificar token no portal SPTrans
2. **Firewall bloqueando**: Liberar porta 80/443
3. **Rate limit**: Aguardar alguns minutos
4. **API fora do ar**: Verificar status no portal SPTrans

---

## 💾 Problemas com MinIO (Data Lake)

### 6. MinIO não inicia

**Sintoma:**
```
Unable to connect to MinIO at minio:9000
```

**Diagnóstico:**
```bash
# Verificar status
docker ps | grep minio

# Ver logs
docker logs minio

# Testar acesso
curl http://localhost:9000/minio/health/live
```

**Solução:**
```bash
# Verificar credenciais no .env
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=miniopassword123

# Recriar volume se necessário
docker-compose down -v
docker volume rm sptrans-minio-data
docker-compose up -d minio
```

---

### 7. Erro ao criar bucket

**Sintoma:**
```python
S3Error: Access Denied
```

**Solução:**
```bash
# Acessar MinIO Console
# http://localhost:9001
# Usuário: admin / Senha: miniopassword123

# Criar bucket manualmente ou via script
docker exec minio mc mb minio/sptrans-datalake
docker exec minio mc policy set public minio/sptrans-datalake
```

---

## ⚡ Problemas com Spark

### 8. Spark job falha com OutOfMemory

**Sintoma:**
```
java.lang.OutOfMemoryError: Java heap space
```

**Solução 1 - Aumentar memória do executor:**
```python
# No código Spark
spark = SparkSession.builder \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()
```

**Solução 2 - Aumentar no docker-compose:**
```yaml
spark-master:
  environment:
    - SPARK_EXECUTOR_MEMORY=4G
    - SPARK_DRIVER_MEMORY=2G
  deploy:
    resources:
      limits:
        memory: 8G
```

---

### 9. Spark não conecta ao MinIO

**Sintoma:**
```
java.io.IOException: No FileSystem for scheme: s3a
```

**Solução:**
```python
# Adicionar JARs do Hadoop-AWS
spark = SparkSession.builder \
    .config("spark.jars.packages", 
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "miniopassword123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
```

---

## 🔄 Problemas com Airflow

### 10. DAGs não aparecem na UI

**Sintoma:**
```
No DAGs found in /opt/airflow/dags
```

**Diagnóstico:**
```bash
# Verificar montagem do volume
docker exec sptrans-airflow ls -la /opt/airflow/dags

# Verificar parsing de DAGs
docker exec sptrans-airflow airflow dags list

# Ver erros de parsing
docker logs sptrans-airflow | grep -i error
```

**Solução:**
```bash
# Verificar sintaxe Python
python dags/dag_02_api_ingestion.py

# Forçar reload de DAGs
docker exec sptrans-airflow airflow dags reserialize

# Reiniciar scheduler
docker-compose restart airflow-scheduler
```

---

### 11. Task fica preso em "running"

**Sintoma:**
```
Task has been running for more than 1 hour
```

**Diagnóstico:**
```bash
# Ver logs da task
docker exec sptrans-airflow airflow tasks logs dag_02_api_ingestion ingest_api_positions 2025-01-20

# Verificar processos
docker exec sptrans-airflow ps aux | grep python
```

**Solução:**
```bash
# Matar task travada
docker exec sptrans-airflow airflow tasks clear dag_02_api_ingestion -t ingest_api_positions

# Aumentar timeout no DAG
default_args = {
    'execution_timeout': timedelta(hours=2)
}
```

---

## 🗄️ Problemas com PostgreSQL

### 12. Não consegue conectar ao banco

**Sintoma:**
```
psql: error: connection to server at "postgres" (172.18.0.2), port 5432 failed
```

**Diagnóstico:**
```bash
# Verificar se está rodando
docker ps | grep postgres

# Testar conexão
docker exec -it postgres psql -U airflow -d airflow

# Ver logs
docker logs postgres
```

**Solução:**
```bash
# Recriar container
docker-compose down postgres
docker volume rm sptrans-postgres-data
docker-compose up -d postgres

# Aguardar inicialização
sleep 10

# Executar scripts SQL
docker exec -i postgres psql -U airflow -d airflow < sql/01_serving_schema.sql
```

---

### 13. Tabela não existe

**Sintoma:**
```sql
ERROR: relation "serving.bus_positions" does not exist
```

**Solução:**
```bash
# Executar scripts de criação na ordem
cd sql/
docker exec -i postgres psql -U airflow -d airflow < 00_create_databases.sql
docker exec -i postgres psql -U airflow -d airflow < 01_serving_schema.sql
docker exec -i postgres psql -U airflow -d airflow < 02_serving_tables.sql

# Verificar tabelas criadas
docker exec -it postgres psql -U airflow -d airflow -c "\dt serving.*"
```

---

## 🐍 Problemas com Python

### 14. Módulo não encontrado

**Sintoma:**
```python
ModuleNotFoundError: No module named 'pyspark'
```

**Solução:**
```bash
# Instalar dependências
pip install -r requirements.txt

# Ou no container
docker exec sptrans-airflow pip install -r /opt/airflow/requirements.txt

# Reconstruir imagem
docker-compose build --no-cache
```

---

### 15. Erro de importação circular

**Sintoma:**
```python
ImportError: cannot import name 'SPTransClient' from partially initialized module
```

**Solução:**
```python
# Reorganizar imports
# Evitar:
from src.ingestion.sptrans_api_client import SPTransClient

# Preferir:
import src.ingestion.sptrans_api_client as api_client
client = api_client.SPTransClient(token)
```

---

## 📊 Problemas de Dados

### 16. Dados duplicados no Data Lake

**Sintoma:**
```
Duplicate key violation in bronze layer
```

**Solução:**
```python
# Adicionar deduplicação
df = df.dropDuplicates(['vehicle_id', 'timestamp'])

# Ou usar Delta Lake com merge
df.write \
    .format("delta") \
    .mode("merge") \
    .option("mergeSchema", "true") \
    .save("s3a://sptrans-datalake/silver/positions/")
```

---

### 17. Coordenadas inválidas

**Sintoma:**
```
Latitude/Longitude outside São Paulo bounds
```

**Solução:**
```python
# Validar coordenadas
def is_valid_coordinate(lat, lon):
    # Limites aproximados de São Paulo
    return (-24.0 <= lat <= -23.0) and (-47.0 <= lon <= -46.0)

# Filtrar dados
df = df.filter(
    (df.latitude >= -24.0) & (df.latitude <= -23.0) &
    (df.longitude >= -47.0) & (df.longitude <= -46.0)
)
```

---

## 🔍 Comandos Úteis para Debug

### Verificar status geral
```bash
# Status de todos containers
docker-compose ps

# Uso de recursos
docker stats

# Logs de todos serviços
docker-compose logs -f
```

### Acessar containers
```bash
# Airflow
docker exec -it sptrans-airflow bash

# Spark
docker exec -it spark-master bash

# PostgreSQL
docker exec -it postgres psql -U airflow

# MinIO
docker exec -it minio bash
```

### Limpar tudo e recomeçar
```bash
# CUIDADO: Remove todos dados!
docker-compose down -v
docker system prune -a
rm -rf logs/* data/*

# Recriar do zero
./scripts/setup.sh
docker-compose up -d
```

---

## 📞 Suporte

### Canais de Suporte
- **Issues GitHub**: Para bugs e features
- **Email SPTrans**: desenvolvedores@sptrans.com.br (problemas com API)
- **Documentação**: Consultar `/docs` do projeto

### Logs Importantes
1. `/logs/airflow/scheduler/*.log` - Airflow scheduler
2. `/logs/spark/*.log` - Jobs Spark
3. `docker logs <container>` - Logs dos containers

---

**Última atualização**: Janeiro 2025
