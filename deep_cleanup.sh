#!/bin/bash

echo "Limpeza iniciada..."

# Backup
tar -czf backup_$(date +%Y%m%d_%H%M%S).tar.gz --exclude='venv' --exclude='.git' --exclude='__pycache__' . 2>/dev/null
echo "Backup criado"

# Cache Python
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null
find . -name "*.pyc" -delete 2>/dev/null
find . -name "*.pyo" -delete 2>/dev/null
find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null
echo "Cache removido"

# Diretórios não usados
rm -rf dags/ config/airflow/ config/prometheus/
rm -rf infra/kubernetes/ infra/terraform/
rm -rf dashboards/superset/
rm -rf data/bronze/ data/silver/ data/gold/ data/serving/ data/backups/
echo "Diretorios removidos"

# Arquivos desnecessários
rm -f Dockerfile Makefile setup-vm.sh setup.py
rm -f infra/docker/airflow.dockerfile infra/docker/superset.Dockerfile
find . -name "*.backup" -delete 2>/dev/null
find . -name "*.bak" -delete 2>/dev/null
echo "Arquivos removidos"

# Criar .gitignore
cat > .gitignore << 'END'
__pycache__/
*.py[cod]
venv/
.env
.DS_Store
*.log
data/bronze/
data/silver/
data/gold/
*.parquet
.pytest_cache/
backup_*.tar.gz
!data/samples/
!data/visualizations/
END

echo "gitignore criado"

# Remover do Git
git rm -r --cached __pycache__/ 2>/dev/null
git rm -r --cached venv/ 2>/dev/null
git rm -r --cached dags/ 2>/dev/null
git rm -r --cached data/bronze/ data/silver/ data/gold/ 2>/dev/null

echo "CONCLUIDO!"
echo "Arquivos pyc: $(find . -name '*.pyc' 2>/dev/null | wc -l)"
echo "Tamanho: $(du -sh --exclude=venv --exclude=.git . 2>/dev/null)"