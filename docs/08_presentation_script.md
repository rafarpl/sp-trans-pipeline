# Roteiro de Apresentação – SPTrans Pipeline

**Duração:** 7–8 minutos  
**Público:** Banca Técnica  

## 1. Introdução (1 min)
“Bom dia, apresento o SPTrans Data Pipeline, uma solução de dados em near real-time que monitora e analisa o sistema de ônibus da cidade de São Paulo.”

## 2. Objetivo e Contexto (1 min)
“O objetivo é criar um produto de dados que consolide as informações da API Olho Vivo e do GTFS, processando-as a cada 2 minutos e gerando métricas de eficiência operacional.”

## 3. Arquitetura e Ferramentas (2 min)
“O pipeline é dividido em camadas: Raw, Silver e Gold. Utilizamos Airflow, Spark, MinIO, PostgreSQL e Grafana, garantindo escalabilidade e automação.”

## 4. Demonstração (2 min)
“Demonstro o Airflow executando as DAGs, os dados chegando ao Postgres e o dashboard no Grafana exibindo KPIs de velocidade média e ônibus ativos.”

## 5. Resultados e Próximos Passos (1–2 min)
“Com isso, obtemos visibilidade operacional em tempo quase real. Os próximos passos incluem integração com APIs públicas e métricas de ocupação via IoT.”

## 6. Encerramento
“Obrigado! Toda a documentação, código-fonte e arquitetura estão disponíveis no repositório.”
