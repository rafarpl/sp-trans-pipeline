# 🏗️ Arquitetura Detalhada - SPTrans Real-Time Pipeline

> **Documento Técnico - Projeto de Conclusão FIA/LABDATA**  
> **Versão**: 1.0 | **Data**: Outubro 2025

---

## 📋 Índice

1. [Visão Geral](#1-visão-geral)
2. [Arquitetura de Referência](#2-arquitetura-de-referência)
3. [Camadas do Sistema](#3-camadas-do-sistema)
4. [Fluxo de Dados Detalhado](#4-fluxo-de-dados-detalhado)
5. [Decisões de Design](#5-decisões-de-design)
6. [Padrões Arquiteturais](#6-padrões-arquiteturais)
7. [Escalabilidade](#7-escalabilidade)
8. [Segurança](#8-segurança)
9. [Resiliência e Tolerância a Falhas](#9-resiliência-e-tolerância-a-falhas)
10. [Monitoramento e Observabilidade](#10-monitoramento-e-observabilidade)

---

## 1. Visão Geral

### 1.1. Contexto do Negócio

O sistema de transporte público de São Paulo opera com aproximadamente:
- **15.000 ônibus** em circulação simultânea
- **1.300 linhas** de ônibus
- **30.000 paradas** distribuídas pela cidade
- **Atualizações a cada 2 minutos** via API SPTrans

**Desafio**: Processar e disponibilizar métricas operacionais em near real-time para tomada de decisão.

### 1.2. Requisitos Funcionais

| ID | Requisito | Prioridade |
|----|-----------|------------|
| RF01 | Coletar posições de todos os ônibus a cada 2 minutos | ALTA |
| RF02 | Enriquecer dados com informações GTFS (rotas, paradas, horários) | ALTA |
| RF03 | Calcular KPIs: velocidade, headway, pontualidade, cobertura | ALTA |
| RF04 | Realizar geocoding reverso (lat/long → endereço) | MÉDIA |
| RF05 | Detectar anomalias e gerar alertas | MÉDIA |
| RF06 | Disponibilizar dashboards interativos | ALTA |
| RF07 | Armazenar histórico de 90 dias | MÉDIA |
| RF08 | API REST para consulta de dados | BAIXA |

### 1.3. Requisitos Não-Funcionais

| ID | Requisito | Métrica | Target |
|----|-----------|---------|--------|
| RNF01 | Latência de ingestão | P95 | < 30 segundos |
| RNF02 | Disponibilidade | Uptime | > 99.5% |
| RNF03 | Throughput | Registros/min | > 7.500 |
| RNF04 | Escalabilidade | Crescimento | +50% sem redesign |
| RNF05 | Qualidade de Dados | Completude | > 98% |
| RNF06 | Recovery Time Objective (RTO) | Tempo | < 15 minutos |
| RNF07 | Recovery Point Objective (RPO) | Perda de dados | < 10 minutos |

---

## 2. Arquitetura de Referência

### 2.1. Diagrama de Alto Nível

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                          CAMADA DE DADOS (DATA SOURCES)                       │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│   ┌────────────────────────┐              ┌────────────────────────┐         │
│   │  API SPTrans           │              │  GTFS SPTrans          │         │
│   │  (Olho Vivo)           │              │  (Static Data)         │         │
│   │                        │              │                        │         │
│   │  Endpoint: /Posicao    │              │  Files:                │         │
│   │  Format: JSON          │              │  - routes.txt          │         │
│   │  Frequency: 2 min      │              │  - trips.txt           │         │
│   │  ~15k vehicles         │              │  - stops.txt           │         │
│   └───────────┬────────────┘              │  - stop_times.txt      │         │
│               │                           │  - shapes.txt          │         │
│               │                           └───────────┬────────────┘         │
│               │                                       │                      │
└───────────────┼───────────────────────────────────────┼──────────────────────┘
                │                                       │
                │                                       │
┌───────────────┼───────────────────────────────────────┼──────────────────────┐
│               │        CAMADA DE ORQUESTRAÇÃO         │                      │
├───────────────┼───────────────────────────────────────┼──────────────────────┤
│               │                                       │                      │
│   ┌───────────▼───────────────────────────────────────▼──────────────────┐  │
│   │                      APACHE AIRFLOW 2.8.0                            │  │
│   │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐     │  │
│   │  │ DAG 1:          │  │ DAG 2:          │  │ DAG 3:          │     │  │
│   │  │ GTFS Ingestion  │  │ API Ingestion   │  │ Bronze→Silver   │     │  │
│   │  │ Schedule: Daily │  │ Schedule: 2min  │  │ Schedule: 10min │     │  │
│   │  │ 3:00 AM         │  │ Catchup: False  │  │ Depends: DAG2   │     │  │
│   │  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘     │  │
│   │           │                    │                    │              │  │
│   │  ┌────────▼────────┐  ┌────────▼────────┐  ┌────────▼────────┐     │  │
│   │  │ DAG 4:          │  │ DAG 5:          │  │ DAG 6:          │     │  │
│   │  │ Silver→Gold     │  │ Gold→Serving    │  │ Data Quality    │     │  │
│   │  │ Schedule: 15min │  │ Schedule: 15min │  │ Schedule: Hourly│     │  │
│   │  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘     │  │
│   │           │                    │                    │              │  │
│   │  ┌────────▼─────────────────────────────────────────▼─────────┐    │  │
│   │  │ DAG 7: Maintenance & Cleanup - Schedule: Daily 2:00 AM     │    │  │
│   │  └─────────────────────────────────────────────────────────────┘    │  │
│   └──────────────────────────────┬───────────────────────────────────────┘  │
│                                  │                                          │
└──────────────────────────────────┼──────────────────────────────────────────┘
                                   │
                                   │ Triggers Spark Jobs
                                   │
┌──────────────────────────────────▼──────────────────────────────────────────┐
│                    CAMADA DE PROCESSAMENTO (PROCESSING)                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌──────────────────────────────────────────────────────────────────────┐  │
│   │                     APACHE SPARK 3.5.0 (PySpark)                     │  │
│   │                                                                      │  │
│   │  Cluster Configuration:                                             │  │
│   │  ├── Master: 1 node (4 cores, 8GB RAM)                              │  │
│   │  ├── Workers: 2 nodes (4 cores each, 8GB RAM each)                  │  │
│   │  └── Total: 12 cores, 24GB RAM                                      │  │
│   │                                                                      │  │
│   │  ┌────────────────────────────────────────────────────────────────┐ │  │
│   │  │  SPARK JOB 1: API Ingestion to Bronze                          │ │  │
│   │  │  • Input: API SPTrans (HTTP REST)                              │ │  │
│   │  │  • Processing: Schema validation, timestamp addition           │ │  │
│   │  │  • Output: Parquet files partitioned by date/hour              │ │  │
│   │  │  • Frequency: Every 2 minutes                                  │ │  │
│   │  │  • Parallelism: 6 partitions                                   │ │  │
│   │  └────────────────────────────────────────────────────────────────┘ │  │
│   │                                                                      │  │
│   │  ┌────────────────────────────────────────────────────────────────┐ │  │
│   │  │  SPARK JOB 2: GTFS Ingestion to Bronze                         │ │  │
│   │  │  • Input: GTFS CSV files                                       │ │  │
│   │  │  • Processing: CSV parsing, schema enforcement                 │ │  │
│   │  │  • Output: Parquet files (one per GTFS file)                   │ │  │
│   │  │  • Frequency: Daily (3 AM)                                     │ │  │
│