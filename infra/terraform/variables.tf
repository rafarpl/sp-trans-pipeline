# =============================================================================
# TERRAFORM - VARIABLES
# =============================================================================
# Variáveis de configuração para infraestrutura SPTrans Pipeline
# =============================================================================

# =============================================================================
# GENERAL
# =============================================================================

variable "project_name" {
  description = "Nome do projeto (usado para nomear recursos)"
  type        = string
  default     = "sptrans-pipeline"
}

variable "environment" {
  description = "Ambiente (development, staging, production)"
  type        = string
  default     = "development"
  
  validation {
    condition     = contains(["development", "staging", "production"], var.environment)
    error_message = "Environment deve ser: development, staging ou production"
  }
}

variable "aws_region" {
  description = "Região AWS para deploy"
  type        = string
  default     = "us-east-1"
}

variable "tags" {
  description = "Tags adicionais para recursos"
  type        = map(string)
  default     = {}
}

# =============================================================================
# NETWORKING
# =============================================================================

variable "vpc_cidr" {
  description = "CIDR block para VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "private_subnet_cidrs" {
  description = "Lista de CIDRs para subnets privadas"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
}

variable "public_subnet_cidrs" {
  description = "Lista de CIDRs para subnets públicas"
  type        = list(string)
  default     = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]
}

# =============================================================================
# EKS CLUSTER
# =============================================================================

variable "kubernetes_version" {
  description = "Versão do Kubernetes para EKS"
  type        = string
  default     = "1.28"
}

variable "eks_node_instance_types" {
  description = "Tipos de instância para nodes do EKS"
  type        = list(string)
  default     = ["t3.large", "t3.xlarge"]
}

variable "eks_node_group_min_size" {
  description = "Número mínimo de nodes"
  type        = number
  default     = 2
}

variable "eks_node_group_max_size" {
  description = "Número máximo de nodes"
  type        = number
  default     = 10
}

variable "eks_node_group_desired_size" {
  description = "Número desejado de nodes"
  type        = number
  default     = 3
}

# =============================================================================
# RDS POSTGRESQL
# =============================================================================

variable "rds_instance_class" {
  description = "Classe de instância RDS"
  type        = string
  default     = "db.t3.medium"
  
  validation {
    condition     = can(regex("^db\\.", var.rds_instance_class))
    error_message = "Instance class deve começar com 'db.'"
  }
}

variable "rds_allocated_storage" {
  description = "Storage inicial para RDS (GB)"
  type        = number
  default     = 100
}

variable "rds_max_allocated_storage" {
  description = "Storage máximo para auto-scaling (GB)"
  type        = number
  default     = 500
}

variable "db_username" {
  description = "Username para RDS PostgreSQL"
  type        = string
  default     = "airflow"
  sensitive   = true
}

variable "db_password" {
  description = "Password para RDS PostgreSQL"
  type        = string
  sensitive   = true
  
  validation {
    condition     = length(var.db_password) >= 8
    error_message = "Password deve ter no mínimo 8 caracteres"
  }
}

variable "rds_backup_retention_period" {
  description = "Período de retenção de backups (dias)"
  type        = number
  default     = 7
}

variable "rds_multi_az" {
  description = "Habilitar Multi-AZ para RDS"
  type        = bool
  default     = false
}

# =============================================================================
# ELASTICACHE REDIS
# =============================================================================

variable "redis_node_type" {
  description = "Tipo de node para Redis"
  type        = string
  default     = "cache.t3.medium"
  
  validation {
    condition     = can(regex("^cache\\.", var.redis_node_type))
    error_message = "Node type deve começar com 'cache.'"
  }
}

variable "redis_num_cache_nodes" {
  description = "Número de nodes Redis"
  type        = number
  default     = 1
}

variable "redis_parameter_group_family" {
  description = "Family do parameter group Redis"
  type        = string
  default     = "redis7"
}

variable "redis_engine_version" {
  description = "Versão do Redis"
  type        = string
  default     = "7.0"
}

variable "redis_automatic_failover_enabled" {
  description = "Habilitar failover automático"
  type        = bool
  default     = false
}

# =============================================================================
# S3 DATA LAKE
# =============================================================================

variable "s3_bucket_prefix" {
  description = "Prefixo para nome do bucket S3"
  type        = string
  default     = "sptrans-datalake"
}

variable "s3_versioning_enabled" {
  description = "Habilitar versionamento S3"
  type        = bool
  default     = true
}

variable "s3_lifecycle_rules_enabled" {
  description = "Habilitar lifecycle rules no S3"
  type        = bool
  default     = true
}

variable "s3_bronze_retention_days" {
  description = "Dias para reter dados Bronze antes de mover para Glacier"
  type        = number
  default     = 90
}

variable "s3_bronze_expiration_days" {
  description = "Dias para expirar dados Bronze"
  type        = number
  default     = 365
}

# =============================================================================
# MONITORING & LOGGING
# =============================================================================

variable "enable_cloudwatch_logs" {
  description = "Habilitar CloudWatch Logs"
  type        = bool
  default     = true
}

variable "log_retention_days" {
  description = "Dias de retenção para logs"
  type        = number
  default     = 30
}

variable "enable_performance_insights" {
  description = "Habilitar Performance Insights para RDS"
  type        = bool
  default     = true
}

# =============================================================================
# COST OPTIMIZATION
# =============================================================================

variable "use_spot_instances" {
  description = "Usar Spot Instances para Spark workers"
  type        = bool
  default     = false
}

variable "enable_nat_gateway" {
  description = "Habilitar NAT Gateway (custo adicional)"
  type        = bool
  default     = true
}

variable "single_nat_gateway" {
  description = "Usar apenas 1 NAT Gateway (economia de custo)"
  type        = bool
  default     = true
}

# =============================================================================
# SECURITY
# =============================================================================

variable "enable_deletion_protection" {
  description = "Habilitar proteção contra deleção em recursos críticos"
  type        = bool
  default     = true
}

variable "enable_encryption_at_rest" {
  description = "Habilitar encryption at rest"
  type        = bool
  default     = true
}

variable "allowed_cidr_blocks" {
  description = "CIDRs permitidos para acesso externo"
  type        = list(string)
  default     = ["0.0.0.0/0"]  # ATENÇÃO: Restringir em produção
}

# =============================================================================
# AIRFLOW
# =============================================================================

variable "airflow_executor" {
  description = "Tipo de executor Airflow"
  type        = string
  default     = "LocalExecutor"
  
  validation {
    condition     = contains(["LocalExecutor", "CeleryExecutor", "KubernetesExecutor"], var.airflow_executor)
    error_message = "Executor deve ser: LocalExecutor, CeleryExecutor ou KubernetesExecutor"
  }
}

variable "airflow_webserver_replicas" {
  description = "Número de réplicas do Airflow webserver"
  type        = number
  default     = 2
}

variable "airflow_scheduler_replicas" {
  description = "Número de réplicas do Airflow scheduler"
  type        = number
  default     = 1
}

# =============================================================================
# SPARK
# =============================================================================

variable "spark_master_replicas" {
  description = "Número de réplicas Spark master"
  type        = number
  default     = 1
}

variable "spark_worker_replicas" {
  description = "Número inicial de workers Spark"
  type        = number
  default     = 3
}

variable "spark_worker_cores" {
  description = "Número de cores por worker Spark"
  type        = number
  default     = 2
}

variable "spark_worker_memory" {
  description = "Memória por worker Spark (ex: 4g)"
  type        = string
  default     = "4g"
}

# =============================================================================
# MINIO
# =============================================================================

variable "minio_replicas" {
  description = "Número de réplicas MinIO (mínimo 4 para distributed mode)"
  type        = number
  default     = 1
}

variable "minio_storage_size" {
  description = "Tamanho do storage MinIO (ex: 100Gi)"
  type        = string
  default     = "100Gi"
}

# =============================================================================
# BACKUP & DISASTER RECOVERY
# =============================================================================

variable "enable_automated_backups" {
  description = "Habilitar backups automáticos"
  type        = bool
  default     = true
}

variable "backup_retention_period" {
  description = "Período de retenção de backups (dias)"
  type        = number
  default     = 7
}

variable "enable_cross_region_backup" {
  description = "Habilitar backup cross-region"
  type        = bool
  default     = false
}

variable "backup_region" {
  description = "Região para backup cross-region"
  type        = string
  default     = "us-west-2"
}

# =============================================================================
# FEATURE FLAGS
# =============================================================================

variable "enable_superset" {
  description = "Deploy Superset para BI"
  type        = bool
  default     = true
}

variable "enable_grafana" {
  description = "Deploy Grafana para monitoring"
  type        = bool
  default     = true
}

variable "enable_prometheus" {
  description = "Deploy Prometheus para metrics"
  type        = bool
  default     = true
}

# =============================================================================
# LOCALS (valores computados)
# =============================================================================

locals {
  common_tags = merge(
    {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "Terraform"
      Owner       = "DataTeam"
    },
    var.tags
  )
  
  # Nome do cluster EKS
  eks_cluster_name = "${var.project_name}-${var.environment}-eks"
  
  # Nome do RDS
  rds_identifier = "${var.project_name}-${var.environment}-postgres"
  
  # Nome do Redis
  redis_cluster_id = "${var.project_name}-${var.environment}-redis"
  
  # Nome do bucket S3
  s3_bucket_name = "${var.s3_bucket_prefix}-${var.environment}"
}

# =============================================================================
# OUTPUTS DE VARIÁVEIS (para debugging)
# =============================================================================

output "configuration_summary" {
  description = "Resumo da configuração"
  value = {
    project_name = var.project_name
    environment  = var.environment
    aws_region   = var.aws_region
    vpc_cidr     = var.vpc_cidr
    kubernetes_version = var.kubernetes_version
  }
}

# =============================================================================
# END
# =============================================================================
