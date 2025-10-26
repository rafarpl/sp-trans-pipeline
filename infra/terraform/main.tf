# =============================================================================
# TERRAFORM - MAIN CONFIGURATION
# =============================================================================
# Infrastructure as Code para SPTrans Pipeline
# Provider: AWS (pode ser adaptado para GCP, Azure, etc)
# =============================================================================

terraform {
  required_version = ">= 1.5.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.23"
    }
    
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.11"
    }
  }
  
  # Backend para armazenar state (remoto)
  backend "s3" {
    bucket         = "sptrans-terraform-state"
    key            = "pipeline/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "terraform-state-lock"
  }
}

# =============================================================================
# PROVIDER CONFIGURATION
# =============================================================================

provider "aws" {
  region = var.aws_region
  
  default_tags {
    tags = {
      Project     = "SPTrans Pipeline"
      Environment = var.environment
      ManagedBy   = "Terraform"
      Owner       = "DataTeam"
    }
  }
}

provider "kubernetes" {
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
  
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args = [
      "eks",
      "get-token",
      "--cluster-name",
      module.eks.cluster_name
    ]
  }
}

provider "helm" {
  kubernetes {
    host                   = module.eks.cluster_endpoint
    cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
    
    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args = [
        "eks",
        "get-token",
        "--cluster-name",
        module.eks.cluster_name
      ]
    }
  }
}

# =============================================================================
# DATA SOURCES
# =============================================================================

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_caller_identity" "current" {}

# =============================================================================
# VPC MODULE
# =============================================================================

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.0"
  
  name = "${var.project_name}-vpc"
  cidr = var.vpc_cidr
  
  azs             = slice(data.aws_availability_zones.available.names, 0, 3)
  private_subnets = var.private_subnet_cidrs
  public_subnets  = var.public_subnet_cidrs
  
  enable_nat_gateway   = true
  single_nat_gateway   = var.environment == "development"
  enable_dns_hostnames = true
  enable_dns_support   = true
  
  # Tags para EKS
  public_subnet_tags = {
    "kubernetes.io/role/elb" = "1"
    "kubernetes.io/cluster/${var.project_name}-eks" = "shared"
  }
  
  private_subnet_tags = {
    "kubernetes.io/role/internal-elb" = "1"
    "kubernetes.io/cluster/${var.project_name}-eks" = "shared"
  }
  
  tags = {
    Name = "${var.project_name}-vpc"
  }
}

# =============================================================================
# EKS CLUSTER
# =============================================================================

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 19.0"
  
  cluster_name    = "${var.project_name}-eks"
  cluster_version = var.kubernetes_version
  
  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets
  
  # Cluster endpoint access
  cluster_endpoint_public_access  = true
  cluster_endpoint_private_access = true
  
  # Cluster addons
  cluster_addons = {
    coredns = {
      most_recent = true
    }
    kube-proxy = {
      most_recent = true
    }
    vpc-cni = {
      most_recent = true
    }
    aws-ebs-csi-driver = {
      most_recent = true
    }
  }
  
  # EKS Managed Node Groups
  eks_managed_node_groups = {
    
    # Node group para workloads gerais
    general = {
      name = "general-node-group"
      
      instance_types = ["t3.large"]
      capacity_type  = "ON_DEMAND"
      
      min_size     = 2
      max_size     = 10
      desired_size = 3
      
      labels = {
        workload = "general"
      }
      
      tags = {
        Name = "general-node-group"
      }
    }
    
    # Node group para Spark (compute-intensive)
    spark = {
      name = "spark-node-group"
      
      instance_types = ["m5.xlarge"]
      capacity_type  = "SPOT"  # Usar SPOT para reduzir custos
      
      min_size     = 1
      max_size     = 5
      desired_size = 2
      
      labels = {
        workload = "spark"
      }
      
      taints = [{
        key    = "workload"
        value  = "spark"
        effect = "NO_SCHEDULE"
      }]
      
      tags = {
        Name = "spark-node-group"
      }
    }
  }
  
  # Manage aws-auth ConfigMap
  manage_aws_auth_configmap = true
  
  tags = {
    Name = "${var.project_name}-eks"
  }
}

# =============================================================================
# RDS POSTGRESQL (Airflow Metadata & Serving Layer)
# =============================================================================

module "rds_postgresql" {
  source  = "terraform-aws-modules/rds/aws"
  version = "~> 6.0"
  
  identifier = "${var.project_name}-postgres"
  
  engine               = "postgres"
  engine_version       = "15.4"
  family               = "postgres15"
  major_engine_version = "15"
  instance_class       = var.rds_instance_class
  
  allocated_storage     = 100
  max_allocated_storage = 500
  storage_encrypted     = true
  
  db_name  = "airflow"
  username = var.db_username
  password = var.db_password
  port     = 5432
  
  multi_az               = var.environment == "production"
  db_subnet_group_name   = module.vpc.database_subnet_group_name
  vpc_security_group_ids = [aws_security_group.rds.id]
  
  # Backups
  backup_retention_period = 7
  backup_window           = "03:00-04:00"
  maintenance_window      = "Mon:04:00-Mon:05:00"
  
  # Monitoring
  enabled_cloudwatch_logs_exports = ["postgresql", "upgrade"]
  create_cloudwatch_log_group     = true
  
  # Performance Insights
  performance_insights_enabled = true
  
  # Deletion protection
  deletion_protection = var.environment == "production"
  skip_final_snapshot = var.environment != "production"
  
  tags = {
    Name = "${var.project_name}-postgres"
  }
}

# =============================================================================
# ELASTICACHE REDIS (Cache & Celery Broker)
# =============================================================================

module "elasticache_redis" {
  source  = "terraform-aws-modules/elasticache/aws"
  version = "~> 1.0"
  
  replication_group_id = "${var.project_name}-redis"
  description          = "Redis for SPTrans Pipeline cache and message broker"
  
  engine_version = "7.0"
  node_type      = var.redis_node_type
  num_cache_nodes = var.environment == "production" ? 3 : 1
  
  automatic_failover_enabled = var.environment == "production"
  multi_az_enabled           = var.environment == "production"
  
  subnet_group_name  = module.vpc.elasticache_subnet_group_name
  security_group_ids = [aws_security_group.redis.id]
  
  # Backups
  snapshot_retention_limit = 7
  snapshot_window          = "03:00-05:00"
  
  # Maintenance
  maintenance_window = "Mon:05:00-Mon:07:00"
  
  tags = {
    Name = "${var.project_name}-redis"
  }
}

# =============================================================================
# S3 BUCKET (Data Lake Alternative - pode usar em conjunto com MinIO)
# =============================================================================

module "s3_datalake" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "~> 3.0"
  
  bucket = "${var.project_name}-datalake-${data.aws_caller_identity.current.account_id}"
  
  # Versionamento
  versioning = {
    enabled = true
  }
  
  # Encryption
  server_side_encryption_configuration = {
    rule = {
      apply_server_side_encryption_by_default = {
        sse_algorithm = "AES256"
      }
    }
  }
  
  # Lifecycle rules
  lifecycle_rule = [
    {
      id      = "bronze-transition"
      enabled = true
      
      prefix = "bronze/"
      
      transition = [
        {
          days          = 90
          storage_class = "STANDARD_IA"
        },
        {
          days          = 180
          storage_class = "GLACIER"
        }
      ]
      
      expiration = {
        days = 365
      }
    },
    {
      id      = "silver-transition"
      enabled = true
      
      prefix = "silver/"
      
      transition = [
        {
          days          = 180
          storage_class = "STANDARD_IA"
        }
      ]
    }
  ]
  
  # Block public access
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
  
  tags = {
    Name = "${var.project_name}-datalake"
  }
}

# =============================================================================
# SECURITY GROUPS
# =============================================================================

# Security Group para RDS
resource "aws_security_group" "rds" {
  name        = "${var.project_name}-rds-sg"
  description = "Security group for RDS PostgreSQL"
  vpc_id      = module.vpc.vpc_id
  
  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = module.vpc.private_subnets_cidr_blocks
    description = "PostgreSQL from private subnets"
  }
  
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "All outbound traffic"
  }
  
  tags = {
    Name = "${var.project_name}-rds-sg"
  }
}

# Security Group para Redis
resource "aws_security_group" "redis" {
  name        = "${var.project_name}-redis-sg"
  description = "Security group for ElastiCache Redis"
  vpc_id      = module.vpc.vpc_id
  
  ingress {
    from_port   = 6379
    to_port     = 6379
    protocol    = "tcp"
    cidr_blocks = module.vpc.private_subnets_cidr_blocks
    description = "Redis from private subnets"
  }
  
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "All outbound traffic"
  }
  
  tags = {
    Name = "${var.project_name}-redis-sg"
  }
}

# =============================================================================
# IAM ROLES & POLICIES
# =============================================================================

# IAM Role para pods acessarem S3
resource "aws_iam_role" "s3_access" {
  name = "${var.project_name}-s3-access-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Federated = module.eks.oidc_provider_arn
      }
      Action = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "${module.eks.oidc_provider}:sub" = "system:serviceaccount:sptrans-pipeline:sptrans-service-account"
        }
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "s3_access" {
  role       = aws_iam_role.s3_access.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

# =============================================================================
# OUTPUTS
# =============================================================================

output "eks_cluster_endpoint" {
  description = "EKS cluster endpoint"
  value       = module.eks.cluster_endpoint
}

output "eks_cluster_name" {
  description = "EKS cluster name"
  value       = module.eks.cluster_name
}

output "rds_endpoint" {
  description = "RDS PostgreSQL endpoint"
  value       = module.rds_postgresql.db_instance_endpoint
  sensitive   = true
}

output "redis_endpoint" {
  description = "ElastiCache Redis endpoint"
  value       = module.elasticache_redis.configuration_endpoint
  sensitive   = true
}

output "s3_bucket_name" {
  description = "S3 Data Lake bucket name"
  value       = module.s3_datalake.s3_bucket_id
}

# =============================================================================
# END
# =============================================================================
