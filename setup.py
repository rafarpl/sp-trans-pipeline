"""
SPTrans Pipeline - Setup Configuration
"""

from setuptools import setup, find_packages
from pathlib import Path

# Ler README para long_description
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding="utf-8")

# Ler requirements
requirements = []
with open("requirements.txt") as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith("#"):
            # Ignorar comentários e linhas vazias
            if not line.startswith("-") and "==" in line:
                requirements.append(line)

setup(
    name="sptrans-pipeline",
    version="2.0.0",
    author="Rafael (rafarpl)",
    author_email="seu-email@exemplo.com",
    description="Real-time data pipeline for SPTrans bus fleet monitoring",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/rafarpl/sp-trans-pipeline",
    project_urls={
        "Bug Reports": "https://github.com/rafarpl/sp-trans-pipeline/issues",
        "Source": "https://github.com/rafarpl/sp-trans-pipeline",
        "Documentation": "https://github.com/rafarpl/sp-trans-pipeline/docs",
    },
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        # Desenvolvimento
        "Development Status :: 4 - Beta",
        
        # Audience
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        
        # Tópicos
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Topic :: Software Development :: Libraries :: Python Modules",
        
        # License
        "License :: OSI Approved :: MIT License",
        
        # Python versions
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        
        # OS
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
    install_requires=[
        # Core
        "python-dotenv>=1.0.0",
        "pydantic>=2.5.0",
        "pydantic-settings>=2.1.0",
        
        # Data Processing
        "pyspark>=3.5.0",
        "delta-spark>=3.0.0",
        "pandas>=2.1.4",
        "numpy>=1.26.2",
        "pyarrow>=14.0.1",
        
        # Databases
        "psycopg2-binary>=2.9.9",
        "sqlalchemy>=2.0.23",
        "redis>=5.0.1",
        
        # API & HTTP
        "requests>=2.31.0",
        "urllib3>=2.1.0",
        "httpx>=0.25.2",
        
        # Messaging
        "confluent-kafka>=2.3.0",
        "kafka-python>=2.0.2",
        
        # Storage
        "boto3>=1.34.10",
        "minio>=7.2.0",
        
        # Monitoring
        "prometheus-client>=0.19.0",
        "python-json-logger>=2.0.7",
        
        # Data Quality
        "great-expectations>=0.18.8",
        "pandera>=0.17.2",
        
        # Geospatial
        "shapely>=2.0.2",
        "geopandas>=0.14.1",
        "geopy>=2.4.1",
        
        # Utilities
        "python-dateutil>=2.8.2",
        "pytz>=2023.3",
        "tenacity>=8.2.3",
        "pybreaker>=1.0.1",
        "jsonschema>=4.20.0",
    ],
    extras_require={
        "dev": [
            # Testing
            "pytest>=7.4.3",
            "pytest-cov>=4.1.0",
            "pytest-asyncio>=0.21.1",
            "pytest-mock>=3.12.0",
            "pytest-xdist>=3.5.0",
            
            # Code Quality
            "black>=23.12.1",
            "isort>=5.13.2",
            "flake8>=6.1.0",
            "pylint>=3.0.3",
            "mypy>=1.7.1",
            "pre-commit>=3.6.0",
        ],
        "docs": [
            # Documentation
            "sphinx>=7.2.6",
            "sphinx-rtd-theme>=2.0.0",
        ],
        "jupyter": [
            # Jupyter
            "jupyter>=1.0.0",
            "jupyterlab>=4.0.9",
            "notebook>=7.0.6",
            "ipykernel>=6.27.1",
            
            # Visualization
            "matplotlib>=3.8.2",
            "seaborn>=0.13.0",
            "plotly>=5.18.0",
        ],
        "airflow": [
            # Airflow (opcional - geralmente instalado separadamente)
            "apache-airflow>=2.8.0",
            "apache-airflow-providers-apache-spark>=4.5.0",
            "apache-airflow-providers-postgres>=5.9.0",
            "apache-airflow-providers-redis>=3.5.0",
        ],
    },
    entry_points={
        "console_scripts": [
            # Scripts CLI
            "sptrans-ingest=src.ingestion.sptrans_api_client:main",
            "sptrans-gtfs=src.ingestion.gtfs_downloader:main",
            "sptrans-setup=scripts.setup:main",
        ],
    },
    include_package_data=True,
    package_data={
        "sptrans_pipeline": [
            "config/*.yaml",
            "config/*.json",
            "sql/**/*.sql",
        ],
    },
    zip_safe=False,
    keywords=[
        "data-engineering",
        "data-pipeline",
        "real-time",
        "spark",
        "airflow",
        "kafka",
        "delta-lake",
        "sptrans",
        "public-transport",
        "gtfs",
    ],
)