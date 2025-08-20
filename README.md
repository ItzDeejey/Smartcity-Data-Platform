# SmartCity Data Platform 🚦

End-to-end data engineering project for ingesting, processing, and analyzing UK traffic & pollution datasets.  
Built with **Apache Airflow, Kafka, Docker, PostgreSQL, Google Cloud Storage, and Python**.

## Features
- 🚗 Real-time streaming of accident, casualty, and vehicle datasets into Kafka.
- 📦 Batch ingestion pipelines from Kaggle → GCS using Airflow DAGs.
- 🗄️ Automated parquet conversion and GCS uploads.
- ⚙️ Nightly production workflows with zero downtime.
- 📊 Jupyter notebooks for exploration and visualization.

## Tech Stack
- **Orchestration**: Apache Airflow  
- **Streaming**: Apache Kafka + Zookeeper  
- **Storage**: PostgreSQL, Google Cloud Storage (GCS)  
- **Processing**: Python (pandas, openpyxl), Bash  
- **Deployment**: Docker & docker-compose  
- **Monitoring**: Kafka UI, Airflow UI  

## How to Run
```bash
git clone https://github.com/ItzDeejey/smartcity-data-platform.git
cd smartcity-data-platform
docker-compose up -d
