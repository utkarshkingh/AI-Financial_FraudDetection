# Fraud Detection System

A comprehensive real-time fraud detection system built with Apache Airflow, MLflow, and streaming data processing capabilities. This system processes financial transactions in real-time to detect fraudulent activities using machine learning models.

![System Architecture](./system-architecture.jpg)

## System Architecture

The system follows a modern data architecture pattern combining batch and real-time processing:

- **Data Ingestion**: Financial transactions are streamed through Confluent Cloud Kafka
- **Stream Processing**: Apache Spark processes streaming data in real-time  
- **Orchestration**: Apache Airflow manages ML workflows and data pipelines
- **ML Operations**: MLflow handles model versioning, tracking, and deployment
- **Storage**: PostgreSQL for structured data, MinIO for object storage
- **Caching**: Redis for high-performance data caching

## Features

- **Real-time Fraud Detection**: Process transactions as they occur
- **ML Model Management**: Automated model training, versioning, and deployment
- **Scalable Architecture**: Containerized microservices with Docker Compose
- **Data Pipeline Orchestration**: Automated workflows with Apache Airflow
- **Model Monitoring**: Track model performance and data drift
- **Distributed Processing**: Leverage Apache Spark for large-scale data processing

## Technology Stack

- **Orchestration**: Apache Airflow with CeleryExecutor
- **ML Operations**: MLflow with S3-compatible storage
- **Stream Processing**: Apache Spark, Kafka (Confluent Cloud)
- **Databases**: PostgreSQL (for Airflow and MLflow)
- **Caching**: Redis (Celery message broker)
- **Object Storage**: MinIO (S3-compatible)
- **Containerization**: Docker & Docker Compose
- **Cloud Services**: AWS integration ready

