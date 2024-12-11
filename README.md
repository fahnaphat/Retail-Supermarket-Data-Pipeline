# Retail Supermarket: ETL with Airflow and DBT-Core
This project implements an end-to-end ETL (Extract, Transform, Load) pipeline to analyze retail supermarket data. The goal is to process and transform data into insightful models, enabling analysis of sales, profitability, and other key metrics at various retail locations.

---
![Retail-analysis](https://github.com/user-attachments/assets/dadbbe75-46a1-4ae9-b26e-2b4f7a37a0f1)

The pipeline begins by treating Neon, a cloud-hosted PostgreSQL database, as a data lake for storing raw data. The process then extracts this data from Neon and, using Airflow DAGs (Directed Acyclic Graphs), automates its transfer and schema management within a PostgreSQL instance on pgAdmin4. dbt models are then applied to transform and aggregate the data, preparing it for comprehensive business analysis.

## Tools and Technologies

 - Apache Airflow
 - dbt (Data Build Tool)
 - Docker and Docker Compose
 - PostgreSQL with Neon
 - pgAdmin4

## Resources
Data Sources:
 - Kaggle data from [Kaggle.com](https://www.kaggle.com/datasets/roopacalistus/superstore)

Enviroment:

 - Python 3.12

## My Learning Experience

 - Setting up an ETL pipeline.
 - Automating database schema creation using Airflow DAGs.
 - Writing efficient SQL models with dbt.
 - Managing databases on Neon and pgAdmin4.

## Using this repo

 1. Clone the repository:
```sh 
  git clone https://github.com/fahnaphat/Retail-Supermarket-Data-Pipeline.git
  cd Retail-Supermarket-Data-Pipeline 
  ```
 2. Build and run the Docker containers:
```sh 
  docker-compose build
  ```
```sh 
  docker-compose up
  ```
 3. Access the services:
	- **Airflow**: `http://localhost:8081`
	 - **pgAdmin**: `http://localhost:5050`
4. Trigger the Airflow DAGs to initialize the database and run dbt models.


