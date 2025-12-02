## High-level architecture of the project
![high-level-architecture.png](docs/high-level-architecture.png)
## Clusters
### Spark
![spark.png](docs/spark.png)
### Airflow
![airflow.png](docs/airflow.png)
### Trino
![trino.png](docs/trino.png)
## Guide
### Start project
```
docker-compose up --build
```
### Select and run specific services
```
docker-compose -f common.yml -f redis.yml -f apache_spark.yml up --build -d
```