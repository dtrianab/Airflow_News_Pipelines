# news_data_collection

This project intents to create an environment capable of scraping and retriving news data from different sources and the analysis of such information to provide insghts on a broad coverage of media, this to faciliate the comprenhension of news on a specific topic. The first part of the project starts with the configuration of servers needed on ETL process, to this goal a docker image is created with airflow components. 

## Airflow Configuration
Airflow is deployed following example docker-compose.yaml provided at airflow web page. 

### Dockerfile

Docker file configures the image to use: apache/airflow:2.2.3-python3.8 and install py libraries.  

### docker-compose.yaml

### Database
- pg Admin
Name:postgres
Port: 5432
Username: airflow
Password: 

## DAGs

### News API

### Googler

### Other Srvice
