# Build custome airflow image 



docker build . -f Dockerfile --tag my-image:0.0.1

mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env

docker-compose up airflow-init

to open ssh
docker exec -it 7b93bbaa8417 /bin/bash