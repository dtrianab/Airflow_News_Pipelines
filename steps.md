# How to Build custom airflow image 

- Initialize the database
On all operating systems, you need to run database migrations and create the first user account. To do it, run.
docker-compose up airflow-init

docker build . -f Dockerfile --tag my-image:0.0.1

mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env

docker-compose up airflow-init

to open ssh
docker exec -it 7b93bbaa8417 /bin/bash