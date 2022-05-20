docker build . -f Dockerfile --tag my-image:0.0.1
docker-compose up airflow-init
docker-compose up -d