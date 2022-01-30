# How to Build custom airflow image 

Based on `Dockerfile` custom image can be pulled from docker repo and commands can be added to aggregate software to the image. Tag is added so docker yaml file can instantiate this image. Building first image is best so docker compose can easily run. 

```console
docker build . -f Dockerfile --tag my-image:0.0.1
```

```console
docker-compose up airflow-init
```

to open ssh
```
docker exec -it 7b93bbaa8417 /bin/bash
```


