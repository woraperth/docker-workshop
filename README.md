# Docker Workshop Demo

Docker-compose demo for R2DE.

## How to run this project
1. Clone this project
2. Run `docker-compose up -d`
3. Run `docker ps` to see the containers are running
4. Open http://localhost. You should see the nginx welcome page.

## Idea to play around
You can test modifying docker-compose.yml and run `docker-compose up -d` again to view the update.

# Airflow Demo

Follow this instruction: [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html) including create `dags`, `logs`, `plugins`, and `data` folders.

The resources is in `airflow/` directory, including addtional requirements to install.

To build the new container image with specified requirements.
```
docker-compose build
```

First initialization of Airflow:
```
docker-compose up airflow-init
```

Then, after initialization, start all containers:
```
docker-compose up -d
```

In the example dag in `dags/`, don't forget to apply database credentials. And please do not commit passwords or credential to git.

## Keeping credentials in Environment Variables

In directory `airflow-env-vars/` contains the demo that requires to keep credentials in `.env` files. Then those variables will be set in container by `docker-compose.yml` in `environment:` section.

The example of `.env` file:
```
AIRFLOW_UID=
AIRFLOW_GID=
MYSQL_HOST=
MYSQL_PORT=
MYSQL_USER=
MYSQL_PASSWORD=
MYSQL_DB=
MYSQL_CHARSET=
```