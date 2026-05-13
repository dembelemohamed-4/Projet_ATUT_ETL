FROM apache/airflow:3.2.1
USER root
RUN apt-get update \
    && apt-get install -y jq \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow
WORKDIR /code

# Copying Pipfile.lock file
COPY ./Pipfile.lock /code/Pipfile.lock

# Generate requirements.tex from Pipefile.lock
RUN jq -r '.default | to_entries[] | .key + .value.version' \
    Pipfile.lock > requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt
COPY --chown=airflow:root dags/* opt/airflow/dags

USER airflow