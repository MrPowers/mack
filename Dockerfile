FROM ubuntu:20.04

ENV TZ=America/Chicago
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update && \
    apt-get -y install --no-install-recommends default-jdk software-properties-common python3-pip python3.9 python3.9-dev libpq-dev build-essential wget libssl-dev libffi-dev vim && \
    apt-get clean

RUN wget https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz && \
    tar xvf spark-3.3.2-bin-hadoop3.tgz && \
    mv spark-3.3.2-bin-hadoop3/ /usr/local/spark && \
    ln -s /usr/local/spark spark

WORKDIR app
COPY . /app

RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.9 2
RUN  update-alternatives --config python3

RUN pip3 install poetry delta-spark
RUN poetry install

ENV PYSPARK_PYTHON=python3
ENV PYSPARK_SUBMIT_ARGS='--packages io.delta:delta-core_2.12:2.2.0 pyspark-shell'
