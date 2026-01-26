FROM jupyter/pyspark-notebook:x86_64-ubuntu-22.04

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip install --no-cache-dir --upgrade pip

WORKDIR /opt/spark

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
