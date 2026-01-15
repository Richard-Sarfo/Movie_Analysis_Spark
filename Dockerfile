FROM octoenergy/pyspark:4.0.0

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip install --no-cache-dir --upgrade pip

WORKDIR /opt/spark-app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY spark/ spark/
