FROM bitnami/spark:3.2.1

USER root
RUN apt-get update  -y && apt-get install iputils-ping nano -y && \
    pip3 install --upgrade pip && \
    pip3 install pyspark==3.2.1