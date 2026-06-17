FROM python:3.11-slim
RUN apt-get update && apt-get install -y openjdk-11-jdk wget libpq-dev && rm -rf /var/lib/apt/lists/*
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
RUN wget -qO- https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | tar -xz -C /opt && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark
ENV PATH=$PATH:/opt/spark/bin
ENV PYSPARK_PYTHON=python3
WORKDIR /app
COPY . /app
COPY jars/snowflake-jdbc-3.13.17.jar jars/spark-snowflake_2.13-3.1.9.jar /opt/spark/jars/
ENV SPARK_CLASSPATH=/opt/spark/jars/snowflake-jdbc-3.13.17.jar:/opt/spark/jars/spark-snowflake_2.13-3.1.9.jar
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "etl_pipeline.py"]

