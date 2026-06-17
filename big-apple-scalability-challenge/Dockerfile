# Use official Python image with Spark dependencies
FROM python:3.11-slim

# Install Java (required by Spark) and other dependencies 
RUN apt-get update && apt-get install -y openjdk-11-jdk wget libpq-dev && rm -rf /var/lib/apt/lists/*

# Install Spark (match pyspark>=4.0.0 local version roughly, or latest 3.5.x if 4.0 not widely mirrored, but we'll use 4.0.0-preview2 or similar. To be safe we keep 3.5.0 here as standard but note config.py handles Windows vs Linux)
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
RUN wget -qO- https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | tar -xz -C /opt && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

ENV PATH=$PATH:/opt/spark/bin
ENV PYSPARK_PYTHON=python3

# Set workdir and copy app code
# (Note: config.py will automatically detect Linux and skip Windows winutils hacks)
WORKDIR /app
COPY . /app

# Copy Snowflake JDBC and Spark connector drivers into Spark classpath
COPY jars/snowflake-jdbc-3.13.17.jar jars/spark-snowflake_2.13-3.1.9.jar /opt/spark/jars/
ENV SPARK_CLASSPATH=/opt/spark/jars/snowflake-jdbc-3.13.17.jar:/opt/spark/jars/spark-snowflake_2.13-3.1.9.jar

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Default command runs the ETL pipeline
CMD ["python", "etl_pipeline.py"]
