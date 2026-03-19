FROM apache/spark:3.5.3

USER root

# Install Python packages
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    elasticsearch==8.11.0

WORKDIR /app
COPY . .

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

CMD ["/bin/bash", "-c", "spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0 --driver-memory 1g --executor-memory 1g --conf spark.driver.host=$(hostname -i) /app/spark_processor.py"]
