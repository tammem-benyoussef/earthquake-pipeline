FROM spark:4.2.0-scala2.13-java17-python3-ubuntu

USER root
WORKDIR /app

RUN pip install --no-cache-dir click python-dotenv boto3 psycopg2-binary

RUN /opt/spark/bin/spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.5.0,com.amazonaws:aws-java-sdk-bundle:1.12.367,org.postgresql:postgresql:42.7.3 \
    --class org.apache.spark.examples.SparkPi \
    /opt/spark/examples/jars/spark-examples_2.13-4.2.0.jar 10

COPY spark/process.py .

ENTRYPOINT ["/opt/spark/bin/spark-submit", "--packages", "org.apache.hadoop:hadoop-aws:3.5.0,com.amazonaws:aws-java-sdk-bundle:1.12.367,org.postgresql:postgresql:42.7.3", "process.py"]