FROM openjdk:8-jdk

ENV SPARK_VERSION=3.1.1
ENV HADOOP_VERSION=3.2.0
ENV SCALA_VERSION=2.12

WORKDIR /app

RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar -P /opt/spark/jars/
RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar -P /opt/spark/jars/

COPY target/scala-${SCALA_VERSION}/taxi-data-ingestion_2.12-0.1.0.jar /app/taxi-data-ingestion.jar

ENV SPARK_HOME /opt/spark
ENV PATH $SPARK_HOME/bin:$PATH

ENTRYPOINT ["spark-submit", "--master", "local[*]", "--class", "com.mobito.bdi.TaxiDataIngestion", "--packages", "org.rogach:scallop_2.12:3.0.3,org.apache.hadoop:hadoop-aws:3.2.0", "/app/taxi-data-ingestion.jar"]
