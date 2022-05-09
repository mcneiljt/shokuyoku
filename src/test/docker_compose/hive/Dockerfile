FROM openjdk:8

WORKDIR /opt

ENV HADOOP_VERSION=3.3.2
# ENV METASTORE_VERSION=3.0.0
ENV HIVE_VERSION=3.1.2

ENV HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
ENV HIVE_HOME=/opt/apache-hive-${HIVE_VERSION}-bin

# RUN curl -L https://downloads.apache.org/hive/hive-standalone-metastore-${METASTORE_VERSION}/hive-standalone-metastore-${METASTORE_VERSION}-bin.tar.gz | tar zxf -
RUN curl -L https://downloads.apache.org/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz| tar zxf -

RUN curl -L https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz | tar zxf -
RUN curl -L https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-8.0.29.tar.gz | tar zxf -
RUN cp mysql-connector-java-8.0.29/mysql-connector-java-8.0.29.jar ${HIVE_HOME}/lib/
RUN rm -rf  mysql-connector-java-8.0.29
RUN curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION=}/hadoop-aws-${HADOOP_VERSION=}.jar --output hadoop-aws-${HADOOP_VERSION=}.jar
RUN cp hadoop-aws-${HADOOP_VERSION=}.jar ${HIVE_HOME}/lib/
RUN rm -rf hadoop-aws-${HADOOP_VERSION=}.jar

RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.215/aws-java-sdk-1.12.215.jar --output aws-java-sdk-1.12.215.jar
RUN cp aws-java-sdk-1.12.215.jar ${HIVE_HOME}/lib/
RUN rm -rf aws-java-sdk-1.12.215.jar

RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-core/1.12.215/aws-java-sdk-core-1.12.215.jar --output aws-java-sdk-core-1.12.215.jar
RUN cp aws-java-sdk-core-1.12.215.jar ${HIVE_HOME}/lib/
RUN rm -rf aws-java-sdk-core-1.12.215.jar


RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-s3/1.12.215/aws-java-sdk-s3-1.12.215.jar --output aws-java-sdk-s3-1.12.215.jar
RUN cp aws-java-sdk-s3-1.12.215.jar ${HIVE_HOME}/lib/
RUN rm -rf aws-java-sdk-s3-1.12.215.jar

RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-dynamodb/1.12.215/aws-java-sdk-dynamodb-1.12.215.jar --output aws-java-sdk-dynamodb-1.12.215.jar
RUN cp aws-java-sdk-dynamodb-1.12.215.jar ${HIVE_HOME}/lib/
RUN rm -rf aws-java-sdk-dynamodb-1.12.215.jar

RUN curl https://repo1.maven.org/maven2/org/apache/tez/tez-dag/0.10.1/tez-dag-0.10.1.jar --output tez-dag-0.10.1.jar
RUN cp tez-dag-0.10.1.jar ${HIVE_HOME}/lib/
RUN rm -rf tez-dag-0.10.1.jar

RUN curl https://repo1.maven.org/maven2/org/apache/tez/tez-api/0.10.1/tez-api-0.10.1.jar --output tez-api-0.10.1.jar
RUN cp tez-api-0.10.1.jar ${HIVE_HOME}/lib/
RUN rm -rf tez-api-0.10.1.jar


COPY conf/metastore-site.xml ${HIVE_HOME}/conf/hive-site.xml
COPY scripts/entrypoint.sh /entrypoint.sh

RUN mkdir -p /tmp/hive && chmod 777 /tmp/hive

RUN groupadd -r hive --gid=1000 && \
    useradd -r -g hive --uid=1000 -d ${HIVE_HOME} hive && \
    chown hive:hive -R ${HIVE_HOME} && \
    chown hive:hive /entrypoint.sh && chmod +x /entrypoint.sh

#RUN mkdir /opt/apache-hive-${HIVE_VERSION}-bin/bin/metastore_db && chmod 777 /opt/apache-hive-${HIVE_VERSION}-bin/bin/metastore_db

USER hive
EXPOSE 9083

ENTRYPOINT ["sh", "-c", "/entrypoint.sh"]
