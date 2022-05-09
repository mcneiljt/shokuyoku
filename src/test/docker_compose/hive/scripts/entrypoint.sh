#!/bin/sh
#https://github.com/arempter/hive-metastore-docker/blob/master/scripts/entrypoint.sh

export HADOOP_HOME=/opt/hadoop-3.3.2
export HADOOP_CLASSPATH=${HADOOP_HOME}/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.1026.jar:${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-aws-3.3.2.jar
export JAVA_HOME=/usr/local/openjdk-8
cd /opt/apache-hive-3.1.2-bin
/opt/apache-hive-3.1.2-bin/bin/schematool -initSchema -dbType mysql
/opt/apache-hive-3.1.2-bin/bin/hive --service metastore
