#!/bin/bash

# spark-2.0.2
SPARK_VERSION=202
SPARK_REAL_VERSION=2.0.2

# spark-1.6.2
#SPARK_VERSION=162
#SPARK_REAL_VERSION=1.6.2

# spark-2.1.1
#SPARK_VERSION=211
#SPARK_REAL_VERSION=2.1.1

SPARK_DEMO=spark-demo-${SPARK_VERSION}

rm -rf ${SPARK_DEMO}
mkdir -p ${SPARK_DEMO}

cp ./target/spark-demo-0.1.0.jar ./${SPARK_DEMO}/spark-demo-${SPARK_VERSION}.jar
cp ./security/mammut.keytab ./${SPARK_DEMO}
#cp ./script/spark-test.sh ./${SPARK_DEMO}
cp ./script/spark-submit.job ./${SPARK_DEMO}

#cp ./target/spark-demo-0.1.0-jar-with-dependencies.jar ./${SPARK_DEMO}/spark-demo-${SPARK_VERSION}.jar
#cp ./conf/project.properties ./${SPARK_DEMO}

#cp ./conf/spark-demo.flow ./${SPARK_DEMO}
#sed s/spark-demo/spark-demo-${SPARK_VERSION}/g ${SPARK_DEMO}/spark-demo.flow > spark-demo.flow.tmp
#mv spark-demo.flow.tmp ./${SPARK_DEMO}/spark-demo.flow

#cp ./conf/spark-demo.job ./${SPARK_DEMO}
#sed s/spark-demo/spark-demo-${SPARK_VERSION}/g ${SPARK_DEMO}/spark-demo.job|sed s/spark-version=2.0.2/spark-version=${SPARK_REAL_VERSION}/g > spark-demo.job.tmp
#mv spark-demo.job.tmp ./${SPARK_DEMO}/spark-demo.job


zip -r ${SPARK_DEMO}.zip ./${SPARK_DEMO}
