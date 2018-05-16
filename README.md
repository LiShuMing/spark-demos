
## 一、项目说明
> 本项目为平时使用Spark/HBase/Kafka等大数据组件的Demo示例，后续会逐渐丰富;

* 支持Kerberos/非Kerberos环境下操作HBase/Kafka/HDFS/Spark;
* 支持Spark Streaming实时读取Kafka；
* 支持Kafka/HBase相关可配置；
* 支持Yarn/local环境下操作;
* 支持Java/Scala两种语言编写Spark相关代码；
* 只支持Spark 2.1.1;

TODO:
* 补充Spark SQL/StructStreaming相关代码;
* 补充Kafka/HBase相关操作;
* 支持Spark多版本；

### 1.1 项目构建说明

> src/main/java: java语言编写程序示例;
> src/main/scala: scala语言编写程序示例;
> src/main/resources: 项目使用resources示例;

### 1.2 具体主类说明

#### 1.2.1 Java 语言
*基本Spark程序*
* com.netease.spark.DummyCountJava:  简单的word count程序; 

*Spark Streaming*相关

* com.netease.spark.streaming.hbase.JavaKafkaToHBaseKerberos: 在Kerberos环境下从Kafka读取并写入HBase;
* com.netease.spark.streaming.hdfs.JavaHdfsToHdfsKerberos: 在Kerberos环境下从Kafka读取并写入HDFS;
* com.netease.spark.streaming..JavaDirectKafkaWordCountKerberos: 在Kerberos环境下从Kafka读取数据并且进行WordCount计算;

#### 1.2.1 Scala 语言
*基本Spark程序*
* com.netease.spark.DummyCountScala:  简单的word count程序; 

*Spark Streaming*相关

* com.netease.spark.streaming.hbase.HBaseTest: 非Kerberos环境下自动生成数据写入HBase;
* com.netease.spark.streaming.hbase.KafkaToHBase: 在Kerberos环境下从Kafka读取并写入HBase;
* com.netease.spark.streaming.hdfs.KafkaToHdfs: 在非Kerberos环境下从Kafka读取并写入HDFS;
* com.netease.spark.streaming.hdfs.HdfsWordCount: 基于Spark Streaming统计HDFS路径下WordCount;
* com.netease.spark.streaming.DirectKafkaWordCount: 在非Kerberos环境下从Kafka读取数据并且进行WordCount计算;
* com.netease.spark.streaming.DirectKafkaWordCountKerberos: 在Kerberos环境下从Kafka读取数据并且进行WordCount计算;

## 二、项目运行
### 2.1 运行环境说明

构建环境分为Kerberos和非Kerberos环境， 但如果想完整地运行本项目，需要依赖如下组件(考虑是否开启Kerberos)：
* Spark环境;
* HDFS环境;
* Yarn环境;
* Kafka环境;
* HBase环境;

> 在运行程序前，请配置好src/main/resources/conf.properties文件，保障访问集群正常;

#### Kafka集群配置
* 生成本机的kafka_client_jaas.conf文件和kafka.service.keytab文件；（keytab文件理论上可以不用kafka的，但需要在broker测添加acl权限）

确定现有kafka topics已有权限：
> bin/kafka-acls.sh --authorizer-properties zookeeper.connect=hzadg-mammut-platform2.server.163.org:2181,hzadg-mammut-platform3.server.163.org:2181 --list --topic spark-test
  
添加任意账号在`test-consumer-group`组有consumer权限：
>  bin/kafka-acls.sh --authorizer-properties zookeeper.connect=hzadg-mammut-platform2.server.163.org:2181,hzadg-mammut-platform3.server.163.org:2181 --add --allow-principal User:*  --consumer --topic spark-test  --group test-consumer-group

添加任意账号在`spark-test` topics有producer权限：
> bin/kafka-acls.sh --authorizer-properties zookeeper.connect=hzadg-mammut-platform2.server.163.org:2181,hzadg-mammut-platform3.server.163.org:2181 --add --allow-principal User:*  --producer --topic spark-test 


#### HBase 环境初始化
```$xslt
cd /usr/ndp/current/hbase_client

kinit -kt /etc/security/keytabs/hbase.service.keytab hbase/hzadg-mammut-platform1.server.163.org@BDMS.163.COM

./bin/hbase shell

create 'hbase-test', 'f1'
put 'hbase-test', 'row1', 'f1:a', 'v1'
put 'hbase-test', 'row2', 'f1:a', 'v2'
```

### 2.2 Java版本实现

#### JavaDirectKafkaWordCount

启动脚本:
```
/usr/ndp/current/spark2_client/bin/spark-submit 
--conf spark.yarn.keytab=/etc/security/keytabs/kafka.service.keytab \
--conf spark.yarn.principal=kafka/hzadg-mammut-platform1.server.163.org@BDMS.163.COM \
--files /usr/ndp/current/kafka_client/conf/kafka_client_jaas.conf,/etc/security/keytabs/kafka.service.keytab \
--driver-java-options "-Djava.security.auth.login.config=/usr/ndp/current/kafka_client/conf/kafka_client_jaas.conf" \
--conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=/usr/ndp/current/kafka_client/conf/kafka_client_jaas.conf" \
--class com.netease.spark.streaming.JavaDirectKafkaWordCountKerberos \
--master yarn \
--deploy-mode client \
./target/spark-demo-0.1.0-jar-with-dependencies.jar hzadg-mammut-platform8.server.163.org:6667 spark-test
```

### JavaKafaToHdfs

启动脚本:
```$xslt
/usr/ndp/current/spark2_client/bin/spark-submit \
--conf spark.yarn.keytab=/etc/security/keytabs/kafka.service.keytab \
--conf spark.yarn.principal=kafka/hzadg-mammut-platform1.server.163.org@BDMS.163.COM    --files /usr/ndp/current/kafka_client/conf/kafka_client_jaas.conf   --driver-java-options "-Djava.security.auth.login.config=/usr/ndp/current/kafka_client/conf/kafka_client_jaas.conf"   --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=/usr/ndp/current/kafka_client/conf/kafka_client_jaas.conf"   --class com.netease.spark.JavaKafkaToHdfs   --master yarn   --deploy-mode cluster   ./target/spark-demo-0.1.0-jar-with-dependencies.jar hzadg-mammut-platform8.server.163.org:6667 spark-test
```

## Scala版本
### DirectKafkaWordCountKerberos
> 统计Kfaka流中的word count.


* 编译项目代码：
> mvn clean pacakge


* 通过命令行提交spark代码：

```$xslt


```
/usr/ndp/current/spark2_client/bin/spark-submit \  
--files /usr/ndp/current/kafka_client/conf/kafka_client_jaas.conf,/etc/security/keytabs/kafka.service.keytab \  
--conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=/usr/ndp/current/kafka_client/conf/kafka_client_jaas.conf" \  
--driver-java-options "-Djava.security.auth.login.config=/usr/ndp/current/kafka_client/conf/kafka_client_jaas.conf" \   
--class com.netease.spark.DirectKafkaWordCountKerberos \  
--master local[2]  \  
./target/spark-demo-0.1.0-jar-with-dependencies.jar  

### Kafka2Hdfs
> 将Kafka数据流写入Hdfs。

注意事项：
* keytab文件复制到提交任务的机器；
* keytab账号(kafka)拥有Hdfs写路径(/test)的写权限；

```$xslt
/usr/ndp/current/spark2_client/bin/spark-submit \
  --conf spark.yarn.keytab=/etc/security/keytabs/kafka.service.keytab  \
  --conf spark.yarn.principal=kafka/hzadg-mammut-platform1.server.163.org@BDMS.163.COM  \
  --files /usr/ndp/current/kafka_client/conf/kafka_client_jaas.conf,/etc/security/keytabs/kafka.service.keytab  \
  --driver-java-options "-Djava.security.auth.login.config=/usr/ndp/current/kafka_client/conf/kafka_client_jaas.conf" \
  --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=/usr/ndp/current/kafka_client/conf/kafka_client_jaas.conf" \
  --class com.netease.spark.streaming.KafkaToHdfs \
  --master local[2]  \
  ./target/spark-demo-0.1.0-jar-with-dependencies.jar  

```

### HBaseTest


在kerberos环境下有两种方式运行：

1. 基于spark提供keytab/principal模式访问HBase：
```shell
/usr/ndp/current/spark2_client/bin/spark-submit \
--conf spark.yarn.keytab=/etc/security/keytabs/hbase.service.keytab \
--conf spark.yarn.principal=hbase/hzadg-mammut-platform1.server.163.org@BDMS.163.COM \
--class com.netease.spark.streaming.hbase.HBaseTest \
--master local[2]  \
./target/spark-demo-0.1.0-jar-with-dependencies.jar  
```

2. 自己手动kinit 然后再运行程序（不推荐:https://marsishandsome.github.io/slides/gen/HadoopSecurity.html#slide25）:

```shell

kinit -kt /etc/security/keytabs/hbase.service.keytab hbase/hzadg-mammut-platform1.server.163.org@BDMS.163.COM

/usr/ndp/current/spark2_client/bin/spark-submit \
--class com.netease.spark.streaming.hbase.HBaseTest \
--master local[2]  \
./target/spark-demo-0.1.0-jar-with-dependencies.jar  
```
### KafkaToHbase

将Kafka数据写入HBase，需要注意的是：
* 使用的keytab拥有Kafka/Hbase的权限，示例是基于hbase用户测试的；

```$xslt
/usr/ndp/current/spark2_client/bin/spark-submit \
--conf spark.yarn.keytab=/etc/security/keytabs/hbase.service.keytab \
--conf spark.yarn.principal=hbase/hzadg-mammut-platform1.server.163.org@BDMS.163.COM \
--class com.netease.spark.streaming.hbase.KafkaToHbase \
--master local[2]  \
./target/spark-demo-0.1.0-jar-with-dependencies.jar  
```
### JavaKafkaToHBaseKerberos
```$xslt
/usr/ndp/current/spark2_client/bin/spark-submit \
--files ./kafka_client_jaas.conf,./kafka.service.keytab,./hbase-site.xml \
--conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./kafka_client_jaas.conf" \
--driver-java-options "-Djava.security.auth.login.config=./kafka_client_jaas.conf" \
--conf spark.yarn.keytab=/etc/security/keytabs/hbase.service.keytab \
--conf spark.yarn.principal=hbase/hzadg-mammut-platform1.server.163.org@BDMS.163.COM \
--class com.netease.spark.streaming.hbase.JavaKafkaToHBaseKerberos \
--master yarn  \
--deploy-mode client ./target/spark-demo-0.1.0-jar-with-dependencies.jar
```
