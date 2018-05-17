
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

#### 2.1.1 Kafka集群配置
> 生成本机的kafka_client_jaas.conf文件和kafka.service.keytab文件；（keytab文件理论上可以不用kafka的，但需要在broker测添加acl权限）


a. 因为kafka默认使用的协议为PLAINTEXT，在kerberos环境下需要变更其通信协议： 在${KAFKA_HOME}/`config/producer.properties`和`config/consumer.properties`下添加
```
security.protocol=SASL_PLAINTEXT
```

b. 在执行前，需要在环境变量中添加KAFKA_OPT选项，否则kafka无法使用keytab：
```
export KAFKA_OPTS="$KAFKA_OPTS -Djava.security.auth.login.config=/usr/ndp/current/kafka_broker/conf/kafka_jaas.conf"
```

其中`kafka_jaas.conf`内容如下：
```
KafkaServer {
com.sun.security.auth.module.Krb5LoginModule required
useKeyTab=true
keyTab="/etc/security/keytabs/kafka.service.keytab"
storeKey=true
useTicketCache=false
serviceName="kafka"
principal="kafka/hzadg-mammut-platform3.server.163.org@BDMS.163.COM";
};
KafkaClient {
com.sun.security.auth.module.Krb5LoginModule required
useTicketCache=true
renewTicket=true
serviceName="kafka";
};
Client {
com.sun.security.auth.module.Krb5LoginModule required
useKeyTab=true
keyTab="/etc/security/keytabs/kafka.service.keytab"
storeKey=true
useTicketCache=false
serviceName="zookeeper"
principal="kafka/hzadg-mammut-platform3.server.163.org@BDMS.163.COM";
};
```

c. 创建新的topic：
```
bin/kafka-topics.sh --create --zookeeper hzadg-mammut-platform2.server.163.org:2181,hzadg-mammut-platform3.server.163.org:2181 --replication-factor 1 --partitions 1 --topic spark-test
```

d. 创建生产者：
```
bin/kafka-console-producer.sh  --broker-list hzadg-mammut-platform2.server.163.org:6667,hzadg-mammut-platform3.server.163.org:6667,hzadg-mammut-platform4.server.163.org:6667 --topic spark-test --producer.config ./config/producer.properties
```

e. 测试消费者：
```
bin/kafka-console-consumer.sh --zookeeper hzadg-mammut-platform2.server.163.org:2181,hzadg-mammut-platform3.server.163.org:2181 --bootstrap-server hzadg-mammut-platform2.server.163.org:6667 --topic spark-test --from-beginning --new-consumer  --consumer.config ./config/consumer.properties
```

f. 确定现有kafka topics已有权限：
```
bin/kafka-acls.sh --authorizer-properties zookeeper.connect=hzadg-mammut-platform2.server.163.org:2181,hzadg-mammut-platform3.server.163.org:2181 --list --topic spark-test
```
  
g. 添加任意账号在`test-consumer-group`组有consumer权限：
```
bin/kafka-acls.sh --authorizer-properties zookeeper.connect=hzadg-mammut-platform2.server.163.org:2181,hzadg-mammut-platform3.server.163.org:2181 --add --allow-principal User:*  --consumer --topic spark-test  --group test-consumer-group
```

h. 添加任意账号在`spark-test` topics有producer权限：
```
bin/kafka-acls.sh --authorizer-properties zookeeper.connect=hzadg-mammut-platform2.server.163.org:2181,hzadg-mammut-platform3.server.163.org:2181 --add --allow-principal User:*  --producer --topic spark-test 
```

#### 2.1.2 HBase 环境初始化
```$xslt
cd /usr/ndp/current/hbase_client

kinit -kt /etc/security/keytabs/hbase.service.keytab hbase/hzadg-mammut-platform1.server.163.org@BDMS.163.COM

./bin/hbase shell

create 'hbase-test', 'f1'
put 'hbase-test', 'row1', 'f1:a', 'v1'
put 'hbase-test', 'row2', 'f1:a', 'v2'
```

### 2.2 代码运行

> 这个问题是许多Spark用户都比较纠结的问题，原因在于Spark繁杂的配置项，如果对其理解不透，则在使用的时候，只能一遍遍地试用了；
> 现在以JavaKafkaToHBaseKerberos为例，讲解如何使用Spark Streaming从Kafka读取数据并写入HBase。

现对spark-sumbit中几个比较重要的配置，做一个说明：
* --files : 必须用','相隔，文件会上传至executor的工作路径，默认并没有加载至classpath中，一般使用在配置文件相关；
* --jars  : 必须用','相隔，文件会上传至executor/driver(cluster模式下)的工作路径，默认会加载至classpath中，一般使用在所依赖的jar相关；

* --class : 加载主类名；
* --master yarn ： yarn集群的方式提交
* --queue : 提交yarn队列的名称；
* --driver-memory : driver申请内存；
* --executor-memory : executor申请内存；
* --executor-cores : 每个executor中使用的cores数量，建议2~5个；


* --conf  ：spark-submit启动spark任务时配置项内容，其中又包含如下几个比较重要的(示例)：
  * --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./kafka_client_jaas.conf" : executor启动是的jvm配置项，一般kerberos系统配置会使用到；
  * --conf spark.yarn.keytab=/etc/security/keytabs/hbase.service.keytab : spark-submit 依赖的keytab配置；
  * --conf spark.yarn.principal=hbase/hzadg-mammut-platform1.server.163.org@BDMS.163.COM : spark-submit 启动依赖的principle配置；
  * --conf spark.driver.extraClassPath=./spark-demos-0.1.0/lib/* : driver启动时添加jvm的classpath，加载必要的jar；
* --driver-java-options ： driver 启动是的jvm配置项，一般kerberos系统配置会使用到；


所以基于上述的配置项，如果运行KafkaToHBase项目，首先
* 将项目依赖的配置文件加载通过--files保障executor配置项是同步的；
* 将kerberos认证相关内容、相关配置复制到项目路径下(./kafka_client_jaas.conf,./kafka.service.keytab,./hbase-site.xml )；
* 将项目依赖的(Spark/Yarn环境没有提供的jar)通过--jars上传至executor工作路径中；


其中注意，由于--files/--jars针对多个文件都是用','分割的，所以可以使用下面这个命令生成凭借字符串(注意变更必要参数)：
>  r='';for i in ` ls  ./lib/`;do  r=${r},"./lib/$i";done ; echo $r

针对`https://github.com/LiShuMing/spark-demos`项目，启动如下：
* 编译完毕后，将`target/spark-demos-0.1.0-distribution.tar.gz`编译文件mv到工作环境，解压；
* 将依赖的`kafka_client_jaas.conf  kafka.service.keytab`复制到项目路径下；
* 基于` r='';for i in ` ls  ./lib/`;do  r=${r},"./lib/$i";done ; echo $r`生成--jars必要拼接串；


最终运行命令如下（具体使用需要调整）：
```
/usr/ndp/current/spark2_client/bin/spark-submit \
--files ./kafka_client_jaas.conf,./kafka.service.keytab,./hbase-site.xml \
--conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./kafka_client_jaas.conf" \
--driver-java-options "-Djava.security.auth.login.config=./kafka_client_jaas.conf" \
--conf spark.yarn.keytab=/etc/security/keytabs/hbase.service.keytab \
--conf spark.yarn.principal=hbase/hzadg-mammut-platform1.server.163.org@BDMS.163.COM \
--conf spark.driver.extraClassPath=./spark-demos-0.1.0/lib/* \
--jars ./lib/commons-cli-1.2.jar,./lib/commons-codec-1.9.jar,./lib/commons-collections-3.2.2.jar,./lib/commons-httpclient-3.1.jar,./lib/commons-io-2.4.jar,./lib/commons-lang-2.6.jar,./lib/commons-lang3-3.3.2.jar,./lib/commons-logging-1.2.jar,./lib/commons-math-2.2.jar,./lib/disruptor-3.3.0.jar,./lib/findbugs-annotations-1.3.9-1.jar,./lib/guava-12.0.1.jar,./lib/hamcrest-core-1.3.jar,./lib/hbase-annotations-1.2.6.jar,./lib/hbase-client-1.2.6.jar,./lib/hbase-common-1.2.6.jar,./lib/hbase-common-1.2.6-tests.jar,./lib/hbase-hadoop2-compat-1.2.6.jar,./lib/hbase-hadoop-compat-1.2.6.jar,./lib/hbase-prefix-tree-1.2.6.jar,./lib/hbase-procedure-1.2.6.jar,./lib/hbase-protocol-1.2.6.jar,./lib/hbase-server-1.2.6.jar,./lib/htrace-core-3.1.0-incubating.jar,./lib/jackson-core-asl-1.9.13.jar,./lib/jackson-jaxrs-1.9.13.jar,./lib/jackson-mapper-asl-1.9.13.jar,./lib/jcodings-1.0.8.jar,./lib/jdk.tools-1.8.jar,./lib/jetty-util-6.1.26.jar,./lib/jline-0.9.94.jar,./lib/joni-2.1.2.jar,./lib/junit-4.12.jar,./lib/kafka-clients-0.10.0.1.jar,./lib/log4j-1.2.17.jar,./lib/lz4-1.3.0.jar,./lib/metrics-core-2.2.0.jar,./lib/netty-3.8.0.Final.jar,./lib/netty-all-4.0.29.Final.jar,./lib/protobuf-java-2.5.0.jar,./lib/slf4j-api-1.7.7.jar,./lib/slf4j-log4j12-1.7.7.jar,./lib/snappy-java-1.1.2.6.jar,./lib/spark-demo-0.1.0.jar,./lib/spark-streaming-kafka-0-10_2.11-2.0.2.jar,./lib/unused-1.0.0.jar,./lib/zookeeper-3.4.6.jar \
--master yarn  \
--class com.netease.spark.streaming.hbase.JavaKafkaToHBaseKerberos \
--executor-memory 1g \
--driver-memory 2g \
--executor-cores 1 \
--queue default \
--deploy-mode client ./lib/spark-demo-0.1.0.jar
```

