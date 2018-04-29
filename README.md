### DirectKafkaWordCountKerberos
> 统计Kfaka流中的word count.


* 编译项目代码：
> mvn clean pacakge

* 生成本机的kafka_client_jaas.conf文件和kafka.service.keytab文件；（keytab文件理论上可以不用kafka的，但需要在broker测添加acl权限）

确定现有kafka topics已有权限：
> bin/kafka-acls.sh --authorizer-properties zookeeper.connect=hzadg-mammut-platform2.server.163.org:2181,hzadg-mammut-platform3.server.163.org:2181 --list --topic spark-test
  
添加任意账号在`test-consumer-group`组有consumer权限：
>  bin/kafka-acls.sh --authorizer-properties zookeeper.connect=hzadg-mammut-platform2.server.163.org:2181,hzadg-mammut-platform3.server.163.org:2181 --add --allow-principal User:*  --consumer --topic spark-test  --group test-consumer-group

添加任意账号在`spark-test` topics有producer权限：
> bin/kafka-acls.sh --authorizer-properties zookeeper.connect=hzadg-mammut-platform2.server.163.org:2181,hzadg-mammut-platform3.server.163.org:2181 --add --allow-principal User:*  --producer --topic spark-test 

* 通过命令行提交spark代码：
>  /usr/ndp/current/spark2_client/bin/spark-submit \  
> --files /usr/ndp/current/kafka_client/conf/kafka_client_jaas.conf,/etc/security/keytabs/kafka.service.keytab \  
> --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=/usr/ndp/current/kafka_client/conf/kafka_client_jaas.conf" \  
> --driver-java-options "-Djava.security.auth.login.config=/usr/ndp/current/kafka_client/conf/kafka_client_jaas.conf" \   
> --class com.netease.spark.DirectKafkaWordCountKerberos \  
> --master local[2]  \  
> ./target/spark-demo-0.1.0-jar-with-dependencies.jar  

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

HBase 环境初始化:
```$xslt
cd /usr/ndp/current/hbase_client

kinit -kt /etc/security/keytabs/hbase.service.keytab hbase/hzadg-mammut-platform1.server.163.org@BDMS.163.COM

./bin/hbase shell

create 'hbase-test', 'f1'
put 'hbase-test', 'row1', 'f1:a', 'v1'
put 'hbase-test', 'row2', 'f1:a', 'v2'
```

在kerberos环境下有两种方式运行：

1. 基于spark提供keytab/principal模式访问HBase：
```shell
/usr/ndp/current/spark2_client/bin/spark-submit \
--conf spark.yarn.keytab=/etc/security/keytabs/hbase.service.keytab \
--conf spark.yarn.principal=hbase/hzadg-mammut-platform1.server.163.org@BDMS.163.COM \
--class com.netease.spark.hbase.HBaseTest \
--master local[2]  \
./target/spark-demo-0.1.0-jar-with-dependencies.jar  
```

2. 自己手动kinit 然后再运行程序（不推荐:https://marsishandsome.github.io/slides/gen/HadoopSecurity.html#slide25）:

```shell

kinit -kt /etc/security/keytabs/hbase.service.keytab hbase/hzadg-mammut-platform1.server.163.org@BDMS.163.COM

/usr/ndp/current/spark2_client/bin/spark-submit \
--class com.netease.spark.hbase.HBaseTest \
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
--class com.netease.spark.hbase.KafkaToHbase \
--master local[2]  \
./target/spark-demo-0.1.0-jar-with-dependencies.jar  
```
