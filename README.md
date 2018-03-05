Spark项目懒人包
==========

项目包含了Java，Scala和Python的简单例子，DummyCountScala和DummyCountJava以及dummycount.py进行分布式数羊到10000并打印10000 is 10000的无聊输出。

### 配置
---
修改pom.xml，确定编译spark.version,默认配置如下：
```
<spark.version>2.0.2</spark.version>
<hadoop.version>2.7.3</hadoop.version>
<scala.version>2.11.8</scala.version>
<scala.binary.version>2.11</scala.binary.version>
<java.version>1.7</java.version>
```

当前Netease Mammut支持的Spark版本有(推荐Spark2.x以上版本):
```
Spark2.0.2-Hadoop2.7.3
Spark2.1.1-Hadoop2.7.3
Spark1.6.2-Hadoop2.7.3
```

### 编译
---
请使用
```
make 
```
成功编译后将在target目录下生成2个jar文件:
spark-demo-${version}.jar: 提交给azkaban这个就行;
spark-demo-${version}-jar-with-dependencies.jar: 提供依赖的jar;

sh build.sh生成spark-demo.zip的文件，供后续mammut平台上传文件包使用。


### 运行(自测)
---

Python:
```
./spark-submit --master loacal[2] ${SPARK_DEMO}/dummycount.py
```
Java:
```
./spark-submit --master local[2] --class com.netease.spark.DummyCountJava  ${SPARK_DEMO}/target/spark-demo-${version}.jar
```
Scala:
```
./spark-submit --master local[2] --class com.netease.spark.DummyCountScala ${SPARK_DEMO}/target/spark-demo-${version}.jar
```
