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
make && sh build.sh
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

### 运行(Azakaban)
在Netease Mammut平台(https://bdms.netease.com)运行，步骤如下：
* 申请集群权限，此步骤略去，找相关负责人;
* 数据开发->新建(左上角)->新建任务->填写任务名称->选择(任务组)->上传刚刚生成的spark-demo.zip文件->选择申请的文件夹->添加描述->确定;
* 在申请的文件夹下会发现，刚刚上传的spark-demo的文件包->选择spark-demo节点->编辑->选择spark版本(跟你编译版本一致)->运行;
* 点击节点查看运行结果;

