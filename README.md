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

可参考编译分支:
> Spark-2.0.2: https://g.hz.netease.com/hzlishuming/spark-azkaban-demo  
> Spark-2.1.1: https://g.hz.netease.com/hzlishuming/spark-azkaban-demo/tree/spark-v2.1.1  
> Spark-1.6.2: https://g.hz.netease.com/hzlishuming/spark-azkaban-demo/tree/spark-v1.6.2  

当前项目、分支开发结构如下：
> master: Spark-2.0.2  
> branch-spark-v2.1.1: Spark-2.1.1  
> branch-spark-v1.6.2: Spark-1.6.2

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

如果想下载已经编译好的zip，可以查看：https://g.hz.netease.com/hzlishuming/spark-azkaban-demo/issues/1 。

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

### 运行(Mammut)

#### 普通zip包项目运行
在(Mammut平台)[https://bdms.netease.com]运行，步骤如下：
* 申请集群权限，此步骤略去，找相关负责人;
* 数据开发->新建(左上角)->新建任务->填写任务名称->选择(任务组)->上传刚刚生成的spark-demo.zip文件->选择申请的文件夹->添加描述->确定;
* 在申请的文件夹下会发现，刚刚上传的spark-demo的文件包->选择spark-demo节点->编辑->选择spark版本(跟你编译版本一致)->运行;
* 点击节点查看运行结果;

#### Spark-submit的形式运行
##### 依赖文件
1. keytab文件， 需在Mammut平台个人中心下载；
2. 编译script脚本；
3. 将keytab文件、生成的jar包、script脚本打包成一个zip文件；


其中，编辑script shell脚本参考如下：
```
# spark-demo.sh

# login using your keytab file
kinit -kt ./spark-demo-202/mammut.keytab mammut/bigdata@HADOOP.HZ.NETEASE.COM

# spark2.0.2
export SPARK_HOME=/home/hadoop/spark-2.0.2-bin-hadoop2.7
# spark-2.1.1
export SPARK_HOME=/home/hadoop/spark-2.1.1-bin-hadoop2.7

# TODO: set conf as you want!!!
${SPARK_HOME}/bin/spark-submit --class com.netease.spark.DummyCountJava --master yarn  --deploy-mode client --driver-memory 2048M --executor-memory 512M --executor-cores 1 --queue default ./spark-demo-202/spark-demo-202.jar
```

##### 在Mammut平台上使用
1. 新建（左上角）-> 新建任务 -> 作业流 -> 指定目录；
2. 选择刚刚创建的作业流->拖拽一个script类型的作业->点击左侧上传资源->上传刚刚打包的zip文件；
3. 编辑刚刚新建的script作业，书写脚本，如上述实例，脚本为./spark-demo-202/spark-demo.sh;
4. 运行作业，即可操作；

