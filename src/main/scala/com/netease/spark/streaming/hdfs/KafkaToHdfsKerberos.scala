/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netease.spark.streaming.hdfs

import com.netease.spark.utils.{BroadConfig, Consts, Env, HdfsConnection}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 简单的Kafka->Hdfs的应用程序，当前支持访问kerberos环境；
  * 注意：
  * * 配置resources/conf.properties相关配置项；
  * * 默认开启了测试环境，根据实际情况关闭；
  * * 默认开启了checkpoint， 根据实际情况关闭与否；
  * * 有些默认配置，根据实际情况设定；
  */
object KafkaToHdfsKerberos {
  val LOG = Logger.getLogger(getClass.getName)

  private def functionToCreateContext(): StreamingContext = {
    // 加载配置文件, 配置文件示例为: conf.properties
    val sparkConf = new SparkConf().setAppName("Kafka2Hdfs")
      .set("spark.streaming.receiver.writeAheadLog.enable", "true") // 先写日志, 提高容错性, 避免 receiver 挂掉
      .set("spark.streaming.receiver.maxRate", "5000") // 每秒的读取速率
      .set("spark.streaming.blockInterval", "1000ms") // block 的大小, 每个 block interval 的数据对应于一个 task
      .set("spark.streaming.kafka.maxRatePerPartition", "1000")
      .set("spark.streaming.stopGracefullyOnShutdown", "true") // 设置为 true 会 gracefully 的关闭 StreamingContext
      .set("spark.streaming.stopSparkContextByDefault", "true")

    if (Env.TEST) {
      sparkConf.setMaster("local[2]")
    }

    // 创建 spark context 和 streaming context, 注意这里也设置了 checkpoint, 目的用于 stream 的状态恢复
    val ctx = new SparkContext(sparkConf)
    val ssc = new StreamingContext(ctx, Seconds(10))

    // 创建 kafka stream, 注意这段代码需要放在里面
    val topics = BroadConfig.getInstance(ctx).value.getProperty(Consts.KAFKA_TOPICS)
    val brokers = BroadConfig.getInstance(ctx).value.getProperty(Consts.KAFKA_BROKERS)
    val zk = BroadConfig.getInstance(ctx).value.getProperty(Consts.KAFKA_ZK)
    val group = BroadConfig.getInstance(ctx).value.getProperty(Consts.KAFKA_GROUP)
    val numStreams = BroadConfig.getInstance(ctx).value.getProperty(Consts.KAFKA_NUM_STREAMS) toInt

    LOG.info(s"topics: $topics, zookeeper: $zk, group id: $group, num streams: $numStreams")

    // Create direct kafka stream with brokers and topics
    // Direct 方式是在 Spark 1.3 引入的，这种方式保证了数据能够正常处理，这种方式会周期性的查询 Kafka 关于 latest offsets(每个 topic + partition)中，
    // 当处理数据的 job 启动时，Kafka 的 simple API 会读取指定 ranges 中的数据，这种方式有几种优点:
    // 1. 简化并行化：对每个 kafka 中的 partition，有一个 RDD 相对应。
    // 2. 高效：避免数据丢失的同时不需要 Write Ahead Log.
    // 3. Exactly-once semantics.
    // 缺点是这种方式没有更新 zk，基于 zk 的监控工具无法有效监控

    val topicsSet = topics.split(",").toSet
    val kafkaConsts = Map[String, Object](
      "bootstrap.servers" -> "hzadg-mammut-platform8.server.163.org:6667",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test1",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaConsts))

    // Get the lines, split them into words, count the words and print
    var offsetRanges = Array[OffsetRange]()
    val lines = messages.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      //TODO: fault tolerance: save offset to zk/mysql/hbase
      for (o <- offsetRanges) {
        println(s"start offset: ${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
      rdd
    }.map(_.value())

    val hdfsPath = BroadConfig.getInstance(ctx).value.getProperty(Consts.HDFS_PATH)
    LOG.info(s"hdfs path: $hdfsPath")

    // 对我们获取的数据, 进行处理, 保存到 hdfs 中
    lines.foreachRDD { rdd =>
      // only can be execution on driver
      val config = BroadConfig.getInstance(rdd.sparkContext).value
      // executed at the worker
      rdd.foreachPartition {
        partitionOfRecords =>
          val connection = HdfsConnection.getHdfsConnection(config)
          partitionOfRecords.foreach(
            record => {
              // connection.writeUTF(record)
              connection.write(record.getBytes("UTF-8"))
              connection.writeBytes("\n")
            }
          )

          // 每次完了之后进行 flush
          try {
            connection.hflush()
          } catch {
            case e: Exception => LOG.error(s"hflush exception: ${e.getMessage}")
          }
      }
    }

    ssc
  }

  def main(args: Array[String]) {
    // 注意我们这里有个 checkpoint 的恢复机制, 应对 driver 的重启(从 metadata 恢复), 另外也可以应对有状态的操作(不过本示例没有)
    val ssc = StreamingContext.getOrCreate("checkpoint/Kafka2Hdfs", functionToCreateContext _)

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}