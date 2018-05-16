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

package com.netease.spark.streaming.hbase

import com.netease.spark.utils.{BroadConfig, Consts, HbaseWriter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{HBaseAdmin, HConnection, HConnectionManager, HTableInterface}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.security.UserGroupInformation
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaToHbaseKerberos {
  val LOG = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Spark Messager")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))

    val ctx = new SparkContext(sparkConf)
    val ssc = new StreamingContext(ctx, Seconds(2))

    // 创建 kafka stream, 注意这段代码需要放在里面
    val kafkaTopics = BroadConfig.getInstance(ctx).value.getProperty(Consts.KAFKA_TOPICS)
    val kafkaBrokers = BroadConfig.getInstance(ctx).value.getProperty(Consts.KAFKA_BROKERS)
    val kafkaZK = BroadConfig.getInstance(ctx).value.getProperty(Consts.KAFKA_ZK)
    val kafkaGroup = BroadConfig.getInstance(ctx).value.getProperty(Consts.KAFKA_GROUP)
    val kafkaNumStreams = BroadConfig.getInstance(ctx).value.getProperty(Consts.KAFKA_NUM_STREAMS) toInt

    LOG.info(s"topics: $kafkaTopics, zookeeper: $kafkaZK, group id: $kafkaGroup, num streams: $kafkaNumStreams")

    val topicsSet = kafkaTopics.split(",").toSet
    val kafkaConsts = Map[String, Object](
      "bootstrap.servers" -> kafkaBrokers,
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

    // TODO, 注意在必须将`HBASE_CONF_DIR`路径设置在Spark Driver的classpath中， 可以通过spark-defaults.conf或者通过--driver-class-path设置；
    val hbaseTable = BroadConfig.getInstance(ctx).value.getProperty(Consts.HBASE_TABLE)

    messages.foreachRDD { x =>
      x.foreachPartition{ y =>
        val hbaseConf = HBaseConfiguration.create()
        hbaseConf.set(TableInputFormat.INPUT_TABLE, hbaseTable)
        UserGroupInformation.setConfiguration(hbaseConf)

        val hbaseOutputWriter: HbaseWriter = new HbaseWriter()
        val c: Configuration = hbaseConf
        try {
          // Check if the table exist create otherwise
          val admin: HBaseAdmin = new HBaseAdmin(c)
          if (!admin.isTableAvailable(hbaseTable)) {
            val tableDesc: HTableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTable))
            admin.createTable(tableDesc)
          }

          // old HBase API usage
          val hConnection: HConnection = HConnectionManager.createConnection(c)
          val table: HTableInterface = hConnection.getTable(hbaseTable)
          val rowkey: String = String.valueOf(System.currentTimeMillis() / 1000)

          // line put
          y.foreach(record => {
            System.out.println("has received -> " + record)
            hbaseOutputWriter.insertOneLineToHbase(rowkey, "messages", record.toString() , "f1", table)
          })
        } catch {
          case e: Exception  => LOG.warn("write to HBase failed", e)
        }

        null
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

