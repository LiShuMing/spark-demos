package com.netease.spark

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object DymmyStreamingScala {

  def main(args: Array[String]): Unit = {

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "10.120.219.35:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark_streaming_group_0",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    val conf = new SparkConf().setAppName("Kafka10 Streaming")
    val ssc = new StreamingContext(conf, Seconds(60))

    val topics = Array("spark-logs")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.foreachRDD(rdd =>
    rdd.foreachPartition { iter =>
      iter.foreach(record =>
        println(s"key:${record.key()}, value:${record.value()}")
      )
    })

    ssc.checkpoint("/tmp/spark-checkpoint")

    ssc.start()
    ssc.awaitTermination()
  }
}
