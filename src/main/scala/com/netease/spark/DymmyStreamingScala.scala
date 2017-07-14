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
    val ssc = new StreamingContext(conf, Seconds(2))

    val topics = Array("spark-logs")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))


    /**
     val wordCount = stream.map(_.value()).flatMap(_.split(" "))
      .filter(_.equalsIgnoreCase("exception")).map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(2), Seconds(30), 2)*/

    // TOP N Problem
    /**
    stream.map(_.value()).flatMap(_.split(" "))
      .map(x => (x, 1L)).reduceByKey(_ + _).map( x => (x._2, x._1))
      .foreachRDD(rdd => {
        rdd.sortByKey(false)
          .map(x => (x._2, x._1))
          .saveAsTextFile("/tmp/spark-streaming-demo-1/wordCount")
      })*/

    val  topN = stream.map(_.value()).flatMap(_.split(" "))
      .map(x => (x, 1L)).reduceByKey(_ + _).map(x => (x._2, x._1))
      .transform(rdd =>
        rdd.sortByKey(false).map(x => (x._2, x._1))
      )
    topN.print()

    //wordCount.print()
    //wordCount.saveAsTextFiles("/tmp/spark-streaming-demo/wordCount")
    ssc.checkpoint("/tmp/spark-checkpoint")

    /**stream.foreachRDD(rdd =>
    rdd.foreachPartition { iter =>
      iter.foreach(record =>
        //println(s"key:${record.key()}, value:${record.value()}")
        if (record.value().toLowerCase.contains("execption")) {

        }
      )
    })*/


    ssc.start()
    ssc.awaitTermination()
  }
}
