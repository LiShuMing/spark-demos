package com.netease.spark

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object DummyStreamingScala {

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

    ssc.checkpoint("/tmp/spark-checkpoint")

    /**
     val wordCount = stream.map(_.value()).flatMap(_.split(" "))
      .filter(_.equalsIgnoreCase("exception")).map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(2), Seconds(30), 2)*/
    //wordCount.print()

    // TOP N Problem
    /**
    stream.map(_.value()).flatMap(_.split(" "))
      .map(x => (x, 1L)).reduceByKey(_ + _).map( x => (x._2, x._1))
      .foreachRDD(rdd => {
        rdd.sortByKey(false)
          .map(x => (x._2, x._1))
          .saveAsTextFile("/tmp/spark-streaming-demo-1/wordCount")
      })*/

    /**
    val  topN = stream.map(_.value()).flatMap(_.split(" "))
      .map(x => (x, 1L)).window(Minutes(2), Seconds(30))
      .reduceByKey(_ + _)
      .map(x => (x._2, x._1))
      .transform(rdd =>
        rdd.sortByKey(false).map(x => (x._2, x._1))
      )
    //topN.print()
    topN.saveAsTextFiles("/tmp/spark-streaming-demo-2/wordCount")*/

    // State
    val updateFunc = (v: Seq[Long], s: Option[Long]) => {
      val current = v.sum
      val previous = s.getOrElse(0L)

      Some(current + previous)
    }

    val newUpdateFunc = (iter: Iterator[(String, Seq[Long], Option[Long])]) => {
      iter.flatMap(t => updateFunc(t._2, t._3).map(s => (t._1, s)))
    }

    val sRDD = stream.map(_.value()).flatMap(_.split(" "))
      .map(x => (x, 1L))
      .updateStateByKey[Long](newUpdateFunc, new HashPartitioner(2), true)
    sRDD.foreachRDD(rdd => {
      val sortRDD = rdd.map{case (k, v) => (v, k)}.sortByKey(false)
      val topNData = sortRDD.take(10).map{case (v, k) => (k, v)}
      topNData.foreach( x =>
        println("!!!!Result:" + x)
      )
    })
    //sRDD.saveAsTextFiles("/tmp/spark-streaming-demo-2/wordCount")

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
