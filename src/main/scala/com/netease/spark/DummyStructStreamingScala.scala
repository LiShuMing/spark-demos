package com.netease.spark

import org.apache.spark.sql.SparkSession

class DummyStructStreamingScala {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkStructStreamingKafka")
      .getOrCreate()

    import spark.implicits._

    val topic = "spark-logs"

    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.120.219.35:9092")
      .option("subscribe", topic)
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
