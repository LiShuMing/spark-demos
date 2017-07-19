package com.netease.spark


import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DummyStructStreamingScala {

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

    // Basic Usage
    val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count().sort($"count".desc)

    // outputMode: complete console
    // https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
    val query = wordCounts.writeStream
      .format("parquet")
      .option("checkpointLocation", "/tmp/spark-checkpoint-demo/")
      .option("path", "/tmp/spark-azkanban-demo-3/struct-streaming")
      .start()

    /**
    val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count()
    val w = Window.partitionBy($"value").orderBy(desc("count"))
    val topN = wordCounts.withColumn("count", rank.over(w)).where(rank <= 10)

    val query = topN.writeStream
      .outputMode("complete")
      .format("console")
      .start()*/

    query.awaitTermination()
  }
}
