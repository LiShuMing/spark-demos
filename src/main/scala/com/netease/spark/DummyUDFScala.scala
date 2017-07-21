package com.netease.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import com.netease.spark.udf.GeometricMean

object DummyUDFScala {
    def main(args: Array[String]) {
        val sqlContext = SparkSession
          .builder()
          .appName("Spark Hive Example")
          //.config("spark.sql.warehouse.dir", warehouseLocation)
          .enableHiveSupport()
          .getOrCreate()

        sqlContext.udf.register("gm", new GeometricMean)

        val ids = sqlContext.range(1, 20)
        ids.registerTempTable("tmp_ids")

        val df = sqlContext.sql("select id, id % 3 as group_id from tmp_ids")

        df.registerTempTable("tmp_simple")
        sqlContext.sql("select group_id, gm(id) from tmp_simple group by group_id").show()


        sqlContext.stop()
    }
}
