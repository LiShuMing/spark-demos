package com.netease.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

object DummyHiveSQLScala {

  case class Record(key: Int, value: String)
  // $example off:spark_hive$

  def main(args: Array[String]) {
    // When working with Hive, one must instantiate `SparkSession` with Hive support, including
    // connectivity to a persistent Hive metastore, support for Hive serdes, and Hive user-defined
    // functions. Users who do not have an existing Hive deployment can still enable Hive support.
    // When not configured by the hive-site.xml, the context automatically creates `metastore_db`
    // in the current directory and creates a directory configured by `spark.sql.warehouse.dir`,
    // which defaults to the directory `spark-warehouse` in the current directory that the spark
    // application is started.

    // $example on:spark_hive$
    // warehouseLocation points to the default location for managed databases and tables
    val warehouseLocation = "spark-warehouse"

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      //.config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    sql("CREATE TABLE IF NOT EXISTS tmp_lsm_src (key INT, value STRING)")
    sql("INSERT INTO TABLE tmp_lsm_src values (0, 'val_0'), " +
      "(1, 'val_1'), (2, 'val_2'), (3, 'val_3'), (4, 'val_4'), (5, 'val_5'), (10, 'val_10')," +
      "(11, 'val_11'), (12, 'val_12'), (13, 'val_13'), (14, 'val_14'), (15, 'val_15'), (10, 'val_10')")
    //sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE tmp_lsm_src")

    // Queries are expressed in HiveQL
    sql("SELECT * FROM tmp_lsm_src").show()

    // Aggregation queries are also supported.
    sql("SELECT COUNT(*) FROM tmp_lsm_src").show()

    // The results of SQL queries are themselves DataFrames and support all normal functions.
    val sqlDF = sql("SELECT key, value FROM tmp_lsm_src WHERE key < 10 ORDER BY key")

    // The items in DaraFrames are of type Row, which allows you to access each column by ordinal.
    val stringsDS = sqlDF.map {
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    }
    stringsDS.show()

    // You can also use DataFrames to create temporary views within a SparkSession.
    val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    recordsDF.createOrReplaceTempView("tmp_lsm_records")

    // Queries can then join DataFrame data with data stored in Hive.
    sql("SELECT * FROM tmp_lsm_records r JOIN tmp_lsm_src s ON r.key = s.key").show()

    spark.stop()
  }
}
