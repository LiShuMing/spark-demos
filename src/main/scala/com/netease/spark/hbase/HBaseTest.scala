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

package com.netease.spark.hbase
import com.netease.spark.utils.{Config, Params}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

object HBaseTest {
  def main(args: Array[String]): Unit = {
    val LOG = Logger.getLogger(getClass.getName)

    val hbaseTable = Config.getInstance().getProperty(Params.HBASE_TABLE).toString

    // TODO, 注意在必须将`HBASE_CONF_DIR`路径设置在Spark Driver的classpath中， 可以通过spark-defaults.conf或者通过--driver-class-path设置；
    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, hbaseTable)

    val sparkConf = new SparkConf().setAppName("HBaseTest")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))

    val sc = new SparkContext(sparkConf)

    // Initialize hBase table if necessary
    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(hbaseTable)) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(args(0)))
      admin.createTable(tableDesc)
    }

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    hBaseRDD.take(10).foreach(x => println(s"${x._1.toString}\t${x._2.toString}"))
    hBaseRDD.count()

    sc.stop()
    admin.close()
  }
}
