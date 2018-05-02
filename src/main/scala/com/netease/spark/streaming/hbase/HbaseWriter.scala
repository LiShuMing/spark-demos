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

import java.util

import org.apache.hadoop.hbase.client.{HTableInterface, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD

class HbaseWriter {
  val LOG = Logger.getLogger(getClass.getName)

  /**
    * insert single line into hbase
    * @param rowkey the line rowkey
    * @param qualifier column's family qualifer
    * @param message value of content
    * @param cf column's family name
    * @param table the target table
    */
  def insertToHbase(rowkey: String, qualifier:String, message: String, cf: String, table: HTableInterface ) : Unit = {


    //defines the key
    val put = new Put(Bytes.toBytes(rowkey))
    put.add(Bytes.toBytes(cf), Bytes.toBytes(qualifier), Bytes.toBytes(message))

    table.put(put)
    LOG.info("written single line to hbase " +  message)
  }

  /**
    *
    * bulk load into hbase from received RDD
    * @param rowkey the line rowkey
    * @param qualifier column's family qualifer
    * @param message value of content
    * @param cf column's family name
    * @param table the target table
    */
  def insertToHbase(rowkey: String, qualifier:String, message: RDD[(String, String)], cf: String, table: HTableInterface ) : Unit = {

    //defines the key
    val puts = new util.ArrayList[Put]()
    message.foreach(x => {
      var counter = 0
      if ((x._1 != null)){
        val put = new Put(Bytes.toBytes(rowkey + "-" + x._1.toString))
        LOG.info("insert into hbase:" + rowkey  + "-" + x._1.toString)
        put.add(Bytes.toBytes(cf), Bytes.toBytes(qualifier), Bytes.toBytes(x._1.toString  +  "--|--"  + x._2.toString))
        puts.add(put)
      } else {
        val put = new Put(Bytes.toBytes(rowkey + "kafka empty message"))
        put.add(Bytes.toBytes(cf), Bytes.toBytes(qualifier), Bytes.toBytes(counter.toString))
        puts.add(put)
        counter = counter  +  1
      }
    })
    table.put(puts)
  }

  def insertOneLineToHbase(rowkey: String, qualifier:String, message: String, cf: String, table: HTableInterface ) : Unit = {
    val put = new Put(Bytes.toBytes(rowkey))
    put.add(Bytes.toBytes(cf), Bytes.toBytes(qualifier), Bytes.toBytes(message))
    table.put(put)
  }
}

