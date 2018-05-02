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

package com.netease.spark;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.netease.spark.utils.Consts;
import com.netease.spark.utils.JConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JavaKafkaToHBase {
  private final static Logger LOGGER = LoggerFactory.getLogger(JavaKafkaToHBase.class);

  private static HConnection connection = null;
  private static HTableInterface table = null;

  public static void openHBase(String tablename) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    synchronized (HConnection.class) {
      if (connection == null)
        connection = HConnectionManager.createConnection(conf);
    }

    synchronized (HTableInterface.class) {
      if (table == null) {
        table = connection.getTable("recsys_logs");
      }
    }
  }

  public static void closeHBase() {
    if (table != null)
      try {
        table.close();
      } catch (IOException e) {
        LOGGER.error("关闭 table 出错", e);
      }
    if (connection != null)
      try {
        connection.close();
      } catch (IOException e) {
        LOGGER.error("关闭 connection 出错", e);
      }
  }

  public static void main(String[] args) throws Exception {
    String hbaseTable = JConfig.getInstance().getProperty(Consts.HBASE_TABLE);
    String kafkaBrokers = JConfig.getInstance().getProperty(Consts.KAFKA_BROKERS);
    String kafkaTopics = JConfig.getInstance().getProperty(Consts.KAFKA_TOPICS);
    String kafkaGroup = JConfig.getInstance().getProperty(Consts.KAFKA_GROUP);

    // open hbase
    try {
      openHBase(hbaseTable);
    } catch (IOException e) {
      LOGGER.error("建立HBase 连接失败", e);
      System.exit(-1);
    }

    SparkConf conf = new SparkConf().setAppName("JavaKafakaToHBase");
    JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000));

    Set<String> topicsSet = new HashSet<>(Arrays.asList(kafkaTopics.split(",")));
    Map<String, Object> kafkaParams = new HashMap<>();
    kafkaParams.put("bootstrap.servers", kafkaBrokers);
    kafkaParams.put("key.deserializer", StringDeserializer.class);
    kafkaParams.put("value.deserializer", StringDeserializer.class);
    kafkaParams.put("group.id", kafkaGroup);
    kafkaParams.put("auto.offset.reset", "earliest");
    kafkaParams.put("enable.auto.commit", false);

    // Create direct kafka stream with brokers and topics
    final JavaInputDStream<ConsumerRecord<String, String>> stream =
        KafkaUtils.createDirectStream(
            ssc,
            LocationStrategies.PreferConsistent(),
            ConsumerStrategies.<String, String>Subscribe((Collection<String>)Arrays.asList((String[])topicsSet.toArray()), kafkaParams)
        );


    JavaDStream<String> lines = stream.map(new Function<ConsumerRecord<String, String>, String>() {
      private static final long serialVersionUID = -1801798365843350169L;

      @Override
      public String call(ConsumerRecord<String, String> record) {
        return record.value();
      }
    }).filter(new Function<String, Boolean>() {
      private static final long serialVersionUID = 7786877762996470593L;

      @Override
      public Boolean call(String msg) throws Exception {
        return msg.length() > 0;
      }
    });

    JavaDStream<Long> nums = lines.count();

    nums.foreachRDD(new VoidFunction<JavaRDD<Long>>() {
      private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");

      @Override
      public void call(JavaRDD<Long> rdd) throws Exception {
        Long num = rdd.take(1).get(0);
        String ts = sdf.format(new Date());
        Put put = new Put(Bytes.toBytes(ts));
        put.add(Bytes.toBytes("f"), Bytes.toBytes("nums"), Bytes.toBytes(num));
        table.put(put);
      }
    });

    ssc.start();
    ssc.awaitTermination();
    closeHBase();
  }
}
