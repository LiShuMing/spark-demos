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

package com.netease.spark.streaming.hbase;

import com.netease.spark.utils.Consts;
import com.netease.spark.utils.HBaseConnPool;
import com.netease.spark.utils.JConfig;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JavaKafkaToHBaseKerberos {
  private final static Logger LOGGER = LoggerFactory.getLogger(JavaKafkaToHBaseKerberos.class);

  public static void main(String[] args) throws Exception {
    final String hbaseTable = JConfig.getInstance().getProperty(Consts.HBASE_TABLE);
    final String kafkaBrokers = JConfig.getInstance().getProperty(Consts.KAFKA_BROKERS);
    final String kafkaTopics = JConfig.getInstance().getProperty(Consts.KAFKA_TOPICS);
    final String kafkaGroup = JConfig.getInstance().getProperty(Consts.KAFKA_GROUP);

    final SparkConf conf = new SparkConf().setAppName("JavaKafakaToHBase");

    //TODO: refer: https://blog.csdn.net/a123demi/article/details/74935849
    //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    //conf.set("spark.kryo.registrator", "com.netease.spark.utils.KafkaRegistrator");

    JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(2000));

    Set<String> topicsSet = new HashSet<>(Arrays.asList(kafkaTopics.split(",")));
    Map<String, Object> kafkaParams = new HashMap<>();
    kafkaParams.put("bootstrap.servers", kafkaBrokers);
    kafkaParams.put("key.deserializer", StringDeserializer.class);
    kafkaParams.put("value.deserializer", StringDeserializer.class);
    kafkaParams.put("group.id", kafkaGroup);
    kafkaParams.put("auto.offset.reset", "earliest");
    kafkaParams.put("enable.auto.commit", false);
    // 在kerberos环境下，这个配置需要增加
    kafkaParams.put("security.protocol", "SASL_PLAINTEXT");

    // Create direct kafka stream with brokers and topics
    final JavaInputDStream<ConsumerRecord<String, String>> stream =
            KafkaUtils.createDirectStream(
                    ssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<String, String>Subscribe(Arrays.asList(topicsSet.toArray(new String[0])), kafkaParams)
            );

    ssc.checkpoint("/tmp/spark-checkpoint");
    //stream.checkpoint(new Duration(10000));

    stream.foreachRDD(new VoidFunction2<JavaRDD<ConsumerRecord<String, String>>, Time>() {
      private static final long serialVersionUID = 1L;

      @Override
      public void call(JavaRDD<ConsumerRecord<String, String>> rdd, Time time) {

        OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
        for (OffsetRange o: offsetRanges) {
        	LOGGER.info("RDD Offset Info:" +  o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
        }

        rdd.foreachPartition(new VoidFunction<Iterator<ConsumerRecord<String, String>>>() {
          @Override
          public void call(Iterator<ConsumerRecord<String, String>> iterator) throws Exception {
            Connection connection = HBaseConnPool.getConn();
            Table table = HBaseConnPool.getTable(connection, hbaseTable);

            List<Put> putList = new ArrayList<>();
            while (iterator.hasNext()) {
              ConsumerRecord<String, String> record = iterator.next();
              // 自定义rowKey
              String rowKey = record.timestamp() + "_" + record.offset();
              String v = record.value();

              if (v != null && !v.isEmpty()) {
                LOGGER.info("value" + v);
                Put put = new Put(Bytes.toBytes(rowKey));
                put.add(Bytes.toBytes("f"), Bytes.toBytes("v"), Bytes.toBytes(v));
                putList.add(put);
              }
            }

            table.put(putList);
          }
        });
      }
    });

    ssc.start();
    ssc.awaitTermination();
  }
}
