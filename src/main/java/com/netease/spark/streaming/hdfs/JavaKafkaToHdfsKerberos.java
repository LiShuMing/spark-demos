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

package com.netease.spark.streaming.hdfs;

import com.netease.spark.utils.Consts;
import com.netease.spark.utils.HdfsUtils;
import com.netease.spark.utils.JConfig;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public final class JavaKafkaToHdfsKerberos {
  private final static Logger LOGGER = LoggerFactory.getLogger(JavaKafkaToHdfs.class);

  public static void main(String[] args) throws Exception {
    final String kafkaBrokers = JConfig.getInstance().getProperty(Consts.KAFKA_BROKERS);
    final String kafkaTopics = JConfig.getInstance().getProperty(Consts.KAFKA_TOPICS);
    final String kafkaGroup = JConfig.getInstance().getProperty(Consts.KAFKA_GROUP);
    final String hdfsSavePath = JConfig.getInstance().getProperty(Consts.HDFS_PATH);

    // Create context with a 2 seconds batch interval
    SparkConf sparkConf = new SparkConf()
        .setAppName("JavaDirectKafkaWordCount")
        .set("spark.streaming.receiver.writeAheadLog.enable", "true") // 先写日志, 提高容错性, 避免 receiver 挂掉
        .set("spark.streaming.receiver.maxRate", "5000") // 每秒的读取速率
        .set("spark.streaming.blockInterval", "1000ms") // block 的大小, 每个 block interval 的数据对应于一个 task
        .set("spark.streaming.kafka.maxRatePerPartition", "1000")
        .set("spark.streaming.stopGracefullyOnShutdown", "true") // 设置为 true 会 gracefully 的关闭 StreamingContext
        .set("spark.streaming.stopSparkContextByDefault", "true");

    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

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
            jssc,
            LocationStrategies.PreferConsistent(),
            ConsumerStrategies.<String, String>Subscribe(Arrays.asList(topicsSet.toArray(new String[0])), kafkaParams)
        );

    JavaDStream<String> lines = stream.transform(new Function<JavaRDD<ConsumerRecord<String, String>>, JavaRDD<ConsumerRecord<String, String>>>() {
      @Override
      public JavaRDD<ConsumerRecord<String, String>> call(JavaRDD<ConsumerRecord<String, String>> rdd) {
        try {
          //TODO: save the offset or recover from offsets
          OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
          OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
          System.out.println(
              o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
        } catch (Exception e) {
          LOGGER.warn("get offset from rdd failed:", e);
        }

        return rdd;
      }
    }).map(new Function<ConsumerRecord<String, String>, String>() {
      @Override
      public String call(ConsumerRecord<String, String> record) {
        LOGGER.info("value:" + record.value());
        return record.value();
      }
    });

    lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
      @Override
      public void call(JavaRDD<String> rdd) throws Exception {
        rdd.foreachPartition(new VoidFunction<Iterator<String>>() {
          @Override
          public void call(Iterator<String> stringIterator) throws Exception {
            FSDataOutputStream fout = HdfsUtils.getHdfsConnection(hdfsSavePath);

            while (stringIterator.hasNext()) {
              String record = stringIterator.next();
              fout.write(record.getBytes("UTF-8"));
              fout.writeBytes("\n");
            }

            try {
              fout.flush();
            } catch (Exception e) {
              LOGGER.warn("flush to hdfs failed:", e);
            }
          }
          //rdd.saveAsTextFile(hdfsSavePath);
        });

      }
    });

    // Start the computation
    jssc.start();
    jssc.awaitTermination();
  }
}
