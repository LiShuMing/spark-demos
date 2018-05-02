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
package com.netease.spark.streaming;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.netease.spark.utils.Consts;
import com.netease.spark.utils.JConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.Durations;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: JavaDirectKafkaWordCount <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <topics> is a list of one or more kafka topics to consume from
 *
 * Example:
 *    $ bin/run-example streaming.JavaDirectKafkaWordCount broker1-host:port,broker2-host:port \
 *      topic1,topic2
 */

public final class JavaDirectKafkaWordCount {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {
    final String kafkaBrokers = JConfig.getInstance().getProperty(Consts.KAFKA_BROKERS);
    final String kafkaTopics = JConfig.getInstance().getProperty(Consts.KAFKA_TOPICS);
    final String kafkaGroup = JConfig.getInstance().getProperty(Consts.KAFKA_GROUP);

    // Create context with a 2 seconds batch interval
    SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

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
            jssc,
            LocationStrategies.PreferConsistent(),
            ConsumerStrategies.<String, String>Subscribe(Arrays.asList(topicsSet.toArray(new String[0])), kafkaParams)
        );

    /**
    stream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
      @Override
      public void call(JavaRDD<ConsumerRecord<String, String>> rdd) {
        final OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

        // TODO: you can save the offset
        rdd.foreachPartition(new VoidFunction<Iterator<ConsumerRecord<String, String>>>() {
          @Override
          public void call(Iterator<ConsumerRecord<String, String>> consumerRecords) {
            OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
            System.out.println(
                o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
          }
        });
      }
    });*/

    JavaDStream<String> lines = stream.map(new Function<ConsumerRecord<String, String>, String>() {
      @Override
      public String call(ConsumerRecord<String, String> record) {
        return record.value();
      }
    });

    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String x) {
        return Arrays.asList(SPACE.split(x)).iterator();
      }
    });
    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
        new PairFunction<String, String, Integer>() {
          @Override
          public Tuple2<String, Integer> call(String s) {
            return new Tuple2<>(s, 1);
          }
        }).reduceByKey(
        new Function2<Integer, Integer, Integer>() {
          @Override
          public Integer call(Integer i1, Integer i2) {
            return i1 + i2;
          }
        });
    wordCounts.print();

    // Start the computation
    jssc.start();
    jssc.awaitTermination();
  }
}

