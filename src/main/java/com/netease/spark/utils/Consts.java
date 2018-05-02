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

package com.netease.spark.utils;

public class Consts {
  // kafka params
  public static final String KAFKA_ZK = "kafka.zookeeper";
  public static final String KAFKA_GROUP = "kafka.groupid";
  public static final String KAFKA_TOPICS = "kafka.topics";
  public static final String KAFKA_NUM_STREAMS = "kafka.num.streams";
  public static final String KAFKA_BROKERS = "kafka.brokers";

  // hdfs params
  public static final String HDFS_PATH = "hdfs.path";

  public static final String HBASE_TABLE = "hbase.table";
  public static final String HBASE_ZK = "hbase.zk";
  public static final String HBASE_PRINCIPLE = "hbase.principle";
  public static final String HBASE_KEYTAB = "hbase.keytab";
}
