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

package com.netease.spark.utils

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

// 配置文件的广播变量, 这里使用了单件模式, 为了避免 driver 挂掉
object Config {
  private var instance: Properties = null

  def getInstance(filename: String = "src/main/resources/conf.properties"): Properties = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          val props = new Properties()
          props.load(new FileInputStream(filename))
          instance = props
        }
      }
    }
    instance
  }
}

