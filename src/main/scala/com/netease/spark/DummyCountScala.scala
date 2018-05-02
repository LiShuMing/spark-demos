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

package com.netease.spark

import com.netease.spark.utils.Env
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object DummyCountScala {
	def main(args: Array[String]) {
		val sparkConf = new SparkConf().setAppName("DummyCountScala")
		if (Env.TEST) {
			sparkConf.setMaster("local[2]")
		}
		val sc = new SparkContext(sparkConf)
		val distData = sc.parallelize(List.range(1L, 101L), 100)

		distData.flatMap(x => List.range(1L, 101L)).mapPartitions( x => {
				val rng = new Random()
				for (i <- x) yield rng.nextLong()
			}).take(1)

    sc.stop()
	}
}
