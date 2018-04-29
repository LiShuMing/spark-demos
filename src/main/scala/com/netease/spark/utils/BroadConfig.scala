package com.netease.spark.utils

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

// 配置文件的广播变量, 这里使用了单件模式, 为了避免 driver 挂掉
object BroadConfig {
  @volatile private var instance: Broadcast[Properties] = null

  def getInstance(sc: SparkContext, filename: String = "src/main/resources/conf.properties"): Broadcast[Properties] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          val props = new Properties()
          props.load(new FileInputStream(filename))
          instance = sc.broadcast(props)
        }
      }
    }
    instance
  }
}

