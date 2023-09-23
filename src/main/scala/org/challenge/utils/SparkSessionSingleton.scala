package org.challenge.utils

import org.apache.spark.sql.SparkSession

object SparkSessionSingleton {
  @transient private var instance: SparkSession = _

  def getSession: SparkSession = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance  = SparkSession.builder()
            .appName("Java Spark SQL basic example")
            .config("spark.master", "local[*]")
            .getOrCreate()
          //instance = SparkSession.builder().master("local[*]").getOrCreate()
        }
      }
    }
    instance
  }
}