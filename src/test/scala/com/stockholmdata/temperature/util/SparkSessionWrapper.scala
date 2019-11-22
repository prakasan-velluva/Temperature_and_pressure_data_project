package com.stockholmdata.temperature.util

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  lazy val spark : SparkSession = {

    val spark = SparkSession.builder()
      .config("spark.master", "local[*]")
      .appName("Tepmerature Data Test")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir("/tmp/spark_checkpoint")

    spark
  }
}