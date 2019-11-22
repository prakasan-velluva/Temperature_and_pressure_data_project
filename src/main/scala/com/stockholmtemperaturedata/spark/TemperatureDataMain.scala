/*
*Copyright (c) 2019, TATA Consultancy Services Limited (TCSL)
*All rights reserved.

*Redistribution and use in source and binary forms, with or without
*modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice,
* this list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright notice,
* this list of conditions and the following disclaimer in the documentation
* and/or other materials provided with the distribution.
* Neither the name of TCSL nor the names of its contributors may be
* used to endorse or promote products derived from this software without
* specific prior written permission.
*/
package com.stockholmtemperaturedata.spark

import java.io.FileNotFoundException
import org.apache.spark.sql.{SparkSession}
import scala.util.Failure

/**
* Read temperature data from dataset and load it to HDFS
* Expecting input as temperature data file
* file is separated by tab delimiter
*/
object TemperatureDataMain {
  def main(args: Array[String]): Unit = {

    val runArgs = TemperatureDataRunArguments(
      args,
      temperatureDataFilePathDefault="/in/data/2013-2017_automatic.txt",
      defaultRunMode = "diagnostic"
    )

    val spark = SparkSession
      .builder()
      .master(runArgs.sparkMaster)
      .appName("TemperatureDataMain")
      .enableHiveSupport()
      .getOrCreate()
    try
    {
     val TemperatureDataLoad = RawIndividualTemperatureObservations.readData(spark,runArgs.temperatureDataFilePath)
      val dataCleance = TemperatureDataProcessing.nullToNanConverter(spark, TemperatureDataLoad)
      TemperatureDataProcessing.temperatureDataWrite(spark, dataCleance)
    }catch {
        case ex: FileNotFoundException => {
          println(s"File "+runArgs.temperatureDataFilePath +"not found")
          Failure(ex)
        }
        case unknown: Exception => {
          println(s"Unknown exception : $unknown")
          Failure(unknown)
        }
      }
    }
}