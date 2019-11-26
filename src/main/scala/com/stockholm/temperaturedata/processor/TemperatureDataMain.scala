/**
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
package com.stockholm.temperaturedata.processor

import java.io.FileNotFoundException
import com.stockholm.temperaturedata.references.RawIndividualTemperatureObservations
import com.stockholm.constants.TemperatureDataDefaultArgs
import org.apache.spark.sql.SparkSession
import scala.util.Failure

object TemperatureDataMain {
  def main(args: Array[String]): Unit = {

    val runArgs = TemperatureDataRunArguments(
      args,
      temperatureDataFilePathDefault=TemperatureDataDefaultArgs.temperatureDataFilePathDefault
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