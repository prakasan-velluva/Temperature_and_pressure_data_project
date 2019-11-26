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
package com.stockholm.pressuredata.processor

import com.stockholm.pressuredata.references._
import com.stockholm.constants.PressureDataDefaultRunArgs
import java.io.FileNotFoundException
import org.apache.spark.sql.SparkSession
import scala.util.Failure

object PressureDataMain {
    def main(args: Array[String]): Unit = {
      val runArgs = PressureDataRunArguments(
      args,
      thermometerDataFilePathDefault=PressureDataDefaultRunArgs.thermometerDataFilePathDefault,
      barometerDataFilePathDefault=PressureDataDefaultRunArgs.barometerDataFilePathDefault,
      mmHgBarometerDataFilePathDefault=PressureDataDefaultRunArgs.mmHgBarometerDataFilePathDefault,
      hPaBarometerDataFilePathDefault=PressureDataDefaultRunArgs.hPaBarometerDataFilePathDefault
    )

    val spark = SparkSession
      .builder()
      .master(runArgs.sparkMaster)
      .appName("TemperatureDataMain")
      .enableHiveSupport()
      .getOrCreate()
    try
    {


     val rawPressureBarometerObservationsLoad = RawPressureBarometerObservations.readData(spark,runArgs.barometerDataFilePath)
     val rawPressureThermometerObservationsLoad = RawPressureThermometerObservations.readData(spark,runArgs.thermometerDataFilePath)
     val mmHgBarometerObservationsLoad = MmHgBarometerObservations.readData(spark,runArgs.mmHgBarometerDataFilePath)
     val hpaBarometerObservationsLoad = HpaBarometerObservations.readData(spark,runArgs.hPaBarometerDataFilePath)

     val rawPressureBarometerObservationsCleance = PressureDataProcessing.nullToNanConverterPressureBarometerObservations(spark, rawPressureBarometerObservationsLoad)
     val rawPressureThermometerObservationsCleance = PressureDataProcessing.nullToNanConverterPressureThermometerObservations(spark, rawPressureThermometerObservationsLoad)
     val mmHgBarometerObservationsCleance = PressureDataProcessing.nullToNanConverterMmHgBarometerObservations(spark, mmHgBarometerObservationsLoad)
     val hpaBarometerObservationsCleance = PressureDataProcessing.nullToNanConverterHpaBarometerObservations(spark, hpaBarometerObservationsLoad)

     val finalPressureObservationsData = PressureDataProcessing.unionDataSets(spark, rawPressureBarometerObservationsCleance,rawPressureThermometerObservationsCleance,mmHgBarometerObservationsCleance,hpaBarometerObservationsCleance)
    }catch {
        case ex: FileNotFoundException => {
          println(s"File "+runArgs.hPaBarometerDataFilePath +"not found")
          Failure(ex)
        }
        case unknown: Exception => {
          println(s"Unknown exception : $unknown")
          Failure(unknown)
        }
      }
    }
}