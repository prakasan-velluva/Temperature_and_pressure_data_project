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
package com.stockholm.temperaturedata

import com.stockholm.temperaturedata.processor.TemperatureDataProcessing
import com.stockholm.temperaturedata.references.RawIndividualTemperatureObservations
import com.stockholm.constants.TemperatureDataDefaultArgs
import com.stockholm.utils.SparkSessionWrapper
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class TemperatureDatTest extends FunSuite with BeforeAndAfter with SparkSessionWrapper {

  private val sampleDataPath: String = TemperatureDataDefaultArgs.testPath

  private var templateData : RawIndividualTemperatureObservations = _

  templateData = RawIndividualTemperatureObservations(
    2013                 //year
    ,1                   //month
    ,1                   //day
    ,Some(5.1)           //morning_observation
    ,Some(4.4)           //noon_observation
    ,Some(2.9)           //evening_observation
    ,Some(5.5)           //estimated_tmax
    ,Some(2.9)           //estimated_tmin
    ,Some(4.1)           //estimated_tmean
  )

  test("Temperature Data") {
    import spark.implicits._
    val temperatureSampleData = Seq(templateData).toDS()
    val temperatureSampleDataSet = TemperatureDataProcessing.nullToNanConverter(spark, temperatureSampleData)
    assert(temperatureSampleDataSet.count() == 1)

    val dataFilePath = RawIndividualTemperatureObservations.readData(spark,sampleDataPath)
    assert(dataFilePath.count()==1)
  }
}