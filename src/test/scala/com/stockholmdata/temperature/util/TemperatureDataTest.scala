package com.stockholmdata.temperature.util

import com.stockholmtemperaturedata.spark._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class TemperatureDatTest extends FunSuite with BeforeAndAfter with SparkSessionWrapper {

  private val sampleDataPath: String = "D:/StockholmProject/src/test/scala/com/stockholmdata/util/in/sample_data.txt"

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