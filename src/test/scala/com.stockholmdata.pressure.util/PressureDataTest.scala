package com.stockholmdata.pressure.util

import com.stockholmpressuredata.spark.{FinalPressureObservations, PressureDataProcessing}
import com.stockholmpressuredata.spark.references._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class PressureDataTest extends FunSuite with BeforeAndAfter with SparkSessionWrapper {

  private val sampleDataPath: String = "in/sample_data.txt"

  private var templateData : RawPressureThermometerObservations = _

  templateData = RawPressureThermometerObservations(
    1859                  //year
    ,1                   //month
    ,1                   //day
    ,Some(257.3)         //barometer_observation1
    ,Some(13.0)          //thermometer_observation1
    ,Some(257.0)         //air_pressure_observation1
    ,Some(257.1)         //barometer_observation2
    ,None                //thermometer_observation2
    ,Some(256.5)         //air_pressure_observation2
    ,Some(256.4)         //barometer_observation3
    ,Some(17.9)          //thermometer_observation3
    ,Some(255.9)         //air_pressure_observation3

  )
  private var templateData1 : RawPressureBarometerObservations = _

  templateData1 = RawPressureBarometerObservations(
    1756                  //year
    ,1                   //month
    ,1                   //day
    ,Some(25.15)         //barometer_observation1
    ,Some(2.4)           //barometer_temperature_observation1
    ,Some(3.4)           //barometer_observation2
    ,Some(5.5)           //barometer_temperature_observation2
    ,Some(2.9)           //barometer_observation3
    ,Some(4.1)           //barometer_temperature_observation3
  )

  private var templateData2 : MmHgBarometerObservations = _

  templateData2 = MmHgBarometerObservations(
    2013                 //year
    ,1                   //month
    ,1                   //day
    ,Some(5.1)           //air_pressure1
    ,Some(4.4)           //air_pressure2
    ,Some(2.9)           //air_pressure3
  )

  private var templateData3 : HpaBarometerObservations = _

  templateData3 = HpaBarometerObservations(
    2013                  //year
    ,1                   //month
    ,1                   //day
    ,Some(5.1)           //air_pressure1
    ,Some(4.4)           //air_pressure2
    ,Some(2.9)           //air_pressure3
  )

  private var finalData : FinalPressureObservations = _

  finalData = FinalPressureObservations(
    2013                 //year
    ,1                   //month
    ,1                   //day
    ,Some(257.3)         //barometer_observation1_thermometer
    ,Some(13.0)          //thermometer_observation1_thermometer
    ,Some(257.0)         //air_pressure_observation1_thermometer
    ,Some(257.1)         //barometer_observation2_thermometer
    ,None                //thermometer_observation2_thermometer
    ,Some(256.5)         //air_pressure_observation2_thermometer
    ,Some(256.4)         //barometer_observation3_thermometer
    ,Some(17.9)          //thermometer_observation3_thermometer
    ,Some(255.9)         //air_pressure_observation3_thermometer
    ,Some(25.15)         //barometer_observation1_barometer
    ,Some(2.4)           //barometer_temperature_observation1_barometer
    ,Some(3.4)           //barometer_observation2_barometer
    ,Some(5.5)           //barometer_temperature_observation2_barometer
    ,Some(2.9)           //barometer_observation3_barometer
    ,Some(4.1)           //barometer_temperature_observation3_barometer
    ,Some(5.1)           //air_pressure1_mmHg
    ,Some(4.4)           //air_pressure2_mmHg
    ,Some(2.9)           //air_pressure3_mmHg
    ,Some(5.1)           //air_pressure1_hPaData
    ,Some(4.4)           //air_pressure2_hPaData
    ,Some(2.9)           //air_pressure3_hPaData
  )

  private var outputData : FinalPressureObservations = _

  outputData = FinalPressureObservations(
    2013                 //year
    ,1                   //month
    ,1                   //day
    ,Some(257.3)         //barometer_observation1_thermometer
    ,Some(13.0)          //thermometer_observation1_thermometer
    ,Some(257.0)         //air_pressure_observation1_thermometer
    ,Some(257.1)         //barometer_observation2_thermometer
    ,Some(18.0)          //thermometer_observation2_thermometer
    ,Some(256.5)         //air_pressure_observation2_thermometer
    ,Some(256.4)         //barometer_observation3_thermometer
    ,Some(17.9)          //thermometer_observation3_thermometer
    ,Some(255.9)         //air_pressure_observation3_thermometer
    ,Some(25.15)         //barometer_observation1_barometer
    ,Some(2.4)           //barometer_temperature_observation1_barometer
    ,Some(3.4)           //barometer_observation2_barometer
    ,Some(5.5)           //barometer_temperature_observation2_barometer
    ,Some(2.9)           //barometer_observation3_barometer
    ,Some(4.1)           //barometer_temperature_observation3_barometer
    ,Some(5.1)           //air_pressure1_mmHg
    ,Some(4.4)           //air_pressure2_mmHg
    ,Some(2.9)           //air_pressure3_mmHg
    ,Some(5.1)           //air_pressure1_hPaData
    ,Some(4.4)           //air_pressure2_hPaData
    ,Some(2.9)           //air_pressure3_hPaData
  )

  test("Pressure Data") {
    import spark.implicits._
    val pressureSampleData1 = Seq(templateData).toDS()
    val pressureSampleDataSet1 = PressureDataProcessing.nullToNanConverterPressureThermometerObservations(spark, pressureSampleData1 )
    assert(pressureSampleDataSet1.count() == 1)

    val pressureSampleData2 = Seq(templateData1).toDS()
    val pressureSampleDataSet2 = PressureDataProcessing.nullToNanConverterPressureBarometerObservations(spark, pressureSampleData2 )
    assert(pressureSampleDataSet2.count() == 1)

    val pressureSampleData3 = Seq(templateData2).toDS()
    val pressureSampleDataSet3 = PressureDataProcessing.nullToNanConverterMmHgBarometerObservations(spark, pressureSampleData3 )
    assert(pressureSampleDataSet3.count() == 1)

    val pressureSampleData4 = Seq(templateData3).toDS()
    val pressureSampleDataSet4 = PressureDataProcessing.nullToNanConverterHpaBarometerObservations(spark, pressureSampleData4 )
    assert(pressureSampleDataSet4.count() == 1)

    val dataFilePath = RawPressureThermometerObservations.readData(spark,sampleDataPath)
    assert(dataFilePath.count()==4)
  }
}