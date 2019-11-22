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
package com.stockholmpressuredata.spark

import com.stockholmpressuredata.spark.references._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

object PressureDataProcessing {

  /** Convert any null value in the file to NaN convert the barometer dataset to final pressure dataset with all columns
    *
    * @param spark spark session
    * @param rawPressureBarometerObservations barometer dataset
    * @return initial pressure dataset
    */
  def nullToNanConverterPressureBarometerObservations(spark: SparkSession,rawPressureBarometerObservations: Dataset[RawPressureBarometerObservations]): Dataset[FinalPressureObservations] = {
    import spark.implicits._

    rawPressureBarometerObservations
      .withColumn("barometer_observation1_raw_pressure_barometer",when(col("barometer_observation1").isNull,lit(Double.NaN)).otherwise(col("barometer_observation1")))
      .withColumn("barometer_temperature_observation1_raw_pressure_barometer",when(col("barometer_temperature_observation1").isNull,lit(Double.NaN)).otherwise(col("barometer_temperature_observation1")))
      .withColumn("barometer_observation2_raw_pressure_barometer",when(col("barometer_observation2").isNull,lit(Double.NaN)).otherwise(col("barometer_observation2")))
      .withColumn("barometer_temperature_observation2_raw_pressure_barometer",when(col("barometer_temperature_observation2").isNull,lit(Double.NaN)).otherwise(col("barometer_temperature_observation2")))
      .withColumn("barometer_observation3_raw_pressure_barometer",when(col("barometer_observation3").isNull,lit(Double.NaN)).otherwise(col("barometer_observation3")))
      .withColumn("barometer_temperature_observation3_raw_pressure_barometer",when(col("barometer_temperature_observation3").isNull,lit(Double.NaN)).otherwise(col("barometer_temperature_observation3")))
      .withColumn("barometer_observation1_raw_pressure_thermometer",lit(Double.NaN))
      .withColumn("thermometer_observation1_raw_pressure_thermometer",lit(Double.NaN))
      .withColumn("air_pressure_observation1_raw_pressure_thermometer",lit(Double.NaN))
      .withColumn("barometer_observation2_raw_pressure_thermometer",lit(Double.NaN))
      .withColumn("thermometer_observation2_raw_pressure_thermometer",lit(Double.NaN))
      .withColumn("air_pressure_observation2_raw_pressure_thermometer",lit(Double.NaN))
      .withColumn("barometer_observation3_raw_pressure_thermometer",lit(Double.NaN))
      .withColumn("thermometer_observation3_raw_pressure_thermometer",lit(Double.NaN))
      .withColumn("air_pressure_observation3_raw_pressure_thermometer",lit(Double.NaN))
      .withColumn("air_pressure1_mmhgbarometer",lit(Double.NaN))
      .withColumn("air_pressure2_mmhgbarometer",lit(Double.NaN))
      .withColumn("air_pressure3_mmhgbarometer",lit(Double.NaN))
      .withColumn("air_pressure1_hpabarometer",lit(Double.NaN))
      .withColumn("air_pressure2_hpabarometer",lit(Double.NaN))
      .withColumn("air_pressure3_hpabarometer",lit(Double.NaN))
      .selectExpr("year",
        "month",
        "day",
        "barometer_observation1_raw_pressure_barometer",
        "barometer_temperature_observation1_raw_pressure_barometer",
        "barometer_observation2_raw_pressure_barometer",
        "barometer_temperature_observation2_raw_pressure_barometer",
        "barometer_observation3_raw_pressure_barometer",
        "barometer_temperature_observation3_raw_pressure_barometer",
        "barometer_observation1_raw_pressure_thermometer",
        "thermometer_observation1_raw_pressure_thermometer",
        "air_pressure_observation1_raw_pressure_thermometer",
        "barometer_observation2_raw_pressure_thermometer",
        "thermometer_observation2_raw_pressure_thermometer",
        "air_pressure_observation2_raw_pressure_thermometer",
        "barometer_observation3_raw_pressure_thermometer",
        "thermometer_observation3_raw_pressure_thermometer",
        "air_pressure_observation3_raw_pressure_thermometer",
        "air_pressure1_mmhgbarometer",
        "air_pressure2_mmhgbarometer",
        "air_pressure3_mmhgbarometer",
        "air_pressure1_hpabarometer",
        "air_pressure2_hpabarometer",
        "air_pressure3_hpabarometer")
      .as[FinalPressureObservations]
  }

  /** Convert any null value in the file to NaN and convert the thermometer dataset to final pressure dataset with all columns
    *
    * @param spark spark session
    * @param rawPressureThermometerObservations thermometer dataset
    * @return initial pressure dataset
    */
  def nullToNanConverterPressureThermometerObservations(spark: SparkSession,rawPressureThermometerObservations: Dataset[RawPressureThermometerObservations]): Dataset[FinalPressureObservations] = {
    import spark.implicits._

      rawPressureThermometerObservations
      .withColumn("barometer_observation1_raw_pressure_thermometer",when(col("barometer_observation1").isNull,lit(Double.NaN)).otherwise(col("barometer_observation1")))
      .withColumn("thermometer_observation1_raw_pressure_thermometer",when(col("thermometer_observation1").isNull,lit(Double.NaN)).otherwise(col("thermometer_observation1")))
      .withColumn("air_pressure_observation1_raw_pressure_thermometer",when(col("air_pressure_observation1").isNull,lit(Double.NaN)).otherwise(col("air_pressure_observation1")))
      .withColumn("barometer_observation2_raw_pressure_thermometer",when(col("barometer_observation2").isNull,lit(Double.NaN)).otherwise(col("barometer_observation2")))
      .withColumn("thermometer_observation2_raw_pressure_thermometer",when(col("thermometer_observation2").isNull,lit(Double.NaN)).otherwise(col("thermometer_observation2")))
      .withColumn("air_pressure_observation2_raw_pressure_thermometer",when(col("air_pressure_observation2").isNull,lit(Double.NaN)).otherwise(col("air_pressure_observation2")))
      .withColumn("barometer_observation3_raw_pressure_thermometer",when(col("barometer_observation3").isNull,lit(Double.NaN)).otherwise(col("barometer_observation3")))
      .withColumn("thermometer_observation3_raw_pressure_thermometer",when(col("thermometer_observation3").isNull,lit(Double.NaN)).otherwise(col("thermometer_observation3")))
      .withColumn("air_pressure_observation3_raw_pressure_thermometer",when(col("air_pressure_observation3").isNull,lit(Double.NaN)).otherwise(col("air_pressure_observation3")))
      .withColumn("barometer_observation1_raw_pressure_barometer",lit(Double.NaN))
      .withColumn("barometer_temperature_observation1_raw_pressure_barometer",lit(Double.NaN))
      .withColumn("barometer_observation2_raw_pressure_barometer",lit(Double.NaN))
      .withColumn("barometer_temperature_observation2_raw_pressure_barometer",lit(Double.NaN))
      .withColumn("barometer_observation3_raw_pressure_barometer",lit(Double.NaN))
      .withColumn("barometer_temperature_observation3_raw_pressure_barometer",lit(Double.NaN))
      .withColumn("air_pressure1_mmhgbarometer",lit(Double.NaN))
      .withColumn("air_pressure2_mmhgbarometer",lit(Double.NaN))
      .withColumn("air_pressure3_mmhgbarometer",lit(Double.NaN))
      .withColumn("air_pressure1_hpabarometer",lit(Double.NaN))
      .withColumn("air_pressure2_hpabarometer",lit(Double.NaN))
      .withColumn("air_pressure3_hpabarometer",lit(Double.NaN))
      .selectExpr("year",
        "month",
        "day",
        "barometer_observation1_raw_pressure_barometer",
        "barometer_temperature_observation1_raw_pressure_barometer",
        "barometer_observation2_raw_pressure_barometer",
        "barometer_temperature_observation2_raw_pressure_barometer",
        "barometer_observation3_raw_pressure_barometer",
        "barometer_temperature_observation3_raw_pressure_barometer",
        "barometer_observation1_raw_pressure_thermometer",
        "thermometer_observation1_raw_pressure_thermometer",
        "air_pressure_observation1_raw_pressure_thermometer",
        "barometer_observation2_raw_pressure_thermometer",
        "thermometer_observation2_raw_pressure_thermometer",
        "air_pressure_observation2_raw_pressure_thermometer",
        "barometer_observation3_raw_pressure_thermometer",
        "thermometer_observation3_raw_pressure_thermometer",
        "air_pressure_observation3_raw_pressure_thermometer",
        "air_pressure1_mmhgbarometer",
        "air_pressure2_mmhgbarometer",
        "air_pressure3_mmhgbarometer",
        "air_pressure1_hpabarometer",
        "air_pressure2_hpabarometer",
        "air_pressure3_hpabarometer")
      .as[FinalPressureObservations]
  }

  /** Convert any null value in the file to NaN and convert the mmhgunit dataset to final pressure dataset with all columns
    *
    * @param spark spark session
    * @param mmHgBarometerObservations mmhgunit dataset
    * @return initial pressure dataset
    */
  def nullToNanConverterMmHgBarometerObservations(spark: SparkSession,mmHgBarometerObservations: Dataset[MmHgBarometerObservations]): Dataset[FinalPressureObservations] = {
    import spark.implicits._

    mmHgBarometerObservations
      .withColumn("air_pressure1_mmhgbarometer",when(col("air_pressure1").isNull,lit(Double.NaN)).otherwise(col("air_pressure1")))
      .withColumn("air_pressure2_mmhgbarometer",when(col("air_pressure2").isNull,lit(Double.NaN)).otherwise(col("air_pressure2")))
      .withColumn("air_pressure3_mmhgbarometer",when(col("air_pressure3").isNull,lit(Double.NaN)).otherwise(col("air_pressure3")))
      .withColumn("barometer_observation1_raw_pressure_barometer",lit(Double.NaN))
      .withColumn("barometer_temperature_observation1_raw_pressure_barometer",lit(Double.NaN))
      .withColumn("barometer_observation2_raw_pressure_barometer",lit(Double.NaN))
      .withColumn("barometer_temperature_observation2_raw_pressure_barometer",lit(Double.NaN))
      .withColumn("barometer_observation3_raw_pressure_barometer",lit(Double.NaN))
      .withColumn("barometer_temperature_observation3_raw_pressure_barometer",lit(Double.NaN))
      .withColumn("barometer_observation1_raw_pressure_thermometer",lit(Double.NaN))
      .withColumn("thermometer_observation1_raw_pressure_thermometer",lit(Double.NaN))
      .withColumn("air_pressure_observation1_raw_pressure_thermometer",lit(Double.NaN))
      .withColumn("barometer_observation2_raw_pressure_thermometer",lit(Double.NaN))
      .withColumn("thermometer_observation2_raw_pressure_thermometer",lit(Double.NaN))
      .withColumn("air_pressure_observation2_raw_pressure_thermometer",lit(Double.NaN))
      .withColumn("barometer_observation3_raw_pressure_thermometer",lit(Double.NaN))
      .withColumn("thermometer_observation3_raw_pressure_thermometer",lit(Double.NaN))
      .withColumn("air_pressure_observation3_raw_pressure_thermometer",lit(Double.NaN))
      .withColumn("air_pressure1_hpabarometer",lit(Double.NaN))
      .withColumn("air_pressure2_hpabarometer",lit(Double.NaN))
      .withColumn("air_pressure3_hpabarometer",lit(Double.NaN))
      .selectExpr("year",
        "month",
        "day",
        "barometer_observation1_raw_pressure_barometer",
        "barometer_temperature_observation1_raw_pressure_barometer",
        "barometer_observation2_raw_pressure_barometer",
        "barometer_temperature_observation2_raw_pressure_barometer",
        "barometer_observation3_raw_pressure_barometer",
        "barometer_temperature_observation3_raw_pressure_barometer",
        "barometer_observation1_raw_pressure_thermometer",
        "thermometer_observation1_raw_pressure_thermometer",
        "air_pressure_observation1_raw_pressure_thermometer",
        "barometer_observation2_raw_pressure_thermometer",
        "thermometer_observation2_raw_pressure_thermometer",
        "air_pressure_observation2_raw_pressure_thermometer",
        "barometer_observation3_raw_pressure_thermometer",
        "thermometer_observation3_raw_pressure_thermometer",
        "air_pressure_observation3_raw_pressure_thermometer",
        "air_pressure1_mmhgbarometer",
        "air_pressure2_mmhgbarometer",
        "air_pressure3_mmhgbarometer",
        "air_pressure1_hpabarometer",
        "air_pressure2_hpabarometer",
        "air_pressure3_hpabarometer")
      .as[FinalPressureObservations]
  }
  /** Convert any null value in the file to NaN and convert the hpaunit dataset to final pressure dataset with all columns
    *
    * @param spark spark session
    * @param hpaBarometerObservations  hpaunit dataset
    * @return initial pressure dataset
    */
  def nullToNanConverterHpaBarometerObservations(spark: SparkSession,hpaBarometerObservations: Dataset[HpaBarometerObservations]): Dataset[FinalPressureObservations] = {
    import spark.implicits._

    hpaBarometerObservations
      .withColumn("air_pressure1_hpabarometer",when(col("air_pressure1").isNull,lit(Double.NaN)).otherwise(col("air_pressure1")))
      .withColumn("air_pressure2_hpabarometer",when(col("air_pressure2").isNull,lit(Double.NaN)).otherwise(col("air_pressure2")))
      .withColumn("air_pressure3_hpabarometer",when(col("air_pressure3").isNull,lit(Double.NaN)).otherwise(col("air_pressure3")))
      .withColumn("barometer_observation1_raw_pressure_barometer",lit(Double.NaN))
      .withColumn("barometer_temperature_observation1_raw_pressure_barometer",lit(Double.NaN))
      .withColumn("barometer_observation2_raw_pressure_barometer",lit(Double.NaN))
      .withColumn("barometer_temperature_observation2_raw_pressure_barometer",lit(Double.NaN))
      .withColumn("barometer_observation3_raw_pressure_barometer",lit(Double.NaN))
      .withColumn("barometer_temperature_observation3_raw_pressure_barometer",lit(Double.NaN))
      .withColumn("barometer_observation1_raw_pressure_thermometer",lit(Double.NaN))
      .withColumn("thermometer_observation1_raw_pressure_thermometer",lit(Double.NaN))
      .withColumn("air_pressure_observation1_raw_pressure_thermometer",lit(Double.NaN))
      .withColumn("barometer_observation2_raw_pressure_thermometer",lit(Double.NaN))
      .withColumn("thermometer_observation2_raw_pressure_thermometer",lit(Double.NaN))
      .withColumn("air_pressure_observation2_raw_pressure_thermometer",lit(Double.NaN))
      .withColumn("barometer_observation3_raw_pressure_thermometer",lit(Double.NaN))
      .withColumn("thermometer_observation3_raw_pressure_thermometer",lit(Double.NaN))
      .withColumn("air_pressure_observation3_raw_pressure_thermometer",lit(Double.NaN))
      .withColumn("air_pressure1_mmhgbarometer",lit(Double.NaN))
      .withColumn("air_pressure2_mmhgbarometer",lit(Double.NaN))
      .withColumn("air_pressure3_mmhgbarometer",lit(Double.NaN))
      .selectExpr("year",
        "month",
        "day",
        "barometer_observation1_raw_pressure_barometer",
        "barometer_temperature_observation1_raw_pressure_barometer",
        "barometer_observation2_raw_pressure_barometer",
        "barometer_temperature_observation2_raw_pressure_barometer",
        "barometer_observation3_raw_pressure_barometer",
        "barometer_temperature_observation3_raw_pressure_barometer",
        "barometer_observation1_raw_pressure_thermometer",
        "thermometer_observation1_raw_pressure_thermometer",
        "air_pressure_observation1_raw_pressure_thermometer",
        "barometer_observation2_raw_pressure_thermometer",
        "thermometer_observation2_raw_pressure_thermometer",
        "air_pressure_observation2_raw_pressure_thermometer",
        "barometer_observation3_raw_pressure_thermometer",
        "thermometer_observation3_raw_pressure_thermometer",
        "air_pressure_observation3_raw_pressure_thermometer",
        "air_pressure1_mmhgbarometer",
        "air_pressure2_mmhgbarometer",
        "air_pressure3_mmhgbarometer",
        "air_pressure1_hpabarometer",
        "air_pressure2_hpabarometer",
        "air_pressure3_hpabarometer")
      .as[FinalPressureObservations]
  }

  /** union all the different datasets to make the final data available
    *
    * @param spark sparksession
    * @param rawPressureBarometerObservationsCleance barometerobservation dataset
    * @param rawPressureThermometerObservationsCleance thermometer observation dataset
    * @param mmHgBarometerObservationsCleance mmHg unit dataset
    * @param hpaBarometerObservationsCleance hPa unit dataset
    * @return final pressure dataset
    */
  def unionDataSets(spark: SparkSession,rawPressureBarometerObservationsCleance: Dataset[FinalPressureObservations],rawPressureThermometerObservationsCleance:Dataset[FinalPressureObservations],mmHgBarometerObservationsCleance:Dataset[FinalPressureObservations],hpaBarometerObservationsCleance:Dataset[FinalPressureObservations]): Dataset[FinalPressureObservations] = {
    import spark.implicits._

     rawPressureThermometerObservationsCleance
      .union(rawPressureBarometerObservationsCleance)
      .union(mmHgBarometerObservationsCleance)
      .union(hpaBarometerObservationsCleance)
      .as[FinalPressureObservations]
  }
  /** Write the final dataset to PressureDataTable table
    *
    * @param spark spark session
    * @param pressureData final pressure dataset
    */
  def pressureDataWrite(spark: SparkSession,pressureData: Dataset[FinalPressureObservations]) = {
    pressureData.write.partitionBy("year")saveAsTable("PressureDataTable")
  }
}
