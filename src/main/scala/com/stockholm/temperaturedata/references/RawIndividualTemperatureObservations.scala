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
package com.stockholm.temperaturedata.references

import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SparkSession}

/** Individual temperature observations
  *
  * @param year year
  * @param month month
  * @param day day
  * @param morning_observation morning observation value
  * @param noon_observation noon observation value
  * @param evening_observation evening observation value
  * @param estimated_tmax estimated maximum temperature
  * @param estimated_tmin estimated minimum temperature
  * @param estimated_tmean estimated average temperature
  */
case class RawIndividualTemperatureObservations(
  year: Int,
  month: Int,
  day: Int,
  morning_observation: Option[Double],
  noon_observation: Option[Double],
  evening_observation: Option[Double],
  estimated_tmax: Option[Double],
  estimated_tmin: Option[Double],
  estimated_tmean: Option[Double]
  )
object RawIndividualTemperatureObservations {
  /** Convert datafile to dataset
    *
    * @param spark spark session
    * @param dataFilePath file location
    * @return temperature dataset
    */
  def readData(spark:SparkSession, dataFilePath: String): Dataset[RawIndividualTemperatureObservations]={
    import spark.implicits._
    spark.read
      .option("header","false")
      .option("inferSchema", "false")
      .option("delimiter", "\\t")
      .schema(schemaFor[RawIndividualTemperatureObservations].dataType.asInstanceOf[StructType])
      .csv(dataFilePath)
      .as[RawIndividualTemperatureObservations]
  }

}
