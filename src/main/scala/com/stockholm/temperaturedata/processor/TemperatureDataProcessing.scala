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

import com.stockholm.temperaturedata.references.RawIndividualTemperatureObservations
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

object TemperatureDataProcessing {
  /**Convert any null value in the file to Nan
    *
    * @param spark spark session
    * @param TemperatureData dataset including null values
    * @return dataset null values converted to NaN
    */
  def nullToNanConverter(spark: SparkSession,TemperatureData: Dataset[RawIndividualTemperatureObservations]): Dataset[RawIndividualTemperatureObservations] = {
    import spark.implicits._
   TemperatureData
      .withColumn("evening_observation",when(col("evening_observation").isNull,lit(Double.NaN)).otherwise(col("evening_observation")))
      .withColumn("estimated_tmax",when(col("estimated_tmax").isNull,lit(Double.NaN)).otherwise(col("estimated_tmax")))
      .withColumn("estimated_tmin",when(col("estimated_tmin").isNull,lit(Double.NaN)).otherwise(col("estimated_tmin")))
      .withColumn("estimated_tmean",when(col("estimated_tmean").isNull,lit(Double.NaN)).otherwise(col("estimated_tmean")))
      .as[RawIndividualTemperatureObservations]
  }
  /**Write the dataset to TemperatureDataTable table
    *
    * @param spark spark session
    * @param TemperatureData final temperature dataset
    */
  def temperatureDataWrite(spark: SparkSession,TemperatureData: Dataset[RawIndividualTemperatureObservations]) = {
    TemperatureData.write.partitionBy("year")saveAsTable("TemperaturDataTable")
  }
}
