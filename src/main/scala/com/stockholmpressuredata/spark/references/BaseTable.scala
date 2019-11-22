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
package com.stockholmpressuredata.spark.references

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import scala.reflect.runtime.universe._
import org.apache.spark.sql.types.StructType

trait BaseTable[T] {
  def readData[A](spark: SparkSession, dataFiles: String)
  (implicit encoder: Encoder[T], tag: TypeTag[T]): Dataset[T] = {
  spark
  .read
  .option("header", "true")
  .option("delimiter", "\\t")
  .option("inferSchema", "false")
  .schema(schemaFor[T].dataType.asInstanceOf[StructType])
  .csv(dataFiles)
  .as[T]
}
}