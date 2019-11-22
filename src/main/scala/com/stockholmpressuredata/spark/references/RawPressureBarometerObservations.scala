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

/**
  *
  * @param year year
  * @param month month
  * @param day day
  * @param barometer_observation1 barometer observation1
  * @param barometer_temperature_observation1 barometer temperature observation1
  * @param barometer_observation2 barometer observation2
  * @param barometer_temperature_observation2 barometer temperature observation2
  * @param barometer_observation3 barometer observation3
  * @param barometer_temperature_observation3 barometer temperature observation3
  */
case class RawPressureBarometerObservations(
  year: Int,
  month: Int,
  day: Int,
  barometer_observation1: Option[Double],
  barometer_temperature_observation1: Option[Double],
  barometer_observation2: Option[Double],
  barometer_temperature_observation2: Option[Double],
  barometer_observation3: Option[Double],
  barometer_temperature_observation3: Option[Double]
  )
object RawPressureBarometerObservations extends BaseTable[RawPressureBarometerObservations]{
}
