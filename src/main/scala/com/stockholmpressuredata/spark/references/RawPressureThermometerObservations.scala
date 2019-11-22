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
  * @param thermometer_observation1 thermometer observation1
  * @param air_pressure_observation1 air pressure reduced to 0 degC 1
  * @param barometer_observation2 barometer observation2
  * @param thermometer_observation2 thermometer observation2
  * @param air_pressure_observation2 air pressure reduced to 0 degC 2
  * @param barometer_observation3 barometer observation3
  * @param thermometer_observation3 thermometer observation3
  * @param air_pressure_observation3 air pressure reduced to 0 degC 3
  */
case class RawPressureThermometerObservations(
  year: Int,
  month: Int,
  day: Int,
  barometer_observation1: Option[Double],
  thermometer_observation1: Option[Double],
  air_pressure_observation1: Option[Double],
  barometer_observation2: Option[Double],
  thermometer_observation2: Option[Double],
  air_pressure_observation2: Option[Double],
  barometer_observation3:  Option[Double],
  thermometer_observation3: Option[Double],
  air_pressure_observation3: Option[Double]
  )
object RawPressureThermometerObservations extends BaseTable[RawPressureThermometerObservations]{
}