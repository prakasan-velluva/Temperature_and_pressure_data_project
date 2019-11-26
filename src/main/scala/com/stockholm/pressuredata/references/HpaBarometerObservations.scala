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
package com.stockholm.pressuredata.references

/** Barometer observations in hPa unit
  *
  * @param year year
  * @param month month
  * @param day day
  * @param air_pressure1 air pressure value1
  * @param air_pressure2 air pressure value2
  * @param air_pressure3 air pressure value3
  */
case class HpaBarometerObservations(
  year: Int,
  month: Int,
  day: Int,
  air_pressure1: Option[Double],
  air_pressure2: Option[Double],
  air_pressure3: Option[Double]
  )

object HpaBarometerObservations extends BaseTable[HpaBarometerObservations]{
}