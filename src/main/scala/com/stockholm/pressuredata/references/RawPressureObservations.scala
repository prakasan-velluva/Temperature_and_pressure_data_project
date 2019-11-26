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


/** final pressure data set
  *
  * @param year year
  * @param month month
  * @param day day
  * @param barometer_observation1_raw_pressure_barometer
  * @param barometer_temperature_observation1_raw_pressure_barometer
  * @param barometer_observation2_raw_pressure_barometer
  * @param barometer_temperature_observation2_raw_pressure_barometer
  * @param barometer_observation3_raw_pressure_barometer
  * @param barometer_temperature_observation3_raw_pressure_barometer
  * @param barometer_observation1_raw_pressure_thermometer
  * @param thermometer_observation1_raw_pressure_thermometer
  * @param air_pressure_observation1_raw_pressure_thermometer
  * @param barometer_observation2_raw_pressure_thermometer
  * @param thermometer_observation2_raw_pressure_thermometer
  * @param air_pressure_observation2_raw_pressure_thermometer
  * @param barometer_observation3_raw_pressure_thermometer
  * @param thermometer_observation3_raw_pressure_thermometer
  * @param air_pressure_observation3_raw_pressure_thermometer
  * @param air_pressure1_mmhgbarometer
  * @param air_pressure2_mmhgbarometer
  * @param air_pressure3_mmhgbarometer
  * @param air_pressure1_hpabarometer
  * @param air_pressure2_hpabarometer
  * @param air_pressure3_hpabarometer
  */
case class FinalPressureObservations(
  year: Int,
  month: Int,
  day: Int,
  barometer_observation1_raw_pressure_barometer: Option[Double],
  barometer_temperature_observation1_raw_pressure_barometer: Option[Double],
  barometer_observation2_raw_pressure_barometer: Option[Double],
  barometer_temperature_observation2_raw_pressure_barometer: Option[Double],
  barometer_observation3_raw_pressure_barometer: Option[Double],
  barometer_temperature_observation3_raw_pressure_barometer: Option[Double],
  barometer_observation1_raw_pressure_thermometer: Option[Double],
  thermometer_observation1_raw_pressure_thermometer: Option[Double],
  air_pressure_observation1_raw_pressure_thermometer: Option[Double],
  barometer_observation2_raw_pressure_thermometer: Option[Double],
  thermometer_observation2_raw_pressure_thermometer: Option[Double],
  air_pressure_observation2_raw_pressure_thermometer: Option[Double],
  barometer_observation3_raw_pressure_thermometer:  Option[Double],
  thermometer_observation3_raw_pressure_thermometer: Option[Double],
  air_pressure_observation3_raw_pressure_thermometer: Option[Double] ,
  air_pressure1_mmhgbarometer: Option[Double],
  air_pressure2_mmhgbarometer: Option[Double],
  air_pressure3_mmhgbarometer: Option[Double],
  air_pressure1_hpabarometer: Option[Double],
  air_pressure2_hpabarometer: Option[Double],
  air_pressure3_hpabarometer: Option[Double]
  )
