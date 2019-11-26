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

/** Data arguments to run
  *
  * @param isLocalMode boolean value specifying the run mode
  * @param sparkMaster spark master
  * @param temperatureDataFilePath file location
  */
case class TemperatureDataRunArguments(
  isLocalMode: Boolean,
  sparkMaster: String,
  temperatureDataFilePath: String
    )
object TemperatureDataRunArguments {
  /** Setting up the arguments to run
    *
    * @param args run Arguments
    * @param temperatureDataFilePathDefault Defaullt
    * @return program run arguments
    */
  def apply(
             args: Array[String],
             temperatureDataFilePathDefault:String
           ): TemperatureDataRunArguments = {

    val isLocalMode = if (args.length == 0) true else false

    val sparkMaster = if (isLocalMode) "local[*]" else "yarn"
    val temperatureDataFilePath = if (isLocalMode) temperatureDataFilePathDefault else args(1)

    TemperatureDataRunArguments(
      isLocalMode,
      sparkMaster,
      temperatureDataFilePath
    )
  }

}

