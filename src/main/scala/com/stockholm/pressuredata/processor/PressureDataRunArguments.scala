/**
*Copyright (c) 2019, TATA Consultancy Services Limited (TCSL)
*All rights reserved.

*Redistribution and use in source and binary forms, with or without
*modification, are permitted provided that the following conditions are met:
*/
package com.stockholm.pressuredata.processor

/** Run Arguments for pressure data program
  *
  * @param isLocalMode boolean value specifying the run mode
  * @param sparkMaster spark master
  * @param thermometerDataFilePath thermometer file location
  * @param barometerDataFilePath barometer file location
  * @param mmHgBarometerDataFilePath mmHg unit file location
  * @param hPaBarometerDataFilePath hPa unit file location
  */
case class PressureDataRunArguments(
  isLocalMode: Boolean,
  sparkMaster: String,
  thermometerDataFilePath: String,
  barometerDataFilePath: String,
  mmHgBarometerDataFilePath: String,
  hPaBarometerDataFilePath: String
  )
object PressureDataRunArguments {

  /** Setting up the arguments to run
    *
    * @param args run Arguments
    * @param thermometerDataFilePathDefault Defaullt thermometer file location
    * @param barometerDataFilePathDefault Defaullt barometer file location
    * @param mmHgBarometerDataFilePathDefault Defaullt mmHg unit file location
    * @param hPaBarometerDataFilePathDefault Defaullt hPa unit file location
    * @return program run arguments
    */
  def apply(
    args: Array[String],
    thermometerDataFilePathDefault: String,
    barometerDataFilePathDefault: String,
    mmHgBarometerDataFilePathDefault: String,
    hPaBarometerDataFilePathDefault: String
    ): PressureDataRunArguments = {

    val isLocalMode = if (args.length == 0) true else false

    val sparkMaster = if (isLocalMode) "local[*]" else "yarn"
    val thermometerDataFilePath = if (isLocalMode) thermometerDataFilePathDefault else args(1)
    val barometerDataFilePath = if (isLocalMode) barometerDataFilePathDefault else args(2)
    val mmHgBarometerDataFilePath = if (isLocalMode) mmHgBarometerDataFilePathDefault else args(3)
    val hPaBarometerDataFilePath = if (isLocalMode) hPaBarometerDataFilePathDefault else args(4)

    PressureDataRunArguments(
      isLocalMode,
      sparkMaster,
      thermometerDataFilePath,
      barometerDataFilePath,
      mmHgBarometerDataFilePath,
      hPaBarometerDataFilePath
    )
  }

}