This program is to read the stockholm atmospheric temperature and pressure data and load the data into HDFS

Dependencies
-------------
Scala  : 2.11.12
Spark : 2.1.0

spark submit command for temperature data
-------------------------------
spark-submit --deploy-mode cluster --class com.stockholm.temperaturedata.processor.TemperatureDataMain --master yarn-client
--driver-memory 20g --executor-memory 20g --jars location ${path_to_spark_jar}/temperature-data.jar
/resources/input/data/temperature/daily/raw/stockholm_daily_temp_obs_1756_1858_t1t2t3.txt


spark submit command for pressure data
-------------------------------
spark-submit --deploy-mode cluster --class com.stockholm.pressuredata.processor.PressureDataMain --master yarn-client
--driver-memory 20g --executor-memory 20g --jars location ${path_to_spark_jar}/pressure-data.jar
/resources/input/data/stockholm_barometer_1756_1858.txt
/resources/input/data/stockholm_thermometer_1859_1861.txt
/resources/input/data/stockholm_mmHgunit_1862_1937.txt
/resources/input/data/stockholm_hPaunit_1938_1960.txt
