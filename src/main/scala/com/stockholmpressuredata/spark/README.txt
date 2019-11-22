This program is to read the stockholm atmospheric pressure data and load the data into HDFS

Dependencies
--------------
scalaVersion: 2.11.12
spark version: 2.1.0

spark submit command as follows
-------------------------------
spark-submit --deploy-mode cluster --class com.stockholmpressuredata.spark.PressureDataMain$ --conf spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive=true --master yarn https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholm_barometer_1756_1858.txt https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholm_barometer_1859_1861.txt https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholm_barometer_1862_1937.txt https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholm_barometer_1938_1960.txt
