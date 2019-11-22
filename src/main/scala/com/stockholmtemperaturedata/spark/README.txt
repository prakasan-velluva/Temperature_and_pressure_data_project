This program is to read the stockholm temperature data and load the data into HDFS
spark submit command as follows
-------------------------------
spark-submit --deploy-mode cluster --class com.stockholmtemperaturedata.spark.TemperatureDataMain --conf spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive=true --master yarn https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholm_daily_temp_obs_1756_1858_t1t2t3.txt