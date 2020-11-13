# Databricks notebook source
import pandas as pd
from pyspark.sql.functions import to_timestamp, col
from pyspark.sql import functions as f
import datetime

#Importing files from Git

urls = ['https://raw.githubusercontent.com/LeapAC/leap-data_engineer-assignment/master/meter_data/input_0.csv',
        'https://raw.githubusercontent.com/LeapAC/leap-data_engineer-assignment/master/meter_data/input_1.csv',
        'https://raw.githubusercontent.com/LeapAC/leap-data_engineer-assignment/master/meter_data/input_2.csv',
        'https://raw.githubusercontent.com/LeapAC/leap-data_engineer-assignment/master/meter_data/input_3.csv',
        'https://raw.githubusercontent.com/LeapAC/leap-data_engineer-assignment/master/meter_data/input_4.csv',
        'https://raw.githubusercontent.com/LeapAC/leap-data_engineer-assignment/master/meter_data/input_5.csv',
        'https://raw.githubusercontent.com/LeapAC/leap-data_engineer-assignment/master/meter_data/input_6.csv',
        'https://raw.githubusercontent.com/LeapAC/leap-data_engineer-assignment/master/meter_data/input_7.csv',
        'https://raw.githubusercontent.com/LeapAC/leap-data_engineer-assignment/master/meter_data/input_8.csv',
        'https://raw.githubusercontent.com/LeapAC/leap-data_engineer-assignment/master/meter_data/input_9.csv']


dfs = pd.concat([pd.read_csv(f) for f in urls],ignore_index=True)
meter_data = spark.createDataFrame(dfs) 
del(dfs)

# COMMAND ----------

#Converting interval_date_time to timestamp
meter_data = meter_data.withColumn('interval_date_time', col('interval_date_time').cast('timestamp'))

# COMMAND ----------

#Finding the time span for the data
time_bound_min = meter_data.agg({"interval_date_time": "min"}).collect()[0][0]
time_bound_max = meter_data.agg({'interval_date_time': 'max'}).collect()[0][0]
time_range_hours = (time_bound_max-time_bound_min).total_seconds()/3600

#Building a dataframe with intervals of 15 minutes for the time span
d1 = datetime.datetime.strptime(str(time_bound_min), "%Y-%m-%d %H:%M:%S")
d2 = datetime.datetime.strptime(str(time_bound_max), "%Y-%m-%d %H:%M:%S")
delta = datetime.timedelta(minutes=15)
times = []
while d1 < d2:
    times.append(d1)
    d1 += delta
times.append(d2)
times = spark.createDataFrame(times, 'timestamp')

# COMMAND ----------

#Building a dataframe with all distinct meters ids
meter_id = (meter_data.select("meter_id").distinct())

# COMMAND ----------

#A cross join (cartesian) for meter id and the intervals of 15 minutes for all possible readings
meter_data_full = meter_id.crossJoin(times)

#Join meter_data to get all readings and spot missing ones
meter_data_full = meter_data_full \
  .alias("t1") \
  .join(meter_data.alias("t2"), (meter_data_full.meter_id == meter_data.meter_id) & (meter_data_full.value == meter_data.interval_date_time),how='left') \
  .select("t1.meter_id", "t1.value", "t2.energy_wh") \
  .sort("t1.meter_id", "t1.value") \

#Adding feature Flag_Null
meter_data_full = meter_data_full.withColumn('Flag_Null', f.when(f.col('energy_wh').isNull(), "Yes").otherwise("No"))

#Adding feature avg_wh (Average Consumption) - I've considered the complete time span...
energy_wh_avg = meter_data_full.groupBy('meter_id').sum('energy_wh').withColumnRenamed("sum(energy_wh)", "energy_wh")
energy_wh_avg = energy_wh_avg.withColumn('avg_wh', energy_wh_avg.energy_wh / time_range_hours)

meter_data_full = meter_data_full \
  .alias("t1") \
  .join(energy_wh_avg.alias("t2"), meter_data_full.meter_id == energy_wh_avg.meter_id,how='left') \
  .select("t1.meter_id", "t1.value", "t1.energy_wh", "t1.Flag_Null", "t2.avg_wh") \
  .sort("t1.meter_id", "t1.value") \





# COMMAND ----------

display(meter_data_full)
