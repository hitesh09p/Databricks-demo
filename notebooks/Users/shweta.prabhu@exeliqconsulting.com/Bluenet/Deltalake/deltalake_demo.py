# Databricks notebook source
dbutils.fs.ls("/databricks-datasets/structured-streaming/events/")

# COMMAND ----------

from pyspark.sql.functions import expr
from pyspark.sql.functions import from_unixtime
events = spark.read \
        .option("inferSchema","true")\
        .option("header","true")\
        .json("/databricks-datasets/structured-streaming/events/")\
        .withColumn("date",expr("time"))\
        .drop("time")\
        .withColumn("date", from_unixtime("date", "yyyy-MM-dd"))
events.show(10)

# COMMAND ----------

events.write.format("delta").mode("overwrite").partitionBy("date").save("/delta/events")

# COMMAND ----------

events_delta = spark.read.format("delta").load("/delta/events")

display(events_delta)

# COMMAND ----------


spark.sql("drop table if exists events")

spark.sql("create table events using DELTA location '/delta/events'")

# COMMAND ----------

historical_events = spark.read \
                   .option("inferSchema", "true")\
                  .json("/databricks-datasets/structured-streaming/events/")\
                  .withColumn("date", expr("time-172800"))\
                  .drop("time")\
                  .withColumn("date", from_unixtime("date", "yyyy-MM-dd"))

display(historical_events)

# COMMAND ----------

historical_events.write.format("delta").mode("append").partitionBy("date").save("/delta/events")

# COMMAND ----------

# MAGIC  %sql
# MAGIC Describe history events;
# MAGIC --Describe extended events
# MAGIC --  spark.sql(select count(*) from events version as of 1)
# MAGIC --  spark.sql(select count(*) from events TIMESTAMP as of '2020-01-28T02:09:12.000+0000')

# COMMAND ----------

df1 = spark.read.format("delta").option("versionAsOf", 1).load("/delta/events")
# df1.count()

df2= spark.read.format("delta").option("timestampAsOf", '2021-05-04T18:08:43.000+0000').load("/delta/events")
df2.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from events

# COMMAND ----------

from pyspark.sql.functions import count

display(events_delta.groupBy("action","date").agg(count("action").alias("action_count")).orderBy("date","action"))

# COMMAND ----------

# DBTITLE 1,Update delta tables
from pyspark.sql.functions import *
from delta.tables import * 

deltaTable = DeltaTable.forPath(spark, "/delta/events")

deltaTable.update(col("action") == 'opn', {"action": lit("Opened")})

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from events limit 10

# COMMAND ----------

updatesDF= spark.read.format("delta").option("versionAsOf", 4).load("/delta/events")

# COMMAND ----------

display(updatesDF)

# COMMAND ----------

deltaTable.alias("events").merge(
updatesDF.alias("updates"),
  "events.action = updates.action")\
.whenMatchedUpdate(set = {"action": "updates.action"})\
.whenNotMatchedInsert(values=
 {
   "action": "updates.action",
   "date" : "updates.date"
 }
 )\
.execute()