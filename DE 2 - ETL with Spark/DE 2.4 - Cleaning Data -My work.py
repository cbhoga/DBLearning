# Databricks notebook source
# MAGIC %run ./Includes/Classroom-Setup-02.4

# COMMAND ----------

user_id
user_first_touch_timestamp
email
updated

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(*), count(user_id),count(user_first_touch_timestamp), count(email), count(updated) from users_dirty

# COMMAND ----------

#inspect missing data
from pyspark.sql.functions import col, isnan, when, count, isnull
usersDF = spark.read.table("users_dirty")

usersDF.select([count(when(isnull(c), c)).alias(c) for c in usersDF.columns]).show()

usersDF.selectExpr("count_if(email IS NULL)")
usersDF.where(col("email").isNull()).count()

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(*) from users_dirty

# COMMAND ----------

usersDF.distinct().display()

# COMMAND ----------

#verified

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW deduped_users AS
# MAGIC Select user_id,user_first_touch_timestamp,max(email) as email, max(updated) as updated from users_dirty
# MAGIC where user_id is not null
# MAGIC Group by user_id, user_first_touch_timestamp;
# MAGIC
# MAGIC select count(*) from deduped_users
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, count, isnan, when, max
dedupedDF = (usersDF
             .where(col("user_id").isNotNull())
             .groupby("user_id", "user_first_touch_timestamp")
             .agg(max("email").alias("email"),max("updated").alias("updated")))

dedupedDF.count()

# COMMAND ----------

#verified

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct(user_id, user_first_touch_timestamp)) from users_dirty where user_id is not null;

# COMMAND ----------

countDF = usersDF.dropDuplicates(
    ["user_id", "user_first_touch_timestamp"]
).filter(
    col("user_id").isNotNull()
).count()

display(countDF)

# COMMAND ----------

#verified

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(row_count) <= 1 no_duplicates FROM(
# MAGIC select user_id,count(*) as row_count from deduped_users
# MAGIC group by user_id)

# COMMAND ----------

from pyspark.sql.functions import count
from pyspark.sql import functions as F

users_count = dedupedDF.groupBy("user_id").count().withColumnRenamed("count", "row_count")
no_duplicates = users_count.filter(F.col("row_count") > 1).count()

users_count.show()
users_count.filter(F.col("row_count") > 1).show()


# COMMAND ----------

from pyspark.sql.functions import count
display(dedupedDF.groupBy("user_id")
.agg(count("user_id").alias("row_count"))
.select(max("row_count")<=1).alia("no_duplicates"))

# COMMAND ----------

#confirm that each email is associated with at more one user_id

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SELECT max(user_count)<= 1 at_most_one_id FROM (
# MAGIC Select email,count(user_id) as user_count from deduped_users
# MAGIC where email is not null
# MAGIC group by email)

# COMMAND ----------

display(dedupedDF
        .groupBy("email")
        .agg(count("email").alias("row_count"))
        .where(col("email").isNotNull())
        .select((max("row_count") <= 1).alias("no_duplicates")))
        

# COMMAND ----------

# MAGIC %sql
# MAGIC Select *,
# MAGIC date_format(first_touch,"MMM d,yyyy") AS first_touch_date,
# MAGIC date_format(first_touch,"HH:mm:ss")AS first_touch_time,
# MAGIC regexp_extract(email,"(?<=@).+",0) as email_domain
# MAGIC FROM(
# MAGIC   select *, CAST(user_first_touch_timestamp/1e6 as timestamp)AS first_touch from deduped_users
# MAGIC )

# COMMAND ----------


