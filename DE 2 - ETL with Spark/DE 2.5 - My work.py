# Databricks notebook source
# MAGIC %run ./Includes/Classroom-setup-02.5

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW events_strings AS
# MAGIC SELECT string(key),string(value) FROM events_raw;
# MAGIC
# MAGIC select * from events_strings;

# COMMAND ----------

from pyspark.sql.functions import col
events_stringsDF = (spark
                    .table("events_raw")
                    .select(col("key").cast("string")
                           col("value").cast("string"))
)
display(events_stringsDF)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM events_strings where value:event_name = "finalize" ORDER BY key limit 1;

# COMMAND ----------

display(events_stringsDF
        .where ("value:event_name = 'finalize'")
        .orderBy("key")
        .limit(1)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT schema_of_json('{"device":"Linux","ecommerce":{"purchase_revenue_in_usd":1075.5,"total_item_quantity":1,"unique_items":1},"event_name":"finalize","event_previous_timestamp":1593879231210816,"event_timestamp":1593879335779563,"geo":{"city":"Houston","state":"TX"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_K","item_name":"Standard King Mattress","item_revenue_in_usd":1075.5,"price_in_usd":1195.0,"quantity":1}],"traffic_source":"email","user_first_touch_timestamp":1593454417513109,"user_id":"UA000000106116176"}') AS schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW parsed_events AS SELECT json.* FROM (
# MAGIC SELECT from_json(value, 'STRUCT<device: STRING, ecommerce: STRUCT<purchase_revenue_in_usd: DOUBLE, total_item_quantity: BIGINT, unique_items: BIGINT>, event_name: STRING, event_previous_timestamp: BIGINT, event_timestamp: BIGINT, geo: STRUCT<city: STRING, state: STRING>, items: ARRAY<STRUCT<coupon: STRING, item_id: STRING, item_name: STRING, item_revenue_in_usd: DOUBLE, price_in_usd: DOUBLE, quantity: BIGINT>>, traffic_source: STRING, user_first_touch_timestamp: BIGINT, user_id: STRING>') AS json 
# MAGIC FROM events_strings);
# MAGIC
# MAGIC SELECT * FROM parsed_events

# COMMAND ----------

from pyspark.sql.functions import from_json, schema_of_json
json_string = """
{"device":"Linux","ecommerce":{"purchase_revenue_in_usd":1047.6,"total_item_quantity":2,"unique_items":2},"event_name":"finalize","event_previous_timestamp":1593879787820475,"event_timestamp":1593879948830076,"geo":{"city":"Huntington Park","state":"CA"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_Q","item_name":"Standard Queen Mattress","item_revenue_in_usd":940.5,"price_in_usd":1045.0,"quantity":1},{"coupon":"NEWBED10","item_id":"P_DOWN_S","item_name":"Standard Down Pillow","item_revenue_in_usd":107.10000000000001,"price_in_usd":119.0,"quantity":1}],"traffic_source":"email","user_first_touch_timestamp":1593583891412316,"user_id":"UA000000106459577"}}"""

parsed_eventsDF = (events_stringsDF
                   .select(from_json(col("value"),schema_of_json(json_string)).alias("parsed"))
                   .select("parsed.*")
)

display(parsed_eventsDF)


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW exploded_events AS
# MAGIC SELECT *,explode(items) AS item 
# MAGIC FROM parsed_events;

# COMMAND ----------

from pypyspark.sql.functions import col, to_timestamp,explode
exploded_df = parsed_evensDF.withColumn("items",explode(col("items"))

display(exploded_df.where(size("items") > 2))

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE exploded_events;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT user_id,
# MAGIC       collect_set(event_name) as event_history,
# MAGIC       array_distinct(flatter(collect_set(items.item_id))) as cart_history
# MAGIC       from exploded_events;

# COMMAND ----------

from pyspark.sql.functions import flatten, array_distinct,collect_set
display(exploded_df
        .groupBy("user_id")
        .agg(collect_set("event_name").alias("events_history"),
             array_distinct(flatten(collect_set("items.item_id"))).alias("cart_history")  ))
             

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW item_purchases AS
# MAGIC
# MAGIC SELECT * 
# MAGIC FROM (SELECT *,explode(items) AS item FROM sales) a 
# MAGIC INNER JOIN item_lookup b ON a.item = b.item_id;
# MAGIC
# MAGIC SELECT * FROM item_purchases;

# COMMAND ----------

exploded_saledDF = spark.table("sales").withColumn("item",explode(col("items"))

itemsDF = spark.table("item_lookup")
item_purchasesDF = exploded_saledDF.join(itemsDF,exploded_saledDF.item.item_id == itemsDF.item_id)
display(item_purchasesDF)

# COMMAND ----------

transactionsDF = item_purchasesDF.groupBy("order_id", 
        "email",
        "transaction_timestamp", 
        "total_item_quantity", 
        "purchase_revenue_in_usd", 
        "unique_items",
        "items",
        "item",
        "name",
        "price")
    .pivot("item_id")
    .sum("item.quantity")
)
display(transactionsDF))

display(transactionsDF) 

# COMMAND ----------


