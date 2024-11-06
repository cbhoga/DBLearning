# Databricks notebook source
# MAGIC %run ../../Includes/_common

# COMMAND ----------

def _create_demo_tmp_vw():
    print("Creating the temp view demo_tmp_vw")

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW demo_tmp_vw(name, value) AS VALUES
        ("Yi", 1),
        ("Ali", 2),
        ("Selina", 3)
        """)

# COMMAND ----------

lesson_config = LessonConfig(name = None,
                             create_schema = True,
                             create_catalog = False,
                             requires_uc = False,
                             installing_datasets = True,
                             enable_streaming_support = False,
                             enable_ml_support = False)

DA = DBAcademyHelper(course_config=course_config,
                     lesson_config=lesson_config)
DA.reset_lesson()
DA.init()

_create_demo_tmp_vw()

DA.conclude_setup()

# COMMAND ----------

print(f"DA:                   {DA}")
print(f"DA.username:          {DA.username}")
print(f"DA.paths.working_dir: {DA.paths.working_dir}")
print(f"DA.schema_name:       {DA.schema_name}")

# COMMAND ----------

path = f"{DA.paths.datasets}"
dbutils.fs.ls(path)

# COMMAND ----------

path = f"{DA.paths.datasets}"
files = dbutils.fs.ls(path)
display(files)

# COMMAND ----------

DA.cleanup()
