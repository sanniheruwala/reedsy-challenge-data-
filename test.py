# import pyspark module

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# create spark session

spark = SparkSession.builder.appName("test").getOrCreate()

# read data from csv file

schema = StructType(
    [
        StructField("popup_name", StringType(), True),
        StructField("blog_post_url", StringType(), True),
        StructField("popup_version|start_date|popup_category", StringType(), True),
        StructField("popup_header", StringType(), True),
        StructField("popup_description", StringType(), True),
        StructField("popup_image_url", StringType(), True),
        StructField("popup_title", StringType(), True),
        StructField("views", IntegerType(), True),
        StructField("registrations", IntegerType(), True)
    ]
)

df = spark.read.option("delimiter", "\t").option("multiline", "true") \
    .option("quote", '"') \
    .option("header", "true") \
    .option("escape", "\\") \
    .option("escape", '"') \
    .csv("data/dataset.tsv", header=True, schema=schema) \
    .withColumnRenamed("popup_version|start_date|popup_category", "array_fields")

# cast array_fields to array type and explode it to get popup_version, start_date, popup_category

df = df.withColumn("array_fields", split(regexp_replace("array_fields", "[\[\]]", ""), "\",\"")) \
    .withColumn("popup_version", regexp_replace(col("array_fields")[0], '"', "")) \
    .withColumn("start_date", regexp_replace(col("array_fields")[1], '"', "")) \
    .withColumn("popup_category", regexp_replace(col("array_fields")[2], '"', "")) \
    .drop("array_fields")

# total registrations and views

df0 = df.select(sum("views").alias("total_views"), sum("registrations").alias("total_registrations"))

# group popup_category and get sum of views and registrations

df1 = df.groupBy("popup_category") \
    .agg(sum("views").alias("views"),
         sum("registrations").alias("registrations"),
         round(sum("registrations") / sum("views"), 3).alias("conversion_rate"),
         round(sum("registrations") / 47432, 3).alias("registration_rate")) \
    .orderBy(desc("registration_rate"))

# group popup_version and get sum of views and registrations

df2 = df.groupBy("popup_version") \
    .agg(sum("views").alias("views"),
         sum("registrations").alias("registrations"),
         round(sum("registrations") / sum("views"), 3).alias("conversion_rate"),
         round(sum("registrations") / 47432, 3).alias("registration_rate")) \
    .orderBy(desc("registration_rate"))

# group start_date and get sum of views and registrations

df3 = df.groupBy("start_date") \
    .agg(sum("views").alias("views"),
         sum("registrations").alias("registrations"),
         round(sum("registrations") / sum("views"), 3).alias("conversion_rate"),
         round(sum("registrations") / 47432, 3).alias("registration_rate")) \
    .orderBy(desc("registration_rate"))

# group popup_name and get sum of views and registrations

df4 = df.groupBy("popup_name") \
    .agg(sum("views").alias("views"),
         sum("registrations").alias("registrations"),
         round(sum("registrations") / sum("views"), 3).alias("conversion_rate"),
         round(sum("registrations") / 47432, 3).alias("registration_rate")) \
    .orderBy(desc("registration_rate"))

# group blog_post_url and get sum of views and registrations

df5 = df.groupBy("blog_post_url") \
    .agg(sum("views").alias("views"),
         sum("registrations").alias("registrations"),
         round(sum("registrations") / sum("views"), 3).alias("conversion_rate"),
         round(sum("registrations") / 47432, 3).alias("registration_rate")) \
    .orderBy(desc("registration_rate"))

# group by all columns and get sum of views and registrations and filter by conversion_rate 1.0

df6 = df.groupBy("popup_name", "blog_post_url", "popup_version", "start_date", "popup_category", "popup_header",
                 "popup_description", "popup_image_url", "popup_title") \
    .agg(sum("views").alias("views"),
         sum("registrations").alias("registrations"),
         round(sum("registrations") / sum("views"), 3).alias("conversion_rate"),
         round(sum("registrations") / 47432, 3).alias("registration_rate")) \
    .orderBy(desc("conversion_rate")) \
    .filter(col("conversion_rate") == 1.0)
