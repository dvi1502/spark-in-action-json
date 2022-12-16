"""
 abs function.

 @author rambabu.posa
"""
from pyspark.sql import (SparkSession, Row)
from pyspark.sql.types import (StructType, StructField, IntegerType, ArrayType, StringType)
from pyspark.sql import (functions as F)
import os


# Creates a session on a local master
spark = SparkSession\
    .builder\
    .appName("functions") \
    .master("local[*]")\
    .getOrCreate()

# ----------------------------------------------------
df = spark.read\
    .format("csv") \
    .option("header", True) \
    .load("./data/functions/functions.csv")
df = df.withColumn("abs", F.abs(F.col("val")))
df.show(5)

# ----------------------------------------------------
df = spark.read\
    .format("csv") \
    .option("header", True) \
    .load("./data/functions/trigo_arc.csv")
df = df\
    .withColumn("acos", F.acos(F.col("val"))) \
    .withColumn("acos_by_name", F.acos("val"))
df.show(5)

# ----------------------------------------------------
df = spark.read\
    .format("csv") \
    .option("header", True) \
    .load("./data/functions/dates.csv")
df = df\
    .withColumn("add_months+2", F.add_months(F.col("date_time"), 2))
df.show(5)

# ----------------------------------------------------
df = spark.read\
    .format("csv") \
    .option("header", True) \
    .load("./data/functions/trigo_arc.csv")
df = df\
    .withColumn("asin", F.asin(F.col("val"))) \
    .withColumn("asin_by_name", F.asin("val"))
df.show(5)

# ----------------------------------------------------
df = spark.read\
    .format("csv") \
    .option("header", True) \
    .load("./data/functions/strings.csv")
df = df.withColumn("base64", F.base64(F.col("fname")))
df.show(5)

# ----------------------------------------------------
df = spark.read\
    .format("csv") \
    .option("header", True) \
    .load("./data/functions/strings.csv")
df = df\
    .withColumn("base64", F.base64(F.col("fname"))) \
    .withColumn("unbase64", F.unbase64(F.col("base64"))) \
    .withColumn("name", F.col("unbase64").cast(StringType()))
df.show(5)

# ----------------------------------------------------
df = spark.read\
    .format("csv") \
    .option("header", True) \
    .load("./data/functions/dates.csv")
df = df\
    .withColumn("weekofyear", F.weekofyear(F.col("date_time")))
df.show(5)

# ----------------------------------------------------
df = spark.read\
    .format("csv") \
    .option("header", True) \
    .load("./data/functions/dates.csv")
df = df\
    .withColumn("year", F.year(F.col("date_time")))
df.show(5)

# ----------------------------------------------------
def create_dataFrame(spark):
    schema = StructType([
        StructField("c1", ArrayType(IntegerType()), False),
        StructField("c2", ArrayType(IntegerType()), False)
    ])
    df = spark.createDataFrame([
        Row(c1=[1010, 1012], c2=[1021, 1023, 1025]),
        Row(c1=[2010, 2012, 2014], c2=[2021, 2023]),
        Row(c1=[3010, 3012], c2=[3021, 3023])
    ], schema)
    return df

def myfun(c1,c2):
    return c1+c2

df = df = create_dataFrame(spark)
df.show(5, False)
df.printSchema()

#df = df.withColumn("zip_with", F.arrays_zip(F.col("c1"), F.col("c2")))
#df = df.withColumn("zipped_add", F.expr("transform(zip_with, x -> (x.c1 + x.c2))"))
df = df.withColumn("zip_with", F.expr("zip_with(c1, c2, (x,y) -> (x+y))"))
#df = df.withColumn("zipped_final2", F.expr("zip_with(c1, c2, (x,y) -> (x+y)"))
#df = df.select("c1,c2,zip_with(c1,c2, (x,y) -> sum(x,y))")
#df = df.withColumn("zip_with", F.expr("transform(zip_with, x -> when(x.c1.isNull() | x.c2.isNull(), lit(-1)).otherwise(x.c1 + x.c2))"))

df.show(5)


spark.stop()
