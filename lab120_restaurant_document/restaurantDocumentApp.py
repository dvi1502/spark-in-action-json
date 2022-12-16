"""
 Building a nested document.

 @author rambabu.posa
"""
import os
import logging
from pyspark.sql import (SparkSession, functions as F)
import arguments as utils

def load_businessesDF(spark,path):
    return spark\
        .read\
        .format("csv") \
        .option("header", True) \
        .load(path)


def load_inspectionsDF(spark,path):
    return spark\
        .read\
        .format("csv") \
        .option("header", True) \
        .load(path)


if __name__ == '__main__':

    args = utils.args_reader()

    # Creates a session on a local master
    spark = SparkSession\
        .builder\
        .appName("Building a restaurant fact sheet") \
        .master("local[*]")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("warn")

    businessDf = load_businessesDF(spark,args.datapath)
    businessDf.show(3)
    businessDf.printSchema()

    inspectionDf = load_inspectionsDF(spark,args.datapath1)
    inspectionDf.show(3)
    inspectionDf.printSchema()

    join_condition = businessDf['business_id'] == inspectionDf['business_id']
    resDf = businessDf\
        .join(inspectionDf, join_condition, "inner")\
        .drop(inspectionDf['business_id'])
    resDf.show(3)
    resDf.printSchema()

    struct_field = F.struct(resDf.business_id, resDf.score, resDf.date, resDf.type)
    factSheetDf = resDf\
        .withColumn("inspections", struct_field) \
        .drop("score", "date", "type")
    factSheetDf.show(3)
    factSheetDf.printSchema()

    logging.warning("Before nested join, we have {} rows.".format(factSheetDf.count()))

    left_columns = businessDf.columns
    factSheetDf = factSheetDf\
        .groupBy(left_columns)\
        .agg(F.collect_list(F.col("inspections")))
    factSheetDf.show(3)
    factSheetDf.printSchema()

    logging.warning("After nested join, we have {} rows.".format(factSheetDf.count()))

    spark.stop()
