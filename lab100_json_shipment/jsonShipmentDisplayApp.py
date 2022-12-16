"""
 Processing of invoices formatted using the schema.org format.

 @author rambabu.posa
"""
import os
from pyspark.sql import SparkSession
import arguments as utils


def main(spark, filename):

    df = spark\
        .read\
        .format("json") \
        .option("multiline", True) \
        .load(filename)

    # Shows at most 5 rows from the dataframe (there's only one anyway)
    df.show(5, 16)
    df.printSchema()

if __name__ == '__main__':

    args = utils.args_reader()
    # Creates a session on a local master
    spark = SparkSession\
        .builder\
        .appName("Display of shipment") \
        .master("local[*]")\
        .getOrCreate()
    # Comment this line to see full log
    spark.sparkContext.setLogLevel('error')
    main(spark, args.datapath)
    spark.stop()
