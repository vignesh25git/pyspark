from pyspark.sql import *
from lib.utils import *
from lib.logger import *
from pyspark.sql.types import *
from pyspark.sql.column import *

from pyspark.sql.functions import *
if __name__ == "__main__":
    custpath = "/Users/vigneshvelusamy/Vicky/Technical/pyspark/Pyspark-Handson/data/custs"
    txnspath = "/Users/vigneshvelusamy/Vicky/Technical/pyspark/Pyspark-Handson/data/txns"

    print("Hello spark ")
    conf = get_spark_app_config()
    spark = (SparkSession \
             .builder \
             .config(conf=conf)
             .getOrCreate())
    cust_schema = StructType([StructField("custid",IntegerType()),
                          StructField("firstname",StringType()),
                          StructField("lastname",StringType()),
                          StructField("age",IntegerType()),
                          StructField("profession",StringType())])

    txns_schema = StructType([StructField("txnid",IntegerType()),
                              StructField("txndate",StringType()),
                              StructField("custid",IntegerType()),
                              StructField("txnamt",FloatType()),
                              StructField("category",StringType()),
                              StructField("productname",StringType()),
                              StructField("city",StringType()),
                              StructField("state",StringType()),
                              StructField("trantype",StringType())])
    input("enter")

    custdf = spark.read.schema(cust_schema).csv(custpath,mode="permissive")
    input("enter")

    custdf.show(5)
    input("enter")

    txndf = spark.read.schema(txns_schema).csv(txnspath,mode="dropmalformed")
    input("enter")

    txndf1 = txndf.withColumn("txn_date",to_date(col("txndate"),"MM-dd-yyyy"))
    input("enter")

    #txndf.withColumn("systs",current_timestamp())
    txndf2= txndf1.drop("txndate")
    input("enter")

    txndf2.printSchema()
    input("enter")

    txndf2.show(5)
#    logger = Log4j(spark)
    input("enter")

#    logger.info("Starting HelloSpark")
#    logger.info("Finished HelloSpark")

