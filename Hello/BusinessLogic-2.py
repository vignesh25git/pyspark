from pyspark.sql import *
from lib.utils import *
from lib.logger import *
from pyspark.sql.types import *
from pyspark.sql.column import *

from pyspark.sql.functions import *
if __name__ == "__main__":
    custpath = "/Users/vigneshvelusamy/Vicky/Technical/pyspark/Pyspark-Handson/data/custs1"
    txnspath = "/Users/vigneshvelusamy/Vicky/Technical/pyspark/Pyspark-Handson/data/txns1"

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
                              StructField("dt",StringType()),
                              StructField("custid",IntegerType()),
                              StructField("amt",FloatType()),
                              StructField("category",StringType()),
                              StructField("product",StringType()),
                              StructField("city",StringType()),
                              StructField("state",StringType()),
                              StructField("transtype",StringType())])

    custdf = (spark.read.schema(cust_schema).
              csv(custpath,mode="permissive",header=False).
              na.drop("all"))
    txndf = (spark.read.schema(txns_schema).
             csv(txnspath,mode="dropmalformed",header=False).
             na.drop("all"))

 #   txndf1 = txndf.withColumn("dt",to_date(col("txndate"),"MM-dd-yyyy"))
    txndf.printSchema()
    txndf1 = txndf.withColumn("dt",to_date("dt","MM-dd-yyyy"))

    enriched_dt_txns=(txndf1.
                      withColumn("yr",year("dt")).
                      withColumn("mth",month("dt")).
                      withColumn("lastday",last_day("dt")).
                      withColumn("nextsunday",next_day("dt",'Sunday')).
                      withColumn("dayofwk",dayofweek("dt")).
                      withColumn("threedaysadd",date_add("dt",3)).
                      withColumn("datediff",datediff(current_date(),"dt")).
                      withColumn("daytrunc",trunc("dt",'mon')))

    enriched_dt_txns.show(2,False)