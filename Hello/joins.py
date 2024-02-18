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

    txndf.printSchema()
    txndf1 = txndf.withColumn("dt",to_date("dt","MM-dd-yyyy"))
    print("transaction " ,txndf1.count())
    print("customer " , custdf.count())

    #cross join
    df_cross = custdf.alias("c").join(txndf1.alias("t"))
    print(df_cross.count())

    print(custdf.columns)
    print(set(custdf.columns).intersection(set(txndf.columns)))

    #inner join
    df_inner = custdf.alias("c").join(txndf.alias("t"),on=[col("c.custid")==col("t.custid")],how="inner")
    print("inner ",df_inner.count())

    #left join
    df_left = custdf.alias("c").join(txndf.alias("t"),on=[col("c.custid")==col("t.custid")],how="left")
    print("left ",df_left.count())

    #right join
    df_right = custdf.alias("c").join(txndf.alias("t"),on=[col("c.custid")==col("t.custid")],how="right")
    print("right ", df_right.count())

    #semi join
    df_semi = custdf.alias("c").join(txndf.alias("t"),on=[col("c.custid")==col("t.custid")],how="semi")
    print("semi ", df_semi.count())

    #anti join
    df_anti = custdf.alias("c").join(txndf.alias("t"),on=[col("c.custid")==col("t.custid")],how="anti")
    print("anti ", df_anti.count())