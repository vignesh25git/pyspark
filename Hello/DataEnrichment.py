from pyspark import *
from pyspark.sql import *
from lib.utils import *
from lib.logger import *
from pyspark.sql.types import *
from pyspark.sql.column import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    file1 = "/Users/vigneshvelusamy/Vicky/Technical/pyspark/Pyspark-Handson/data/custs"
    conf = get_spark_app_config()
    spark = (SparkSession \
             .builder \
             .config(conf=conf)
             .enableHiveSupport()
             .getOrCreate())


    cust_schema = StructType([StructField("custid",IntegerType()),
                              StructField("firstname",StringType()),
                              StructField("lastname",StringType()),
                              StructField("age",IntegerType()),
                              StructField("profession",StringType())])

    df1 = spark.read.csv(file1,sep=",",header=True,schema=cust_schema,mode='permissive')
    #add a column
    df2 = df1.withColumn("src",lit("db"))
    df2.show()
    df3 = df1.select("*",lit("db").alias("src"))
    df3.show()
    #rename a col
    df4 = df1.withColumnRenamed("age","custage")
    df4.printSchema()
    df5 = df1.withColumn("curr_dt",current_date()).withColumn("curr_ts",current_timestamp())
    df5.show()
    df6 = df1.select("*",current_date().alias("curdate"),current_timestamp().alias("curts"))
    df6.show(10,False)

    #concat
    df7 = df1.withColumn("name",concat("firstname","lastname")).drop("firstname","lastname")
    df7.show(10,False)
    df8 = df1.select(concat("firstname",lit(" "),"lastname").alias("Name"),"age","profession")
    df8.show()
    df9 = df8.withColumn("fname",split("Name"," ")[0]).withColumn("lname",split("Name"," ")[1]).drop("Name")
    df9.show()
