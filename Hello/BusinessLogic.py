from pyspark import *
from pyspark.sql import *
from lib.utils import *
from lib.myudf import *
from lib.logger import *
from pyspark.sql.types import *
from pyspark.sql.column import *
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
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
    print(age_validation(23))
    agevalidate = udf(age_validation)
    spark.udf.register("agevalidate",agevalidate)

    df2 = df1.withColumn("Age_Category",agevalidate("age"))
    df2.show()

    df3 = df1.groupby("profession").agg(sum(col("age")).alias("Sum_age"),min(col("age")).alias("minAge"),max(col("age")).alias("maxage  "))
    df3.show()