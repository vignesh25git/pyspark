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
    df1.where("profession is null").show()
    print(df1.na.drop("any").count())
    print(df1.na.drop("all").count())
    print(df1.na.drop("any",subset=["custid","age"]).count())
    print(df1.na.drop("any",thresh=4).count())
    dict={"Therapist":"Pysician","Not Available":"NA"}
    df1.na.fill("Not Available","profession").where("profession = 'Not Available'").na.replace(dict,subset="profession").show()
#   df2=df1.na.drop("any",thresh=2)
 #   df2.where("custid == 4000001").show()
 #   print(len(df2.collect()))
    df1.createOrReplaceTempView("cust")
    df2 = spark.sql("""
    select custid,firstname,lastname,age,
    case when profession='Therapist' then 'Pysician'
         when profession='NA' then 'Not Available'
         else profession end as profession
         from 
         (select custid,firstname,lastname,age,coalesce('profession','NA') profession
         from cust) temp
    """)
    df2.where("profession = 'Not Available'").show()
