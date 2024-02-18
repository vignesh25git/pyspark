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
    df1.where("custid == 4000001").show()
    print(df1.count())
    print(len(df1.collect()))
    df2=df1.dropDuplicates(subset=["custid"])
    print(len(df2.collect()))
    df1.printSchema()
    spark.sql("SELECT * FROM range(10) where id > 7").show()
# sql method
    df1.createOrReplaceTempView("cust")
    spark.sql("select distinct custid from cust order by custid limit 5").show()
    spark.sql(
    """
    select custid,firstname,lastname,age,profession from 
    ( select row_number() over(partition by custid order by age desc) as rownum, 
             custid,
             firstname,
             lastname,
             age,
             profession 
        from cust) tmp
        where rownum=1   
    """).show(20,False)
    df1.explain()
