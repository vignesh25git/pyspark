from pyspark.sql import *
from lib.utils import *
from lib.prop import *
from lib.logger import *
from pyspark.sql.types import *
from pyspark.sql.column import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    file1 = "/Users/vigneshvelusamy/Vicky/Technical/pyspark/Pyspark-Handson/data1/file1"
    file2 = "/Users/vigneshvelusamy/Vicky/Technical/pyspark/Pyspark-Handson/data1/file2"
    print("hi")
    d = readPropertyFile("/Users/vigneshvelusamy/Vicky/Technical/pyspark/Pyspark-Handson/conn.prop")
    print("heyy")
    jdbc = d.get("jdbc")
    driver = d.get("driver")
    conf = get_spark_app_config()
    print("hello")
    spark = (SparkSession \
             .builder \
             .config(conf=conf)
             .getOrCreate())

    df1 = spark.read.jdbc(url=jdbc,table="employees",properties ={'driver':driver})
    #df1.show(10)
    #df1.printSchema()
    print(df1.count())
    df1.select("first_name").where("emp_no == 10001").show(10)
    df1.select(col("first_name").alias("Emp name")).where("gender == 'M'").show(10)
    df1.groupby("gender").agg(count(col("emp_no"))).show()
    df1.groupby("gender").agg(min(col("emp_no"))).show()
    df1.groupby("gender").agg(min(col("birth_date")),max(col("birth_date"))).show()
