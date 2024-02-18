from pyspark.sql import *
from lib.utils import *
from lib.logger import *
from pyspark.sql.types import *
from pyspark.sql.column import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    file1 = "/Users/vigneshvelusamy/Vicky/Technical/pyspark/Pyspark-Handson/data1/file1"
    file2 = "/Users/vigneshvelusamy/Vicky/Technical/pyspark/Pyspark-Handson/data1/file2"
    conf = get_spark_app_config()
    spark = (SparkSession \
         .builder \
         .config(conf=conf)
         .getOrCreate())

    df1 = spark.read.csv(file1,sep="~",header=True,inferSchema=True)
    df2 = spark.read.csv(file2,sep="~",header=True,inferSchema=True)
    df1.show()
    df1.printSchema()
    df2.show()
    df2.printSchema()

#   df3 = df1.union(df2) # UNION can only be performed on inputs with the same number of columns,
    df3 = df1.unionByName(df2,allowMissingColumns=True)
    df3.show()
    df3.printSchema()

    df1_updated = df1.withColumn("dept",lit("Dummy"))
    df4 = df1_updated.unionByName(df2,allowMissingColumns=True)
    df4.show()
    print(df4.rdd.getNumPartitions())
    df4.repartition(4)
    df4.write.mode("overwrite").json("/Users/vigneshvelusamy/Vicky/Technical/pyspark/Pyspark-Handson/data3/")




