from pyspark.sql import *
from lib.utils import *
from lib.logger import *
from pyspark.sql.types import *
from pyspark.sql.column import *
from pyspark.sql.window import Window

from pyspark.sql.functions import *
if __name__ == "__main__":
    custpath = "/Users/vigneshvelusamy/Vicky/Technical/pyspark/Pyspark-Handson/data/custs"

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

    custdf = (spark.read.schema(cust_schema).
              csv(custpath,mode="permissive",header=False).
              na.drop("all"))

    print("customer " , custdf.count())
    custdf.repartition(2)
    print("Part  " , custdf.rdd.getNumPartitions()  )

 
    custdf_surr_key = custdf.withColumn("Sequence",monotonically_increasing_id())
    custdf_surr_key.show()

    custdf_surr_key1 = custdf.select(row_number().over(Window.partitionBy("profession").orderBy("age")).alias("sequence"),"*")
    custdf_surr_key1.where("profession in ('Carpenter','Writer')").show(1000)