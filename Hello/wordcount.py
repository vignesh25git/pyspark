from pyspark.sql import *
from lib.utils import *
from lib.logger import *
from pyspark.sql.types import *
from pyspark.sql.column import *

from pyspark.sql.functions import *
if __name__ == "__main__":
    file1 = "/Users/vigneshvelusamy/Vicky/Technical/pyspark/Pyspark-Handson/data/wordcountproblem"

    print("Hello spark ")
    conf = get_spark_app_config()
    spark = (SparkSession \
             .builder \
             .config(conf=conf)
             .getOrCreate())
#    input("Enter")
    sc = spark.sparkContext
    rdd = sc.textFile(file1)                    #creating rdd from the text file
    rdd1 = rdd.map(lambda x:x.split(" "))       #split into words
    rdd2 = rdd1.flatMap(lambda x:x)             #flatten your list
    rdd3 = rdd2.map(lambda x:(x,1))             #adding 1 to each element
    rdd4 = rdd3.reduceByKey(lambda x,y:x+y)     #grouping and summing
    print(rdd4.take(10))
    input("Done. Hit Enter")