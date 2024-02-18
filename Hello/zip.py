from pyspark.sql import *
from lib.utils import *
from lib.logger import *
from pyspark.sql.types import *
from pyspark.sql.column import *

from pyspark.sql.functions import *
if __name__ == "__main__":
    file1 = "/Users/vigneshvelusamy/Vicky/Technical/pyspark/Pyspark-Handson/data/word"
    file2 = "/Users/vigneshvelusamy/Vicky/Technical/pyspark/Pyspark-Handson/data/word1"

    print("Hello spark ")
    conf = get_spark_app_config()
    spark = (SparkSession \
             .builder \
             .config(conf=conf)
             .getOrCreate())
    #    input("Enter")
    sc = spark.sparkContext
    rdd = sc.textFile(file1)                    #creating rdd from the text file
    frdd = sc.textFile(file2)                    #creating rdd from the text file

    rdd1 = rdd.zip(frdd)
    print(rdd1.take(5))

