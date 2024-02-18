import configparser

from pyspark import SparkConf

def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("/Users/vigneshvelusamy/Vicky/Technical/pyspark/PySpark-Handson/spark.conf")

    for (key,val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key,val)
    spark_conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
    spark_conf.set("spark.jars","/Users/vigneshvelusamy/Downloads/code/mysql-connector-java.jar")
    return spark_conf


