class Log4j(object):
    def __init__(self, spark):
        log4j = spark._jvm.org.apache.log4j
        root_class = "guru.learningjournal.spark.examples"
        conf = spark.sparkContext.getConf()
        app_name = conf.get("spark.app.name")
        self.logger = log4j.LogManager.getLogger(app_name)
        self.logger.info("Hi writing log info")
        self.logger.warn("Hi writing log warn")
        self.logger.error("Hi writing log error")
        self.logger.debug("Hi writing log debug")

    def warn(self, message):
        self.logger.warn(message)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def debug(self, message):
        self.logger.debug(message)