from configparser import *


def readPropertyFile(propfilepath):
    dic = {}
    con = ConfigParser()
    con.read(propfilepath)
    jdbc = con.get("JDBC",'url')
    dic["jdbc"] = jdbc
    driver = con.get("JDBC",'driver')
    dic["driver"] = driver
    print("jdbc", dic.get("jdbc"))
    print("driver", dic.get("driver"))
    return dic



