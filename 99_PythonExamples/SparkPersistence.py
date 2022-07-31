from pyspark import *
def isNotHeader(l:str):
    boolean = not (l[0:3] == "host" and l.find("bytes")>0)
# Configure Spark
conf = SparkConf().setAppName("Create RDD")
conf = conf.setMaster("local[*]")
spark = SparkContext(conf=conf)
spark.setLogLevel("ERROR")
# Logs
julyFirstLogs = spark.textFile("E://PycharmProjects//pythonProject//data//nasa_19950701.tsv")
augustFirstLogs = spark.textFile("E://PycharmProjects//pythonProject//data//nasa_19950801.tsv")
# Union Example
aggregatedLogLines = julyFirstLogs.union(augustFirstLogs)
cleanLogLines = aggregatedLogLines.filter(lambda line: isNotHeader(line))
cleanLogLines.saveAsTextFile("out/nasa_logs_all_hosts.csv")
#Statistics Sample
sample = aggregatedLogLines.sample(withReplacement = "true", fraction = 0.1)
sample.saveAsTextFile("out/sample_nasa_logs.csv")
#Intersection
intersectionLogLines = julyFirstLogs.intersection(augustFirstLogs)
cleanedHostIntersection = intersectionLogLines.filter(lambda line: isNotHeader(line))
cleanedHostIntersection.saveAsTextFile("out/nasa_logs_same_hosts.csv")

