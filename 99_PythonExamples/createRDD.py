from pyspark import *
# Configure Spark
conf = SparkConf().setAppName("Create RDD")
conf = conf.setMaster("local[*]")
spark  = SparkContext(conf=conf)
spark.setLogLevel("INFO")
# in Python from a local collection
myCollection = "Spark RDD is for any text, structured or unstructured - Big Data Processing Made Simple".split(" ")
words = spark.parallelize(myCollection, 2)
print(words.collect())
# in Python from a File or a directory
myFileLineCount = spark.textFile("E://PycharmProjects//pythonProject//textdata//PoemsByKeats.txt").count()
print(myFileLineCount)
myFileWordCount = spark.textFile("E://PycharmProjects//pythonProject//textdata//PoemsByKeats.txt").flatMap(lambda line: line.split(" ")).count()
print(myFileWordCount)
myFileCount = spark.wholeTextFiles("E://PycharmProjects//pythonProject//textdata").count()
print(myFileCount)
