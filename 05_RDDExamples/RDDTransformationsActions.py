from pyspark import *

# Configure Spark
conf = SparkConf().setAppName("RDD Transformations").setMaster("local[*]")
spark = SparkContext(conf=conf)
spark.setLogLevel("ERROR")
# Create RDD
rdd = spark.textFile("test.txt")
# Transformation FlatMap
rdd2 = rdd.flatMap(lambda x: x.split(" "))
# Transformation Map
rdd3 = rdd2.map(lambda x: (x,1))
# Trasformation reduceByKey
rdd4 = rdd3.reduceByKey(lambda a,b: a+b)
# Transformation Map and sortByKey
rdd5 = rdd4.map(lambda x: (x[1],x[0])).sortByKey()
#Print rdd6 result to console using action collect
print(rdd5.collect())
# Action - count
print("Count : "+str(rdd5.count()))
# Transformation Filter
rdd6 = rdd2.filter(lambda x: 'at' in x[1])
print(rdd6.collect())
# Action - first
firstRec = rdd5.first()
print("First Record : "+str(firstRec[0]) + ","+ firstRec[1])
# Action - max
datMax = rdd5.max()
print("Max Record : "+str(datMax[0]) + ","+ datMax[1])
# Action - reduce
totalWordCount = rdd5.reduce(lambda a,b: (a[0]+b[0],a[1]))
print("dataReduce Record : "+str(totalWordCount[0]))
# Action - take
data3 = rdd5.take(3)
for f in data3:
    print("data3 Key:"+ str(f[0]) +", Value:"+f[1])
# Action - collect
data = rdd5.collect()
for f in data:
    print("Key:"+ str(f[0]) +", Value:"+f[1])
# Action save to file
rdd5.saveAsTextFile("/tmp/wordCount")