from pyspark import *
# Python3 code to convert a tuple
# into a string using reduce() function
import functools
import operator
# Configure Spark
conf = SparkConf().setAppName("RDD Basics").setMaster("local[*]")
spark = SparkContext(conf=conf)
spark.setLogLevel("ERROR")
#Create RDD from parallelize and Tuple
def convertTuple(tup):
    str = functools.reduce(operator.add, (tup))
    return str
ab = spark.parallelize([('Suria', 'Lecturer'), ('Venkat', 'Lecturer' ), ('Liu Fan','Lecturer'), ('Eunice', 'Course Administrator'), ('Dinesh', 'Course Administrator'),('Thiri', 'Course Administrator')])
print("The first record is "+convertTuple(ab.first()))
#Create RDD from parallelize
data = [1,2,3,4,5,6,7,8,9,10,11,12]
rdd1=spark.parallelize(data)
print(rdd1.collect())
# Creates empty RDD with no partition
rdd2 = spark.emptyRDD
#Create empty RDD with partition
#This creates 10 partitions
rdd3 = spark.parallelize([],10)
#Outputs: initial partition count
print("initial partition count:"+str(rdd1.getNumPartitions()))

## Simple Set Operations.
baseRdd1 = spark.parallelize(["hello", "hi", "suria", "big", "data", "hub", "hub", "hi"])
print(baseRdd1.collect())
baseRdd2 = spark.parallelize(["aloysis", "arutprakash","clement", "jovi"])
print(baseRdd2.collect())
baseRdd3 = spark.parallelize([1, 2, 3, 4], 2)
print(baseRdd3.collect())
sampledRdd = baseRdd1.sample("false", 0.5)
unionRdd = baseRdd1.union(baseRdd2).repartition(1)
print(unionRdd.collect())
intersectionRdd = baseRdd1.intersection(baseRdd2)
print(intersectionRdd.collect())

