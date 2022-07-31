# Databricks notebook source
# MAGIC %md
# MAGIC <h2>Spark Core Examples</h2>
# MAGIC The main purpose of this notebook is to explain the most common Spark mapper transformations by simple working examples. 
# MAGIC 
# MAGIC <h3>map(f)</h3>
# MAGIC Spark’s map() function is a one-to-one transformation. It transforms each element of the source RDD[V] into one element of the resulting target RDD[T].
# MAGIC <h3>flatMap(f)</h3>
# MAGIC Spark’s flatMap function is a one-to-many transformation. It transforms each element of source RDD[V] to 0 or more elements of target RDD[T].
# MAGIC <h3>groupByKey()</h3>
# MAGIC Group the values for each key in the RDD into a single sequence.
# MAGIC <h3>reduceByKey()</h3>
# MAGIC Merge the values for each key using an associative and commutative reduce function.
# MAGIC <h3>sortByKey()</h3>
# MAGIC Sort the RDD by key, so that each partition contains a sorted range of the elements in ascending order.

# COMMAND ----------

words = sc.parallelize(
   ["scala", 
   "java", 
   "hadoop", 
   "spark", 
   "akka",
   "kafka",
   "spark vs hadoop", 
   "kubernates vs yarn",
   "pyspark",
   "pyspark and spark"]
)

# COMMAND ----------

words.collect()

# COMMAND ----------

wordsflatmap = words.flatMap(lambda line : line.split(" "))
wordsflatmap.collect()
list = wordsflatmap.map(lambda x:len(x))
print("Flat Lines : %s" % (wordsflatmap.take(10)))
print("Flat Lines Length: %s" % (list.take(10)))

# COMMAND ----------

//Local File
// textFile = sc.textFile("file:///home/cloudera/Desktop/word_count.text")
// HDFS File
// textFile = sc.textFile("hdfs://localhost:8020/user/cloudera/word_count.text")

myFile = sc.textFile("/FileStore/tables/PoemsByKeats.txt")
myFile.take(5)


# COMMAND ----------

myFile.first()


# COMMAND ----------

myFile.count()

# COMMAND ----------

keatsflatmap = myFile.flatMap(lambda line : line.split(" "))
keatsflatmap.collect()


# COMMAND ----------

word_deep_filter=keatsflatmap.filter(lambda line : ("thy" in line))
word_deep_filter.collect()
word_deep_filter.count()

# COMMAND ----------

# Union

one = sc.parallelize(range(1,10))
two = sc.parallelize(range(5,15))
one.union(two).collect()


# COMMAND ----------

one.union(two).distinct().collect()

# COMMAND ----------

x = sc.parallelize([("spark", 1), ("hadoop", 4)])
y = sc.parallelize([("spark", 2), ("hadoop", 5)])
joined = x.join(y)
final = joined.collect()
print ("Join RDD -> %s" % (final))

# COMMAND ----------

nums = sc.parallelize([1, 2, 3, 4, 5])
result = nums.reduce(lambda x,y: x+y )
print ("Adding the elements -> %i" % (result))
