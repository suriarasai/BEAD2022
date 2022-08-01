from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Map and FlatMap Examples').getOrCreate()

data = ["Project Gutenberg’s",
        "Alice’s Adventures in Wonderland",
        "Project Gutenberg’s",
        "Adventures in Wonderland",
        "Project Gutenberg’s"]
rdd=spark.sparkContext.parallelize(data)
for element in rdd.collect():
    print(element)
#Map
print("Map")
rdd2=rdd.map(lambda x: (x,1))
for element in rdd2.collect():
    print(element)
#Flatmap
print("Flat Map")
rdd3=rdd.flatMap(lambda x: x.split(" "))
for element in rdd3.collect():
    print(element)