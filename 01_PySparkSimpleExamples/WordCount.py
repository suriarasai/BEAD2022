from pyspark import SparkContext, SparkConf
if __name__ == "__main__":
    conf = SparkConf().setAppName("word count").setMaster("local[2]")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    lines = sc.textFile("data/Singapore.txt")
    words = lines.flatMap(lambda line: line.split(" "))
    wordCounts = words.countByValue()
    for word, count in wordCounts.items():
        print("{} : {}".format(word, count))