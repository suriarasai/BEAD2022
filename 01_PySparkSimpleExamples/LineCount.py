from pyspark import SparkContext, SparkConf
from pyspark import SparkContext
from pyspark import SparkConf
## Shared variables and data
APP_NAME = "My Spark Application"
## Closure functions
## Main functionality
def main(sc):
    """
    Describe RDD transformations and actions here.
    """
    numberoflines = sc.textFile("data/dummy.txt").count()
    print("The total number of lines is " + str(numberoflines))
    pass
if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
# Execute main functionality
main(sc)
