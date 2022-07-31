from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from graphframes import *

# Configure Spark
cnfg = SparkConf().setAppName("GraphExamples").setMaster("local[4]")
sc = SparkContext(conf=cnfg)
spark = SparkSession(sc)

# Set File Paths
tripdelaysFilePath = "E://PycharmProjects//pythonProject//data//departuredelays.csv"
airportsnaFilePath = "E://PycharmProjects//pythonProject//data//airport-codes-na.txt"
# Obtain airports dataset
# Note, this dataset is tab-delimited with a header
airportsna = spark.read.csv(airportsnaFilePath, header='true', inferSchema='true', sep='\t')
airportsna.createOrReplaceTempView("airports_na")
# Obtain departure Delays data# Note, this dataset is comma-delimited with a header
departureDelays = spark.read.csv(tripdelaysFilePath, header='true')
departureDelays.createOrReplaceTempView("departureDelays")
departureDelays.cache()
# Available IATA codes from the departuredelays sample datasettrip
tripIATA = spark.sql("select distinct iata from (select distinct origin as iata from departureDelays union all select distinct destination as iata from departureDelays) a")
tripIATA.createOrReplaceTempView("tripIATA")
# Only include airports with atleast one trip from the
# `departureDelays` dataset
airports = spark.sql("select f.IATA, f.City, f.State, f.Country from airports_na f join tripIATA t on t.IATA = f.IATA")
airports.createOrReplaceTempView("airports")
airports.cache()
# Build `departureDelays_geo` DataFrame
# Obtain key attributes such as Date of flight, delays, distance,
# and airport information (Origin, Destination)
departureDelays_geo = spark.sql("select cast(f.date as int) as tripid, cast(concat(concat(concat(concat(concat(concat('2014-', concat(concat(substr(cast(f.date as string), 1, 2), '-')), substr(cast(f.date as string), 3, 2)), ''), substr(cast(f.date as string), 5, 2)), ':'), substr(cast(f.date as string), 7, 2)), ':00') as timestamp) as `localdate`, cast(f.delay as int), cast(f.distance as int), f.origin as src, f.destination as dst, o.city as city_src, d.city as city_dst, o.state as state_src, d.state as state_dst from departuredelays f join airports o on o.iata = f.origin join airports d on d.iata = f.destination")
# Create Temporary View and cache
departureDelays_geo.createOrReplaceTempView("departureDelays_geo")
departureDelays_geo.cache()
# Review the top 10 rows of the `departureDelays_geo` DataFrame
departureDelays_geo.show(10)

# Create Vertices (airports) and Edges (flights)
tripVertices = airports.withColumnRenamed("IATA", "id").distinct()
tripEdges = departureDelays_geo.select("tripid", "delay", "src", "dst", "city_dst", "state_dst")
# Cache Vertices and Edges
tripEdges.cache()
tripVertices.cache()
tripGraph = GraphFrame(tripVertices, tripEdges)
print("Airports: %d" % tripGraph.vertices.count())
print("Trips: %d" % tripGraph.edges.count())
tripGraph.edges.groupBy().max("delay").show(1)
print("On-time / Early Flights: %d" % tripGraph.edges.filter("delay <= 0").count())
print("Delayed Flights: %d" % tripGraph.edges.filter("delay > 0").count())
tripGraph.edges.filter("src = 'SEA' and delay > 100").show(10)
# Calculate the inDeg (flights into the airport) and
# outDeg (flights leaving the airport)
inDeg = tripGraph.inDegrees
outDeg = tripGraph.outDegrees
# Calculate the degreeRatio (inDeg/outDeg)
degreeRatio = inDeg.join(outDeg, inDeg.id == outDeg.id).drop(outDeg.id).selectExpr("id", "double(inDegree)/double(outDegree) as degreeRatio").cache()
# Join back to the 'airports' DataFrame
# (instead of registering temp table as above)
transferAirports = degreeRatio.join(airports, degreeRatio.id == airports.IATA).selectExpr("id", "city", "degreeRatio").filter("degreeRatio between 0.9 and 1.1")
# List out the top 10 transfer city airports
transferAirports.orderBy("degreeRatio").limit(10).show()

import pyspark.sql.functions as func
topTrips = tripGraph.edges.groupBy("src", "dst").agg(func.count("delay").alias("trips"))
# Show the top 20 most popular flights (single city hops)
topTrips.orderBy(topTrips.trips.desc()).limit(20).show()

# Obtain list of direct flights between SEA and SFO
filteredPaths = tripGraph.bfs(fromExpr = "id = 'SEA'",  toExpr = "id = 'SFO'",  maxPathLength = 1)
filteredPaths.show(10)