## import required packages
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
from pyspark import SparkConf, SparkContext
from collections import Counter

## spark context is required, if executed as a script
#conf = SparkConf().setAppName('MyApplication')
#conf = conf.setMaster("local[*]")
#sc   = SparkContext(conf=conf)

## Create RDD for test data
uber_test_data = sc.textFile('/user/1368B30/Uber/TestData.csv')

## Split the fields as a comma separator
uber_fea_test_data = uber_test_data.map(lambda line : line.split(','))

## Generate features - Longitude and Latitude - Cast as float
def convertDataFloat(line):
    return array([float(line[1]),float(line[2])])

## Apply the above function to each line
parsedTestData = uber_fea_test_data.map(lambda line : convertDataFloat(line)) 

## Load the saved model
sameModel = KMeansModel.load(sc, "/user/1368B30/Uber/Model")

## Verify Cluster Centers
sameModel.centers

## Predict clusters for the parsed streaming data
outputRdd = sameModel.predict(parsedTestData)

## Add an unique index to the each data point to the base RDD and Result RDD - Required for joins
uber_fea_test_data_1 = uber_fea_test_data.zipWithIndex()
outputRdd_1 = outputRdd.zipWithIndex()

## Find the no. of data points in each cluster 
predictionsValue = outputRdd.collect()
valsCount = Counter(predictionsValue) ## Returns counter type from collections


## Verify the base RDD and result RDD
uber_fea_test_data_1.take(2)
outputRdd_1.take(2)

## Flip Key and Values - Key - Index , Value - Data point and cluster numbers,(to join the data with the cluster numbers)
uber_fea_test_data_2 = uber_fea_test_data_1.map(lambda x : (x[1],(x[0][0],x[0][1],x[0][2],x[0][3])))
outputRdd_2 = outputRdd_1.map(lambda x : (x[1],x[0]))

## Join the RDDs based on index
finalWriteRdd = uber_fea_test_data_2.join(outputRdd_2)
finalWriteRdd = finalWriteRdd.map(lambda x : x[1]) 

## Extract only hour from the date and time
def covnertTSToHour(value):
    if value and not value.isspace():
        #value = "2/28/2017 0:00:00"
        time = value.split(" ")[1].split(":")
        return int(time[0])
        
## Apply the above function        
finalWriteRdd = finalWriteRdd.map(lambda y : (covnertTSToHour(y[0][0]),y[0][1],y[0][2],y[0][3],y[1]))

## Function to add comma as separator
def toCSVLine(data):
    return ','.join(str(d) for d in data)
	
lines = finalWriteRdd.map(toCSVLine)

## Write the results to local file system
lines.coalesce(1).saveAsTextFile("/user/1368B30/Uber/Output")
