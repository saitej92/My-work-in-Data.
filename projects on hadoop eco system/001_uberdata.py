#from pyspark import SparkConf, SparkContext
#conf = SparkConf().setAppName('MyApplication')
#conf = conf.setMaster("local[*]")
#sc   = SparkContext(conf=conf)


from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
#from numpy import array
#from math import sqrt

#For HDFS Location, assuming the file is FlumeData.1495875830774
#uber_data = sc.textFile('hdfs://localhost:8020/tmp/App2/FlumeData.1495875830774')

#For Local directory
uber_data = sc.textFile('/user/ashwinp/uber/uber-raw-data-sep14.csv')
uber_data.first()

#To remove the header column
uber_data_woH = uber_data.zipWithIndex().filter(lambda (row,index): index > 0).keys()
uber_data_woH.take(5)

#firstRow= uber_data.take(1)
#uber_data_woH1 = uber_data_woH.filter(lambda x: x != firstRow[0])
#uber_data_woH1.take(5)


#To get each row as a separate entity
uber_fea_data = uber_data_woH.map(lambda line : line.split(','))

uber_fea_data.first()

uber_fea_data.take(2)

type(uber_fea_data)


def convertDataFloat(line):
    return array([float(line[1]),float(line[2])])


parsedUberData = uber_fea_data.map(lambda line : convertDataFloat(line)) 


parsedUberData.first()


parsedUberData.count()


#uber_fea_data1 = uber_data_woH1.map(lambda line : line.split(','))
#uber_fea_data1.first()
#uber_fea_data1.take(2)
#type(uber_fea_data1)
#parsedUberData1 = uber_fea_data1.map(lambda line : convertDataFloat(line)) 
#parsedUberData1.first()
#parsedUberData1.count()

clusters = KMeans.train(parsedUberData,8,initializationMode="random")

clusters.centers

#from numpy import array
#from math import sqrt
def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))


WSSSE = parsedUberData.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Within Set Sum of Squared Error = " + str(WSSSE))


#To save the model in HDFS location
clusters.save(sc, '/user/1368B30/Uber/Model')


