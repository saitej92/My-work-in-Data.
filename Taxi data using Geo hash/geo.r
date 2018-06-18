####Libraries used to run postgressql on R environment and sql df to run sql queries.
library (RPostgreSQL)
library(sqldf)
##Establish a connection with driver(JDBC ) and with the host connection DB and also communication between sqldf and postgresql)
drv <- dbDriver("PostgreSQL")
con1 <- dbConnect(drv, host="grab-redshift.cj8er6fx0hi4.us-east-2.redshift.amazonaws.com",
                  port="5439",dbname="grab",user="saiteja",password='Jdusjda1273%^&*943')
options(sqldf.RPostgreSQL.user ="saiteja",sqldf.RPostgreSQL.password ="Jdusjda1273%^&*943",sqldf.RPostgreSQL.dbname ="grab",sqldf.RPostgreSQL.host ="grab-redshift.cj8er6fx0hi4.us-east-2.redshift.amazonaws.com",sqldf.RPostgreSQL.port =5439)

df = sqldf("select  pickup_latitude as latitude, pickup_longitude as longitude from saiteja.trip 
           union   select dropoff_latitude as latitude, dropoff_longitude as longitude from saiteja.trip limit 100000",drv='PostgreSQL')
###library to encode geohash from latitude and longitude coordinates
library(geohash)
#convert varchar to double for function compatibility.
df$latitude<- as.double(as.character(df$latitude))
df$longitude<- as.double(as.character(df$longitude))

#apply gh_encode function to all coordinates in the data set.
df$location <- apply(df[,c('latitude', "longitude")], 1, function(x) {
  gh_encode(x[1], x[2])})
##importing the data to a csv file for easier import of data to the DB
write.csv(df, file='C:\\Users\\saite\\Downloads\\geo.csv')
