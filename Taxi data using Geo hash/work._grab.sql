-- Assumptions:
-- I created geohashes using R scripting and loaded some geohash locations into a newly created location table


--problem 1 Calculate the supply demand ratio for all locations given a time window.
-- Based on the given data set:
-- a. supply  = total num of trips that were completed (assumed that as driver supply count)
-- b. demand = total num of trips
WITH supply_data AS (SELECT
                       l.location,
                       tpep_pickup_datetime::date                                    AS pickup_date,
                       extract(hour FROM tpep_pickup_datetime::timestamp)            AS pickup_hour,
                       count(1)                                                                   AS ct
                     FROM trip t
					 join location l on t.pickup_latitude::decimal = l.latitude and t.pickup_longitude::decimal = l.longitude
                     WHERE t.pickup_latitude <> '0'
                     GROUP BY 1, 2, 3),
    demand_data AS (SELECT
                       l.location,
                       tpep_pickup_datetime::date                                    AS pickup_date,
                       extract(hour FROM tpep_pickup_datetime::timestamp)            AS pickup_hour,
                       count(1)                                                                   AS ct
                     FROM trip t
					 join location l on t.pickup_latitude::decimal = l.latitude and t.pickup_longitude::decimal = l.longitude
                    GROUP BY 1, 2, 3)
SELECT
  d.location,
  s.pickup_date,
  s.pickup_hour,
  (s.CT ::decimal / d.ct) as s_d_ratio
FROM demand_data d
  LEFT JOIN supply_data s ON s.location = d.location AND s.pickup_date = d.pickup_date AND s.pickup_hour = d.pickup_hour

------------------------------------------------------------------------------------------------------------------------
-- p2 Which is the second busiest area in terms of demand, given a time window like for a day,
--hour etc.
-- got records for '2015-01-15' and hour = 15 (3 pm)

  with temp_data as (select location, count(1) as trip_count
from saiteja.trip t 
join location l on t.pickup_latitude::decimal = l.latitude and t.pickup_longitude::decimal = l.longitude
where tpep_pickup_datetime::date = '2015-01-15' and  extract(hour from tpep_pickup_datetime::timestamp) = 15
group by 1,2)
select * from temp_Data t1
where 1 = (select count(trip_count) from temp_data t2 where t2.trip_count > t1.trip_count )
------------------------------------------------------------------------------------------------------------------------
-- p3Print all the trips with no-charge payment types, with travel distance more than the average
--trip-distance for a given day.
-- again, used '2015-01-15' as date for a given day

select t.id,p.payment_type,p.fare_amount, p.total_amount
from saiteja.trip t
join payment p on t.id = p.id
where p.total_amount = 0 and t.trip_distance > (select avg(trip_distance) from trip)
and tpep_pickup_datetime::date = '2015-01-15'
------------------------------------------------------------------------------------------------------------------------
-- p4 Which hour in a day people tend to travel more in groups?
select 
 -- tpep_dropoff_datetime::date as dropoff_date,
  extract (hour from tpep_dropoff_datetime::timestamp) as hour,
  count(1) as group_trip_count
from saiteja.trip 
where passenger_count > 1 
and pickup_latitude<> 0 and dropoff_longitude<> 0
group by 1
order by 2 desc
------------------------------------------------------------------------------------------------------------------------
-- p5 You are a sports retailer who wants to open a new premium store. What would be the best
--location for you to open up a store
-- To open a sports retail store, I'd prefer to open in any of the top 5 locations of pick up or dropoff
select location, count(1) as trip_count
from saiteja.trip t 
join location l on t.pickup_latitude::decimal = l.latitude and t.pickup_longitude::decimal = l.longitude
where pickup_latitude <> '0'
group by 1
order by 2 desc
limit 5
------------------------------------------------------------------------------------------------------------------------
-- p6 What is the revenue split across the payment type for each day?
select t.tpep_pickup_datetime::date, p.payment_type, sum(total_amount)
from saiteja.trip t
join saiteja.payment p on p.id = t.id
group by t.tpep_pickup_datetime::date, p.payment_type
------------------------------------------------------------------------------------------------------------------------
-- p7A ride can be considered a suspicious ride if the pickup and dropoff location are in the same
--geo_hash and the trip distance is unusually longer. The company uses suspicious rides to do
--fraud detection. What are the top 5 geohashes that have the maximum suspicious rides.
-- If the trip duration is greater than 60 minutes, I've assumed that it can be marked as suspicious


with 
dropoff_locations as (select l.* from saiteja.trip t 
join location l on t.dropoff_latitude::decimal = l.latitude and t.dropoff_longitude::decimal = l.longitude
)
select l.location, count(1) as trip_count
from saiteja.trip t 
join location l on t.pickup_latitude::decimal = l.latitude and t.pickup_longitude::decimal = l.longitude
join dropoff_locations d on t.dropoff_latitude::decimal = d.latitude and t.dropoff_longitude::decimal = d.longitude
where  l.location = d.location
and datediff( minute, t.tpep_pickup_datetime::timestamp, t.tpep_dropoff_datetime::timestamp) > 60
and t.pickup_latitude <> 0
group by 1
order by 2 desc
limit 5

----------------------------------------------------------------------------------------------------------------------
-- p8 We want to analyze the impact of weather on supply, demand and congestion. For this part of
--the exercise, you may include hourly weather data to your data set.

copy weather
from 's3://grab-interview-redshift/weather-data.txt' 
iam_role 'arn:aws:iam::575564640239:role/redshift-interview';

-- Below would by my approach to show the supply demand ratio on diff weather factors

WITH supply_data AS (SELECT
                       location,
                       tpep_pickup_datetime::date                                    AS pickup_date,
                       extract(hour FROM tpep_pickup_datetime::timestamp)            AS pickup_hour,
                       count(1)                                                                   AS ct
                     FROM trip t 
join location l on t.dropoff_latitude::decimal = l.latitude and t.dropoff_longitude::decimal = l.longitude
                     WHERE pickup_latitude <> '0'
                     GROUP BY 1, 2, 3),
    demand_data AS (SELECT
                       location,
                       tpep_pickup_datetime::date                                    AS pickup_date,
                       extract(hour FROM tpep_pickup_datetime::timestamp)                                    AS pickup_hour,
                      count(1)                                                                   AS ct
                    FROM trip t 
join location l on t.dropoff_latitude::decimal = l.latitude and t.dropoff_longitude::decimal = l.longitude
                    GROUP BY 1, 2, 3)
SELECT
  d.location,
  d.pickup_date,
  d.pickup_hour,
  w.temperature,
  w.precipication,
  w.wind,
  w.humidity,
  (s.CT ::decimal / d.ct) as s_d_ratio
FROM demand_data d
  LEFT JOIN supply_data s ON s.location = d.location AND s.pickup_date = d.pickup_date AND s.pickup_hour = d.pickup_hour
  join saiteja.weather w on d.pickup_date = w.date and w.pickup_hour = w.hour


