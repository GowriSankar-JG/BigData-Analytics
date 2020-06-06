/*
Problem Statement 4 - Which route (origin & destination) has seen the maximum diversion?
*/

-- Register piggybank.jar to use CSVExcelStorage
REGISTER '/home/gowri/BigData/Aviation_Data/piggybank.jar';

--Load flight details 
flight_data = load '/home/gowri/BigData/Aviation_Data/DelayedFlights.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');

-- Get the origin, destination and diverted details
flight_details = foreach flight_data generate (chararray) $17 as origin,(chararray)$18 as dest, (int)$24 as diverted;

-- Filter the flight details - origin and destination should not be null and it should be diverted.
flight_details_filter = FILTER flight_details by origin is not null and dest is not null and diverted==1 ;
 
-- group result by origin and destination
group_by_origindest = group flight_details_filter by (origin,dest);
 
-- get the number of diversions
count_diversions = foreach group_by_origindest generate group, COUNT(flight_details_filter.diverted);
 
-- sort results by descending order of diversions
sort_flight_diversions = order count_diversions by $1 DESC;
 
-- fetch top 10 rows from the result
top10_flight_diversions = LIMIT sort_flight_diversions 10;

dump top10_flight_diversions ;
