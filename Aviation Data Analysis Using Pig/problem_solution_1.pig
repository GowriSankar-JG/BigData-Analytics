/*
	Problem Statement 1 - Find out the top 5 most visited destinations.
*/
-- Register piggybank.jar to use CSVExcelStorage
REGISTER '/home/gowri/BigData/Aviation_Data/piggybank.jar';

--Load flight details 
flight_data = load '/home/gowri/BigData/Aviation_Data/DelayedFlights.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');

-- Get the destinations 
flight_dest_details = foreach flight_data generate (chararray) $18 as dest;
 
-- Filter the destinations - get only not null destinations
dest_data = filter flight_dest_details by dest is not null;

-- group result by destination 
dest_group = group dest_data by dest;
 
-- count destinations for every group
dest_count = foreach dest_group generate group, COUNT(dest_data.dest);
 
-- sort results by count of destinations in descending order
top_dest = order dest_count by $1 DESC;
 
-- fetch top 5 rows from the result
top5_dest = LIMIT top_dest 5;

-- load Airport details 
airport_data = load '/home/gowri/BigData/Aviation_Data/airports.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');

-- get destination, city and country 
airport_country_city = foreach airport_data generate (chararray)$0 as dest, (chararray)$2 as city, (chararray)$4 as country;

-- join flight data and airport data to get destination's country and city
dest_country_city = join top5_dest by $0, airport_country_city by dest;

-- sort final result by count of destinations
final_result = order dest_country_city by $1 DESC ;
 
-- display the results
dump final_result;
