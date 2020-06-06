/*
Problem Statement 3 - Top ten origins with the highest AVG departure delay
*/

-- Register piggybank.jar to use CSVExcelStorage
REGISTER '/home/gowri/BigData/Aviation_Data/piggybank.jar';

--Load flight details 
flight_data = load '/home/gowri/BigData/Aviation_Data/DelayedFlights.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');

-- Get the month, cancelled and cancellation code 
flight_details = foreach flight_data generate (int) $16 as delay,(chararray) $17 as origin;

-- Filter the flight details - delay time should not be null and origin should not be null
flight_details_filter = FILTER flight_details by origin is not null and delay is not null ;
 
-- group result by origin
group_by_origin = group flight_details_filter by origin;
 
-- get the average of delays by origin
sum_delay = foreach group_by_origin generate group, AVG(flight_details_filter.delay);
 
-- sort results by descending order of delay
sort_flight_delays = order sum_delay by $1 DESC;
 
-- fetch top 10 rows from the result
top10_flight_delays = LIMIT sort_flight_delays 10;


-- load Airport details 
airport_data = load '/home/gowri/BigData/Aviation_Data/airports.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');

-- get destination, city and country 
airport_country_city = foreach airport_data generate (chararray)$0 as origin, (chararray)$2 as city, (chararray)$4 as country;

-- join flight data and airport data to get destination's country and city
dest_country_city = join top10_flight_delays by $0, airport_country_city by origin;

-- sort final result by count of destinations
final_result = order dest_country_city by $1 DESC ;
 
-- display the results
dump final_result;
