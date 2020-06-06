/*
Problem Statement 2 - Which month has seen the most number of cancellations due to bad weather?
*/
-- Register piggybank.jar to use CSVExcelStorage
REGISTER '/home/gowri/BigData/Aviation_Data/piggybank.jar';

--Load flight details 
flight_data = load '/home/gowri/BigData/Aviation_Data/DelayedFlights.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');

-- Get the month, cancelled and cancellation code 
flight_details = foreach flight_data generate (chararray) $2 as month,(int) $22 as cancelled, (chararray)$23 as reason;
 
-- Filter the flight details - get only cancelled flights due to bad weather
cancelled_flights_badweather = filter flight_details by cancelled==1 AND reason=='B';

-- group results by month
group_cancelled_flights_by_month = group cancelled_flights_badweather by month;
 
-- count bad weather cancellations by month
count_cancelled_flights_by_month = foreach group_cancelled_flights_by_month generate group, COUNT(cancelled_flights_badweather.reason);
 
-- sort results by count of cancellations in descending order
sort_cancellation_by_month = order count_cancelled_flights_by_month by $1 DESC;
 
-- fetch top row from the result
top_cancellation_month = LIMIT sort_cancellation_by_month 1;

dump top_cancellation_month ;
