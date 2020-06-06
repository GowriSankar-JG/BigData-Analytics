--Creating a relation 'ratings' 
--Loading the Ratings data from your HDFS
ratings = LOAD 'u.data' AS (userID:int, movieID:int, rating:int, ratingTime:int);

--Creating a relation 'metadata'
--Loading the Movies data from the HDFS. It is '|' (pipe) separated
metadata = LOAD 'u.item' USING PigStorage('|')
	AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoRelease:chararray, imdbLink:chararray);

--Sort them by Release Date
--Use FOREACH GENERATE to generate a new relation from an existing relation 
--Goes through each row and generates a new relation with the following columns
nameLookup = FOREACH metadata GENERATE movieID, movieTitle;

--Grouping them by their MovieID (The Reduce Operation)
groupedRatings = GROUP ratings BY movieID;

--Get the Average Rating for Each Movie & the number of ratings
avgRatings = FOREACH groupedRatings GENERATE group AS movieID, 
	AVG(ratings.rating) AS avgRating, COUNT(ratings.rating) AS numRatings;

--Filtering movies with Average rating < 2 
badMovies = FILTER avgRatings BY avgRating < 2.0;

--Joining badMovies & nameLookup relations using movieID
namedBadMovies = JOIN badMovies By movieID, nameLookup BY movieID;

--Clean up the results with only the required columns
finalResults = FOREACH namedBadMovies GENERATE nameLookup::movieTitle AS movieName,
	badMovies::avgRating AS avgRating, badMovies::numRatings AS numRatings;
    
--Order them by the number of ratings
finalResultsSorted = ORDER finalResults BY numRatings DESC;

DUMP finalResultsSorted;
store finalResultsSorted into 'finalResultsSorted';
