movies = load 'movie.txt' using PigStorage(',') as (id:int, title:chararray, year:int, rating:float, views:int);

SPLIT movies into lowrated if rating<3.0f, highrated if rating>3.0f;

grouplow = group lowrated all;
grouphigh = group highrated all;

lowratedcount = foreach grouplow generate COUNT(lowrated.id);
dump lowratedcount;

highratedcount = foreach grouphigh generate COUNT(highrated.id);
dump highratedcount;

groupmovies = group movies all;
avgmovies = foreach groupmovies generate AVG(movies.views);


maxview = foreach grouplow generate MAX(lowrated.title),MAX(lowrated.views);

minview = foreach grouphigh generate MIN(highrated.title),MIN(highrated.views);

groupyear = group lowrated by year ;
groupyear = order groupyear by $0 ASC;

maxyear4 = foreach groupyear generate group as release, COUNT(lowrated.rating) as count, AVG(lowrated.rating);

hgroupyear = group highrated by year ;
hgroupyear = order highrated by $0 desc;

hyear = foreach hgroupyear generate group as release, COUNT(highrated.rating) as count;

store lowrated into 'lowrated' USING JsonStorage();
store highrated into 'highrated' USING JsonStorage();

store maxyear4 into 'maxyear' USING PigStorage(',');
