-- Lastest datasource from the two most commonly appearing regions.
select datasource
from trips_fact a
join (
 SELECT region, count(0) cnt  FROM trips_fact
 group by region
 order by 2 desc
 limit 0,2) b on (a.region = b.region)
 order by datetime desc
 limit 0,1;

-- Regions with "cheap_mobile" as datasource
select distinct region from  trips_fact 
where datasource = 'cheap_mobile';