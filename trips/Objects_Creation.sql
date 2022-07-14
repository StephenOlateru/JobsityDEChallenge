/* Creates a table trips_raw which will receive the raw data from the csv or any other source */
DROP SCHEMA if exists jobsity ;
CREATE SCHEMA jobsity ;

use jobsity;

/* Creates the table to hold the raw data */
drop table if exists trips_raw ; 
CREATE TABLE trips_raw (
  region VARCHAR(100) NULL,
  origin_coord VARCHAR(100) NULL,
  destination_coord VARCHAR(100) NULL,
  datetime VARCHAR(45) NULL,
  datasource VARCHAR(100) NULL,
  etl_date DATE NULL
  ,status_id integer
);

/* Create tables to monitor ingestion and processing tasks*/
drop table if exists etl_status ; 
CREATE TABLE etl_status (
  id INT NOT NULL AUTO_INCREMENT,
  data_url VARCHAR(1000) NULL,
  etl_date DATETIME NULL,
  etl_status VARCHAR(45) NOT NULL DEFAULT 'Initiated',
  record_count integer default 0,
  start_time TIMESTAMP NOT NULL DEFAULT current_timestamp,
  end_time TIMESTAMP NOT NULL DEFAULT current_timestamp on update current_timestamp,
  PRIMARY KEY (id));
  
drop table if exists etl_status_details ; 
CREATE TABLE etl_status_details (
  id INT NOT NULL AUTO_INCREMENT,
  etl_status_id int,
  data_url VARCHAR(1000) NULL,
  etl_date DATETIME NULL,
  etl_status VARCHAR(45) NOT NULL DEFAULT 'Initiated',
  start_time TIMESTAMP NOT NULL DEFAULT current_timestamp,
  end_time TIMESTAMP NOT NULL DEFAULT current_timestamp on update current_timestamp,
  PRIMARY KEY (id));
  
  /* Create date dimension table*/
  drop table if exists date_dim ; 
  CREATE TABLE date_dim (
  date_id INT NOT NULL AUTO_INCREMENT,
  date DATETIME not NULL,
  day_of_week varchar(10) NULL,
  week_no INT NULL,
  week_desc VARCHAR(45) NULL,
  month_no INT NULL,
  month_desc VARCHAR(45) NULL,
  quarter_no INT NULL,
  quarter_desc VARCHAR(45) NULL,
  year INT NULL,
  date_added TIMESTAMP NOT NULL DEFAULT current_timestamp,
  PRIMARY KEY (date_id),
  UNIQUE INDEX `date_UNIQUE` (`date` ASC) );
  
/* Create region dimension table*/
drop table if exists region_dim ; 
CREATE TABLE region_dim (
  region_id INT NOT NULL AUTO_INCREMENT,
  region_desc varchar(50)  NULL,
  date_added TIMESTAMP NOT NULL DEFAULT current_timestamp,
  PRIMARY KEY (region_id));
  
  Insert into region_dim
    (region_id, region_desc)
    select 0, 'No Region' 
    ;
    
/* Create data source dimension  table*/
drop table if exists datasource_dim ; 
CREATE TABLE datasource_dim (
  source_id INT NOT NULL AUTO_INCREMENT,
  source_desc varchar(50) NULL,
  date_added TIMESTAMP NOT NULL DEFAULT current_timestamp,
  PRIMARY KEY (source_id));
 Insert into datasource_dim
    (source_id, source_desc)
    select 0, 'No Data Source' 
    ;

/* Creates a table trips_fact which is the details fact table in the data warehouse */
 drop table if exists trips_fact ; 
CREATE TABLE trips_fact (
  id INT NOT NULL AUTO_INCREMENT ,
  region varchar(50),
  datasource varchar(50),
  source_data_id int default 0,
  datetime datetime,
  date_id integer default 0,
  trip_date date,
  time_of_day varchar(20),
  origin_coord VARCHAR(45) NULL,
  destination_coord VARCHAR(45) NULL,
  x1 decimal(20,17) null,
  y1 decimal(20,17) null,
  x2 decimal(20,17) null,
  y2 decimal(20,17) null,
  trip_count int default 1,
  etl_date DATE NULL,
  date_refreshed TIMESTAMP NOT NULL DEFAULT current_timestamp,
  PRIMARY KEY (id));
  
  /* Creates an aggregation table from trips_fact in the data warehouse */
  drop table if exists trips_aggr_facts ; 
  CREATE TABLE trips_aggr_facts (
  id INT NOT NULL AUTO_INCREMENT,
  region_id DATETIME NULL,
  origin_id VARCHAR(45) NULL,
  date_id INT NULL,
  week_no INT NULL,
  weekly_avg DECIMAL(20,2) NULL,
  weekly_total INT NULL,
  date_added TIMESTAMP NOT NULL DEFAULT current_timestamp,
  PRIMARY KEY (id));
  
  
DROP PROCEDURE IF EXISTS  load_date_dim;
DELIMITER $$
CREATE  PROCEDURE `load_date_dim`(v_start_date date, v_days_no int)
/*
-- =============================================
-- Author:      Stephen Olateru
-- Create date: 13th July 2022
-- Description: Stores the date details - date, week, month, quarter and year for easier reporting in the data warehouse.
-- Parameters start date and no of days from the start date.
-- =============================================
*/
BEGIN
	declare loop_date date;
    declare counter integer default 1;
    declare qtr integer;
    declare code varchar(50);
    declare msg text;
    
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
    BEGIN
      GET DIAGNOSTICS CONDITION 1
        code = RETURNED_SQLSTATE, msg = MESSAGE_TEXT;
    END;
    
    set loop_date = v_start_date;
    while counter < v_days_no do
		
        set qtr = ceil(date_format(loop_date,'%c')/3);
        
        insert into date_dim
        (date, day_of_week, week_no, week_desc, month_no, month_desc, quarter_no, quarter_desc, year)
        values
        (date_format(loop_date,'%Y-%m-%d'),date_format(loop_date,'%a')
        ,date_format(loop_date,'%V'),date_format(loop_date,'%Y-%V')
        ,date_format(loop_date,'%c'),date_format(loop_date,'%M')
        ,qtr,concat_ws('','Q',qtr)
        ,date_format(loop_date,'%Y')
        
        );
        
        set loop_date = DATE_ADD(loop_date, interval 1 DAY);
        set counter = counter +1;
    
    end while;
END$$
DELIMITER ;

DROP PROCEDURE IF EXISTS  load_dimensions;
DELIMITER $$
CREATE  PROCEDURE `load_dimensions`()
/*
-- =============================================
-- Author:      Stephen Olateru
-- Create date: 13th July 2022
-- Description: Loads new dimensions into region_dim and datasource_dim tables
-- =============================================
*/
BEGIN
-- Region dimension
Insert into region_dim
(region_desc)
select distinct trim(region) reg
from trips_raw a where a.region not in (select b.region_desc from region_dim b)
order by 1;


-- Load data source dimension
Insert into datasource_dim
(source_desc)
select distinct trim(datasource) reg
from trips_raw a where a.datasource not in (select b.source_desc from datasource_dim b)
order by 1;

commit;

END$$
DELIMITER ;

DROP PROCEDURE IF EXISTS  load_facts;
DELIMITER $$
CREATE  PROCEDURE `load_facts`()
/*
-- =============================================
-- Author:      Stephen Olateru
-- Create date: 13th July 2022
-- Description: Loads the enriched data from the raw data
-- =============================================
*/
BEGIN
	
		insert into trips_fact
		(region, datasource,source_data_id, date_id, origin_coord, destination_coord
		,datetime, trip_date,time_of_day
		, x1, y1, x2, y2, trip_count, etl_date)
		 select x.* from 
		 (select  a.region, a.datasource, source_data_id,0 date_id, origin_coord , destination_coord
		 , a.datetime, date_format(a.datetime,'%Y%m%d') trip_date, right(datetime,8) time_of_day
		 ,trim(substring(origin_coord,8,locate(' ',origin_coord,8)-8)) x1, trim(replace(substring(origin_coord,locate(' ',origin_coord,8)+1),')',''))  y1
		  ,trim(substring(destination_coord,8,locate(' ',destination_coord,8)-8)) x2, trim(replace(substring(destination_coord,locate(' ',destination_coord,8)+1),')',''))  y2
		 , 1, date_format(now(),'%Y-%m-%d')
		 from  trips_raw a
         -- left join date_dim d
			-- on (date_format(a.datetime,'%Y%m%d') = date_format(d.date,'%Y%m%d'))
		 ) x
		 order by time_of_day, x1, y1, x2, y2
        --  limit 0,10
         ;
		commit;
      
END$$
DELIMITER ;

DROP TRIGGER IF EXISTS  etl_status_trig;
DELIMITER $$
CREATE TRIGGER etl_status_trig AFTER INSERT ON etl_status FOR EACH ROW 
/*
-- =============================================
-- Author:      Stephen Olateru
-- Create date: 13th July 2022
-- Description: Inserts into etl_status_details for every record stored in etl_status table.
-- =============================================
*/
BEGIN
	insert into etl_status_details
    (data_url, etl_status_id, etl_date, etl_status, start_time, end_time)
    values
    (NEW.data_url,NEW.ID, NEW.etl_date, NEW.etl_status, NEW.start_time, NEW.end_time);

END$$
DELIMITER ;

-- Populating the date dimension from 1st January 2017 to the next 3000 days.
call load_date_dim('2017-01-01', 3000);