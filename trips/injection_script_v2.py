import time
from dask import dataframe as dd
import configparser
import pandas as pd
from datetime import date
import sys
import sqlalchemy
import mysql.connector
from mysql.connector import Error

config = configparser.ConfigParser()

#Prompt for the input file URL
print ('Enter the source data URL:')
file_url = input()

#Get configuration settings from settings.ini
config.read('settings.ini')
hostname = config['Database']['hostname']
database = config['Database']['database']
username = config['Database']['username']
pwd = config['Database']['password']

#Database connection
conn="mysql://"+username+":"+pwd+"@"+hostname+"/"+database
databaseEng = sqlalchemy.create_engine(conn,  echo=False)

#Checking database connection
try:
    with databaseEng.connect() as con:
        con.execute("SELECT 0")
except Exception as e:
    print(f'Unable to connect to the database: {str(e)}')
    sys.exit()


#We are keeping track of the ETL status by inserting a record for the URL
etl_status_df = pd.DataFrame({'data_url': [file_url], 'etl_date': [date.today() ]})
etl_status_df.to_sql(con=databaseEng, name="etl_status", if_exists="append", index=False)

#Get the last Id inserted
last_data = databaseEng.execute("select id from etl_status order by 1 desc limit 0,1").fetchall()
last_id = int(list(last_data[0])[0])

start = time.time()

#Read data into dataframe.
try:
    print ('Data loading starts')
    dask_df = dd.read_csv(file_url,blocksize=25e6)
    end = time.time()
    print("Data read in : ", round((end-start),4), "seconds")
except FileNotFoundError:
    databaseEng.execute(f"update etl_status set etl_status = 'Failed' where id = {last_id}")
    print(f'Specified file does not exist')
    sys.exit()


start = time.time()

#Store the data frame into the raw data table
dask_df['source_data_id']=last_id
dask_df.to_sql(name="trips_raw", uri=conn, if_exists="replace", chunksize=1000, parallel=True)
end = time.time()

#Get total number of rows stored into the database
r_count_data = databaseEng.execute("select count(0) cnt from trips_raw").fetchall()
r_count = int(list(r_count_data[0])[0])
    
print ('Trip data loaded ',r_count, 'records to the database successfully in ',round((end-start),4), "seconds")

#Loading completion
etl_status_df = pd.DataFrame({'data_url': [file_url], 'etl_date': [date.today() ], 'etl_status':['Data Loaded'],'etl_status_id':[last_id]})
etl_status_df.to_sql(con=databaseEng,  name="etl_status_details", if_exists="append", index=False, chunksize=1000)


#process data
try:
    conn = mysql.connector.connect(host=hostname, database=database, user=username, password=pwd)
    
    cur = conn.cursor()
    
    #Process facts
    start = time.time()
    cur.callproc('load_facts')
    
    etl_status_df = pd.DataFrame({'data_url': [file_url], 'etl_date': [date.today() ], 'etl_status':['Facts processed'],'etl_status_id':[last_id]})
    etl_status_df.to_sql(con=databaseEng,  name="etl_status_details", if_exists="append", index=False, chunksize=1000)
    end = time.time()
    
    databaseEng.execute(f"update etl_status set etl_status = 'Completed', record_count = {r_count} where id = {last_id}")
    print ("Facts data loaded successfully in ", round((end-start),4), "seconds");
    
except Error as e:
    print ("There is an error connecting to the database", e);
    