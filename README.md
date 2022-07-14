# JobsityDEChallenge
This is Jobsity Data Engineering Challenge
#### Assumptions
Below are some of the assumptions for this demostration

  1.	The data format remains the same for the various data points
  2. 	The data is data cleaning and thus there is no need for data cleaning processes (for this demonstration purpose).
  3.	Each URL represents a unique set of data.
  4.	A file is loaded per time i.e. one file is processed at a time.
  5.	The file type will always be a CSV but could be from different sources. 


#### Requirements
  1. MySQL database
  2. Python 3.8 or higher version
  3. Python libraries
      - pandas (pip install pandas)
      - configparser (pip install configparser)
      - sqlalchemy (pip install sqlalchemy)
      - mysql (pip install mysql)
      - dask (pip install dask)


#### Configuration and Setup
Once the database and the python environments are ready, do the following:
1. Download the trips directory into the C drive (or any other drive as the root of the demo)
2. Open the configuration file (settings.ini) and input the database connection details in the [Database] section. See examples below
      - hostname=localhost 
      - database=jobsity
      - username=root
      - password=ppppp

#### Steps to execute data ingestion and data processing
  1. From the command promt, change the directory to C:\trips (or the demo root where the trips folder was saved)
  2. Execute the data ingestion and processing script (injection_script_v2.py), by typing: python injection_script_v2.py
  3. You will be prompted for the URL to the CSV data. Paste the URL and click enter.
  4. If the settings are okay and the file is a valid CSV, the data will be ingested and processed.
  

#### Weekly Average Reports
To view the weekly average trip reports, follow the steps below:
1. From the command promt, change the directory to C:\trips (or the demo root where the trips folder was saved)
2. Execute the data weekly average script (weekly_avg.py), by typing: python weekly_avg.py
3. You will be prompted for the report type; type 1 to view the report by region or 2 to view the report by coordinates.
4. If 1 is typed, you will be prompted for the name of the region.
5. If 2 is typed, you will be prompted for the coordinates one after the other - x1, y1, x2, y2
6. The report will be displayed.

#### Data Ingestion Status
To view the status of previously ingested data, follow the steps below:
  1. From the command promt, change the directory to C:\trips (or the demo root where the trips folder was saved)
  2. Execute the check status script (check_status.py), by typing: python check_status.py
  3. There are 5 options to choose from:
      -  1 for last ingested data upload
      -  2 for the last 5 uploads in which the ID of one will be selected to fetch the status
      -  3 for the last 10 uploads in which the ID of one will be selected to fetch the status
      -  4 for all uploads in which the ID of one will be selected to fetch the status
      -  5 option is selected if you know the ID of the etl you are interested in.
  4. If 1 option is selected, you will get the details of the last data ingested status
  5. If 2,3, or 4 option is selected, you will be prompted for the ID of interest to get the data ingested status
  6. If 5 option is selected, you 
      
