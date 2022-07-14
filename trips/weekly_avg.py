from sqlalchemy import create_engine
import pymysql
import pandas as pd
import sys
import configparser

#Configuration settings and database connection
config = configparser.ConfigParser()
config.read('settings.ini')

hostname = config['Database']['hostname']
database = config['Database']['database']
username = config['Database']['username']
pwd = config['Database']['password']


#Set report type: Report type 1 for Region, 2 by Coordinates:
try:
    report_type = int(input("Report type (1 for Region, 2 by Coordinates):"))
    
except:
    print ('Invalid input, kindly enter 1 or 2.')
    sys.exit()

newEngine       = create_engine('mysql+pymysql://'+username+':'+pwd+'@'+hostname+'/'+database)

#Process the request depending on the report_type selected above
if report_type == 1:
    print ('Region')
    region = input("Enter region here:")
    sql_text = f"select region, date_format(datetime,'%%Y-%%V')  Week_No , round(count(0)/7,4) weekly_avg FROM trips_fact a where upper(region) = upper('{region}') group by region, date_format(datetime,'%%Y-%%V')"
    conn    = newEngine.connect()
    data    = pd.read_sql(sql_text, conn);
    
    pd.set_option('display.expand_frame_repr', False)
    print(data)

    conn.close()
else:
    print ('Enter coordinates x1,y1 for first location and x2,y2 for second location.')
    try:
        x1 = float(input("Enter x1:"))
        y1 = float(input("Enter y1:"))
        x2 = float(input("Enter x2:"))
        y2 = float(input("Enter y2:"))
    except:
        print ('Invalid input, kindly enter 1 or 2.')
        sys.exit()
   
    conn    = newEngine.connect()
   
    sql_text = f"select region, date_format(datetime,'%%Y-%%V')  Week_No , round(count(0)/7,4) weekly_avg  FROM trips_FACT a where x1 >= {x1} and y1 <={y1} and x2 >= {x2} and y2 <= {y2} group by region, date_format(datetime,'%%Y-%%V');"
    
    data    = pd.read_sql(sql_text, conn);

    pd.set_option('display.expand_frame_repr', False)
    print(data)

    conn.close()
