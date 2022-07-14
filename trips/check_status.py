import configparser
import sqlalchemy

#Configuration settings and database connection
config = configparser.ConfigParser()
config.read('settings.ini')

hostname = config['Database']['hostname']
database = config['Database']['database']
username = config['Database']['username']
pwd = config['Database']['password']

conn="mysql://"+username+":"+pwd+"@"+hostname+"/"+database
databaseEng = sqlalchemy.create_engine(conn,  echo=False)

#Checking database connection
try:
    with databaseEng.connect() as con:
        con.execute("SELECT 0")
except Exception as e:
    print(f'Unable to connect to the database: {str(e)}')
    sys.exit()
    
    
print ('First, you need to fetch the Data URL ID')

status_type = int(input('Enter 1-Last upload, 2-Last 5 uploads, 3-Last 10 uploads, 4-Fetch all, 5-I have the ID:'))
#Status types: Enter 1-Last upload, 2-Last 5 uploads, 3-Last 10 uploads, 4-Fetch all, 5-I have the ID:'
if status_type==1:
    sql_text = "select a.id, b.etl_status final_status, record_count, a.data_url, a.etl_status processes FROM etl_status_details a join etl_status b on (a.etl_status_id = b.id)  where etl_status_id = (SELECT id FROM etl_status order by id desc limit 0,1);"
elif status_type==2:
    sql_text = "select id, data_url, record_count,etl_status from etl_status order by 1 desc  limit 0,5"
elif status_type ==3:
    sql_text = "select id, data_url, record_count,etl_status from etl_status order by 1 desc limit 0,10"
elif status_type==4:
    sql_text = "select id, data_url, record_count,etl_status from etl_status order by 1 desc"
elif status_type ==5:
    sql_text = 'N'



if (status_type ==1 or status_type==2 or status_type==3 or status_type == 4): 
    status_list = databaseEng.execute(sql_text).fetchall()
    print("The details of the data ingestion/processing")
    for i in status_list:
        print (i)

if (status_type ==5 or status_type==2 or status_type==3 or status_type == 4):
    status_id = int(input('Enter the status ID:'))
    sql_text = f"select a.id, b.etl_status final_status, a.data_url,record_count, a.etl_status processes FROM etl_status_details a join etl_status b on (a.etl_status_id = b.id)  where etl_status_id = {status_id}"
    status_list = databaseEng.execute(sql_text).fetchall()
    
    print("The details of the data ingestion/processing")
    for i in status_list:
        print (i)