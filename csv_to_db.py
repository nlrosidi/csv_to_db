
import os
import numpy as np
import pandas as pd
import psycopg2


data_path = os.getcwd() #add path to files
schema = #add schema for database if applicable
host = #add endpoint
dbname = #add name of database
user = #add username
pwd = #add password

#list files in directory
files = os.listdir(data_path)
    
#remove non-csv files
while True:
    try:
        files.remove('.DS_Store')
    except:
        break

#loop through the files and create the dataframe
df = {}
for file in files:
    try:
        df[file] = pd.read_csv(data_path+file)
    except UnicodeDecodeError:
        df[file] = pd.read_csv(data_path+file, encoding="ISO-8859-1") #if utf-8 encoding error
    print(file)
        
#grab names of csv files
df_key = []
for key, value in df.items():
    df_key.append(key)

print('loaded datasets to memory')

#loop through all files and upload to db    
for k in df_key:
    dataframe = df[k] #call dataframe

    #force column names to be lower case, no spaces, no dashes
    dataframe.columns = [x.lower().replace(" ", "").replace("-","_").replace(r"/","_").replace("\\","_") for x in dataframe.columns]

    #save to csv
    dataframe.to_csv(k, header = dataframe.columns, index=False, encoding='utf-8') 

    #processing data
    replacements = {
        'timedelta64[ns]': 'varchar',
        'object': 'varchar',
        'float64': 'float',
        'int64': 'int',
        'datetime64': 'timestamp'
    }

    col_str = ", ".join("{} {}".format(n, d) for (n, d) in zip(dataframe.columns, dataframe.dtypes.replace(replacements)))
    tbl_name = '{0}.{1}'.format(schema, k.split('.')[0]) #save table name and schema

    #open database connection
    conn_string = "host={0} dbname={1} user={2} password={3}".format(host, dbname, user, pwd)
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    print('opened database successfully')
        
    #execute on db
    cursor.execute("drop table if exists {0};".format(tbl_name))
    cursor.execute("create table %s (%s)" % (tbl_name, col_str))
    print('{0} created successfully'.format(tbl_name))

    #open the file, save as an object, and upload to db
    my_file = open(k) 
    print('file opened in memory')
        
    SQL_STATEMENT = """
    COPY %s FROM STDIN WITH
        CSV
        HEADER
        DELIMITER AS ','
    """
    cursor.copy_expert(sql=SQL_STATEMENT % tbl_name, file=my_file)
    print('file copied to db')
        
    cursor.execute("grant select on table {0} to public;".format(tbl_name))
    conn.commit()

    print('table {0} imported to db'.format(tbl_name))
    cursor.close()
        
print('all files imported to db')

