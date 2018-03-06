import os
import numpy as np
import pandas as pd
import psycopg2


def process_file(conn, tbl_name, file_object):
    SQL_STATEMENT = """
        COPY %s FROM STDIN WITH
            CSV
            HEADER
            DELIMITER AS ','
        """
    cursor = conn.cursor()
    cursor.copy_expert(sql=SQL_STATEMENT % tbl_name, file=file_object)
    conn.commit()
    
    cursor.execute("grant select on table {0} to public;".format(tbl_name))
    conn.commit()
    
    print('table {0} imported to db'.format(tbl_name))
    cursor.close()


def import_csv_to_db(data_path, schema, host, dbname, user, pwd)
    
    #list files in directory
    files = os.listdir(data_path)
    
    #remove non-csv files
    try:
        files.remove('.DS_Store')
    except OSError:
        pass

    #loop through the files and create the dataframe
    df = {}
    for file in files:
        df[file] = pd.read_csv(data_path+file)
        print(file)
        
    #grab names of csv files
    df_key = []
    for key, value in df.items():
        df_key.append(key)
        
    #loop through all files and upload to db    
    for k in df_key:
        dataframe = df[k] #call dataframe

        #force column names to be lower case, no spaces, no dashes
        dataframe.columns = [x.lower().replace(" ", "").replace("-","_") for x in dataframe.columns]

        #save to csv
        dataframe.to_csv(k, header = dataframe.columns, index=False) 

        #open database connection
        conn_string = "host={0} dbname={1} user={2} password={3}".format(host, dbname, user, pwd)
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        print('Opened database successfully')

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

        #execute on db
        cursor.execute("create table %s (%s)" % (tbl_name, col_str))
        print('{0} created successfully'.format(tbl_name))
        conn.commit()

        #open the file, save as an object, and upload to db
        my_file = open(k) 
        try:
            process_file(conn, tbl_name, my_file)
        finally:
            conn.close()
        
    print('all files imported to db')


#Main 
data_path = os.getcwd()
schema = #db schema
host = #host endpoint
dbname = #db name
user = #user name
pwd = #password

import_csv_to_db(data_path, schema, host, dbname, user, pwd)

