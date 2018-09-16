# -*- coding: utf-8 -*-
"""Copy of import_csv_to_db-v4.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1ugS8iGnReIdr1Rmz18fLrfoplRbC7SWe

#Import CSVs to DB

Script to upload CSVs to a postgres database
"""

!pip install psycopg2
import os
import numpy as np
import pandas as pd
import psycopg2
from google.colab import files

"""## Functions"""

def csv_files():

    #get names of only csv files
    csv_files = []
    for file in os.listdir(os.getcwd()):
        if file.endswith(".csv"):
            csv_files.append(file)

    return csv_files

def configure_dataset_directory(csv_files, dataset_dir):
  
    #make dataset folder to process csv files
    try: 
      mkdir = 'mkdir {0}'.format(dataset_dir)
      os.system(mkdir)
    except:
      pass

    #move csv files to dataset folder
    for csv in csv_files:
      mv_file = 'mv {0} {1}'.format(csv, dataset_dir)
      os.system(mv_file)

    return mkdir

def create_df(dataset_dir):
  
    data_path = os.getcwd()+'/'+dataset_dir+'/'

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
    
    return df, df_key

def clean_tbl_name(filename, schema):
  
    #rename csv, force lower case, no spaces, no dashes
    cleaned_tbl_name = filename.lower().replace(" ", "").replace("-","_").replace(r"/","_").replace("\\","_").replace(".","_").replace("$","")
    
    tbl_name = '{0}.{1}'.format(schema, cleaned_tbl_name.split('.')[0]) #save table name and schema

    return tbl_name

def clean_colname(df):
  
    #force column names to be lower case, no spaces, no dashes
    df.columns = [x.lower().replace(" ", "").replace("-","_").replace(r"/","_").replace("\\","_").replace(".","_").replace("$","") for x in df.columns]
    
    #processing data
    replacements = {
        'timedelta64[ns]': 'varchar',
        'object': 'varchar',
        'float64': 'float',
        'int64': 'int',
        'datetime64': 'timestamp'
    }

    col_str = ", ".join("{} {}".format(n, d) for (n, d) in zip(dataframe.columns, dataframe.dtypes.replace(replacements)))
    
    return col_str, df.columns

def upload_to_db(schema, host, dbname, user, pwd, tbl_name, col_str, file, dataframe, dataframe_columns):
    
    #open database connection
    conn_string = "host={0} dbname={1} user={2} password={3}".format(host, dbname, user, pwd)
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    print('opened database successfully')
        
    #execute on db
    cursor.execute("drop table if exists {0};".format(tbl_name))
    cursor.execute("create table %s (%s)" % (tbl_name, col_str))
    print('{0} created successfully'.format(tbl_name))

    #save to csv
    dataframe.to_csv(k, header = dataframe_columns, index=False, encoding='utf-8') 
    
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

"""## Upload files"""

#upload files to colab server
uploaded = files.upload()

for fn in uploaded.keys():
  print('User uploaded file "{name}" with length {length} bytes'.format(
      name=fn, length=len(uploaded[fn])))

"""## Run Main"""

#main

#user settings
dataset_dir = 'datasets' #folder name to process csv files
schema = 'datasets' #postgres db schema name

#configure environment and create main df
csv_files = csv_files()
configure_dataset_directory(csv_files, dataset_dir)
df, df_key = create_df(dataset_dir)


#loop through all files and upload to db    
for k in df_key:
    
    #call dataframe
    dataframe = df[k] 

    #clean table name
    tbl_name = clean_tbl_name(k, schema)
    
    #clean column names
    col_str, dataframe.columns = clean_colname(dataframe)
    
    
    upload_to_db(schema, 
                 host = 'enter host url', 
                 dbname = 'enter db name',
                 user = 'enter username',
                 pwd = 'enter pwd',
                 tbl_name = tbl_name, 
                 col_str = col_str,
                 file = k,
                 dataframe = dataframe,
                 dataframe_columns = dataframe.columns)
    
        
print('all files imported to db')