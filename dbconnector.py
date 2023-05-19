import pyodbc
import pandas as pd
import yaml
import os
import pyarrow
import logging
from datetime import datetime

#create a directory for the log file
log_directory = "Logs"
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

#set up logging
log_filename = os.path.join(log_directory,"dataconversion"+"_"+str(datetime.strftime(datetime.now(), '%Y%m%d_%H%M%S')) + ".log")
logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

try:
    conf = yaml.safe_load(open('credentials.yml'))
    uid = conf['user']['uid']
    pwd = conf['user']['pwd']

    #db connection information
    driver = "{SQL Server}"
    server = "YOUR_SQL_SERVER_CONNECTIONS"
    database = "AdventureWorks2019"

    #establish connection to the db
    conn = pyodbc.connect(
        'DRIVER=' + driver + ';SERVER=' + server + ';DATABASE=' + database + ';UID=' + uid + ';PWD=' + pwd)

    #get list of tables in the database using list comprehension
    cursor = conn.cursor()
    tables = cursor.tables(tableType='TABLE')
    table_names = [f"{table.table_schem}.{table.table_name}" for table in tables]


    #iterate over each table then convert to a dataframe and save as parquet
    for table_name in table_names:
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, conn)
        schema, table = table_name.split('.')
        parquet_directory = "AdventureWorks2019"
        parquet_filename = f"{parquet_directory}/{schema}/{table}.parquet"

        #check if the file exists
        if os.path.exists(parquet_filename):
            #if file exists then generate a unique filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            parquet_filename = f"{parquet_directory}/{schema}/{table}_{timestamp}.parquet"

        #create directory if it does not exist
        if not os.path.exists(parquet_directory):
            os.makedirs(parquet_directory)

        #create schema directory if it does not exist
        if not os.path.exists(os.path.join(parquet_directory, schema)):
            os.makedirs(os.path.join(parquet_directory, schema))

        #specify the compression method as either 'snappy', 'gzip', 'lzo', 'zstd'
        compression = 'zstd'

        #write df to parquet with compression
        df.to_parquet(parquet_filename, index=False, compression=compression)
        logging.info(f"Table '{table_name}' converted and saved as '{parquet_filename}' with {compression} compression")

    #end logging
    logging.info("Conversion completed.")

except Exception as e:
    logging.exception("An error occurred during conversion:")
    raise e

finally:
    #close connection to MS SQL Server
    if 'conn' in locals():
        conn.close()