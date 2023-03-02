
import pandas as pd
import time
import os
from s3fs.core import S3FileSystem
import boto3
import psycopg2


s3_bucket = 'fhnw-cas-de'






def read_data_from_DB(s3_file):
    print('#####################################')
    print('read_data_from_DB')
    start_time =  time.time()
    dict = {'custom_1988_2020_10.csv': 'limit 10',
            'custom_1988_2020_10000000.csv': 'limit 10000000',
            'custom_1988_2020_20000000.csv': 'limit 20000000',
            'custom_1988_2020_50000000.csv': 'limit 50000000', 
            'custom_1988_2020_full.csv': ''}
    limit = dict[s3_file]
    conn = psycopg2.connect(database='postgres', user=DB_master,password=DB_PW, host=DB_URL, port='5432')
    sql = f'SELECT * FROM japan_trade {limit}'
    df = pd.read_sql(sql = sql, con= conn)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f'Execution time: {elapsed_time} seconds')
    # print(df.head())
    return pd.DataFrame([{'Method' : 'read_data_from_DB', 'dataset' : s3_file, 'elapsed_time' : elapsed_time}])



def download_and_load_data(s3_file):
    print('#####################################')
    print('download_and_load_data')
    start_time =  time.time()

    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY , aws_secret_access_key=SECRET)
    s3.download_file(s3_bucket, s3_file,f'./{s3_file}')
    df = pd.read_csv(f'./{s3_file}')
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f'Execution time: {elapsed_time} seconds')
    # print(df.head())
    return pd.DataFrame([{'Method' : 'download_and_load_data', 'dataset' : s3_file, 'elapsed_time' : elapsed_time}])
    
    
def read_data_from_s3(s3_file):
    print('#####################################')
    print('read_data_from_s3')
    start_time =  time.time()

    s3 = S3FileSystem(anon=False, key=ACCESS_KEY, secret=SECRET)
    df = pd.read_csv(f's3://{s3_bucket}/{s3_file}')

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f'Execution time: {elapsed_time} seconds')
    # print(df.head())
    return pd.DataFrame([{'Method' : 'read_data_from_s3', 'dataset' : s3_file, 'elapsed_time' : elapsed_time}])

def read_data_from_s3_pyarrow(s3_file):
    print('#####################################')
    print('read_data_from_s3_pyarrow')
    start_time =  time.time()

    s3 = S3FileSystem(anon=False, key=ACCESS_KEY, secret=SECRET)
    df = pd.read_csv(f's3://{s3_bucket}/{s3_file}', engine = 'pyarrow')

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f'Execution time: {elapsed_time} seconds')
    # print(df.head())
    return pd.DataFrame([{'Method' : 'read_data_from_s3_pyarrow', 'dataset' : s3_file, 'elapsed_time' : elapsed_time}])
    

if __name__ == "__main__":
    dict = {'0': 'custom_1988_2020_10.csv',
        '1': 'custom_1988_2020_10000000.csv',
        '2': 'custom_1988_2020_20000000.csv',
        '5': 'custom_1988_2020_50000000.csv', 
        'full': 'custom_1988_2020_full.csv'}
    s3_file = dict['1']
    print('Evaluation Running Times')
    df = read_data_from_s3(s3_file)
    df = pd.concat([df,read_data_from_s3_pyarrow(s3_file)])
    df = pd.concat([df,download_and_load_data(s3_file)])
    df = pd.concat([df,read_data_from_DB(s3_file)])
    print(df)