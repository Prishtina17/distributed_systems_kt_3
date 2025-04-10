import subprocess

import pandas as pd
from sklearn.preprocessing import StandardScaler
import pyarrow as pa
import pyarrow.parquet as pq


CONTAINER_NAME = 'namenode'

df1 = pd.read_csv('Road Accident Data.csv')
df2 = pd.read_csv('World Important Dates.csv')


print(df1.info())
print(df2.info())

def preprocess_road_data(df):
    columns = ['Latitude', 'Longitude', 'Number_of_Casualties', 'Number_of_Vehicles', 'Speed_limit']
    scaler = StandardScaler()
    scaler.fit(df[columns])
    df[columns] = scaler.transform(df[columns])
    print(df[columns].head())
    return df

def preprocess_world_dates_data(df):
    columns = ['Sl. No']
    scaler = StandardScaler()
    scaler.fit(df[columns])
    df[columns] = scaler.transform(df[columns])
    print(df[columns].head())
    return df

def save_to_parquet(df, file_name):
    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_name)
    print(f"Данные успешно сохранены в файл: {file_name}")




df1 = preprocess_road_data(df1)

df2 = preprocess_world_dates_data(df2)

save_to_parquet(df1, 'road_accident_data.parquet')
save_to_parquet(df2, 'world_important_dates.parquet')

local_files = ['road_accident_data.parquet', 'world_important_dates.parquet']
container_path = '/tmp/'

for file in local_files:
    copy_command = f"docker cp {file} {CONTAINER_NAME}:{container_path}{file}"
    subprocess.run(copy_command, shell=True, check=True)
    print(f"Файл {file} скопирован в контейнер.")

hdfs_paths = {
    'road_accident_data.parquet': '/anton.mikhaylovskiy/road_accident_data.parquet',
    'world_important_dates.parquet': '/anton.mikhaylovskiy/world_important_dates.parquet'
}

for file, hdfs_path in hdfs_paths.items():
    upload_command = f"docker exec {CONTAINER_NAME} hdfs dfs -put {container_path}{file} {hdfs_path}"
    subprocess.run(upload_command, shell=True, check=True)
    print(f"Файл {file} загружен в HDFS по пути: {hdfs_path}")
