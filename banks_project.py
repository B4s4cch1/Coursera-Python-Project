import numpy as np
import pandas as pd
import datetime
import requests
from bs4 import BeautifulSoup
import sqlite3

def log_progress(message):
    now=datetime.datetime.now()
    writer="{timestamp} : {message} \n".format(timestamp=now,message=message)
    with open("code_log.txt","a") as file:
        file.write(writer)
    return

def extract(url, table_attributes):
    request=requests.get(url)
    text=request.text
    soup=BeautifulSoup(text,'html.parser')
    df=pd.DataFrame(columns=table_attributes)
    tables=soup.find_all("tbody")
    table_rows=tables[0].find_all("tr")

    for table_row in table_rows:
        table_elements=table_row.find_all("td")
        if len(table_elements) != 0:
            auxdf=pd.DataFrame(columns=table_attributes)
            auxdf['Name']=table_elements[1].find_all("a")[1].contents
            auxdf['MC_USD_Billion']=table_elements[2].contents
            df=pd.concat([df,auxdf],ignore_index=True)

    df['MC_USD_Billion']=df["MC_USD_Billion"].str.replace("\n","")
    return df

def transform(df,csv_path):
    df['MC_USD_Billion']=df['MC_USD_Billion'].astype(float)
    dic={}
    exchange=pd.read_csv(csv_path)

    for idx in exchange.index:
       dic[exchange["Currency"].iloc[idx]]=exchange['Rate'].iloc[idx]
    
    df['MC_GBP_Billion'] = np.round(df['MC_USD_Billion']*float(dic['GBP']),2)
    df['MC_EUR_Billion'] = np.round(df['MC_USD_Billion']*float(dic['EUR']),2)
    df['MC_INR_Billion'] = np.round(df['MC_USD_Billion']*float(dic['INR']),2)
    return df

def load_to_csv(df,output_path):
    df.to_csv(output_path,index=False)
    return

def load_to_db(df,sql_connection,table_name):
    df.to_sql(table_name,sql_connection,if_exists='replace',index=False)
    return

def run_query(query_statement,sql_connection):
    df=pd.read_sql(query_statement,sql_connection)
    print(df)
    print("\n")
    return

url="https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks"
csv_path="https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
table_attributes=["Name","MC_USD_Billion"]
output_path="Largest_banks_data.csv"
dbName="Banks.db"
table_name="Largest_banks"


log_progress("Preliminaries complete. Initiating ETL process")

df=extract(url,table_attributes)
log_progress("Data extraction complete. Initiating Transformation process")

df=transform(df,csv_path)
log_progress("Data transformation complete. Initiating Loading process")

load_to_csv(df,output_path)
log_progress("Data saved to CSV file")

sql_connection=sqlite3.connect(dbName)
log_progress("SQL Connection initiated")

load_to_db(df,sql_connection,table_name)
log_progress("Data loaded to Database as a table, Executing queries")

query_statement="SELECT * FROM Largest_banks"
run_query(query_statement,sql_connection)
query_statement="SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement,sql_connection)
query_statement="SELECT Name from Largest_banks LIMIT 5"
run_query(query_statement,sql_connection)
log_progress("Process Complete")

sql_connection.close()
log_progress("Server Connection closed")