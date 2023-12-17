#VMT1991 - Version 1 generate on 17/12/2023

import os
import pyodbc, struct
from azure import identity
from typing import Union
from fastapi import FastAPI
from pydantic import BaseModel
import csv
import time

path_file=""
table_name=""
colume_list=[]
data_import={}
num_column=0
AZURE_SQL_CONNECTIONSTRING = ""

def prepare_connection_string(server,database,username,password):
    global AZURE_SQL_CONNECTIONSTRING
    server = 'sqlserver0906.database.windows.net'
    database = 'csvtosql'
    username = 'sqladmin'
    password = 'Azure@123456'
    driver= '{ODBC Driver 18 for SQL Server}'
    AZURE_SQL_CONNECTIONSTRING = 'DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password
    
   

def Create_Table(table_name):
    global AZURE_SQL_CONNECTIONSTRING,colume_list,num_column
    print(AZURE_SQL_CONNECTIONSTRING)
    print(f"Create New Table: {table_name}")
    conn = pyodbc.connect(AZURE_SQL_CONNECTIONSTRING)
    cursor = conn.cursor()
    sql_str= "CREATE TABLE "+table_name+" ( "
    for i in range(0,num_column-1):
        sql_str+=colume_list[i].replace(" ","_") +" nvarchar(190),"
    sql_str+=colume_list[num_column-1]+" nvarchar(190))"
    print(sql_str)
    cursor.execute(sql_str)

    conn.commit()
    conn.close()

def Import_To_Sql(table_name):
    global data_import
    global AZURE_SQL_CONNECTIONSTRING,colume_list,num_column
    conn = pyodbc.connect(AZURE_SQL_CONNECTIONSTRING)
    cursor = conn.cursor()
    print(f"Start import data from csv file to table: {table_name}")
    
    for i in data_import:
        a_row=data_import[i]
        sql_str=f"INSERT INTO {table_name} VALUES ("
        for a_column in a_row:
            sql_str+="'"+a_column+"', "
        sql_str=sql_str[:-2]+")"
        #print(sql_str)
        cursor.execute(sql_str)


    conn.commit()
    conn.close()


def Prepare_data(path_file):
    global colume_list
    global num_column
    global data_import
    with open(path_file) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        num_column=0
        for row in csv_reader:
            if line_count == 0:
                #print(f'Column names are {", ".join(row)}')
                print(f"Column names are: {row}")
                num_column = len(row)
                for j in range(0,num_column):
                    colume_list.append(row[j])
                line_count += 1
            else:
                data_import[line_count]=row
                line_count += 1
        print(f'Processed {line_count} lines.')





server=input("Please input Azure SQL server name: ")
database=input("Please input Azure SQL server database name need import data from csv file: ")
print("----------------------------------------------------------------------------------------")
username=input("Please input Azure SQL server database username: ")
print("----------------------------------------------------------------------------------------")
password=input("Please input Azure SQL server database password: ")
print("----------------------------------------------------------------------------------------")
table_name=input(f"Please input table name in database {database}: ")
print("----------------------------------------------------------------------------------------")
path_file=input(f"Please CSV file path for importing to table{table_name}: ")
print("----------------------------------------------------------------------------------------")
prepare_connection_string(server,database,username,password)
Prepare_data(path_file)
Create_Table(table_name)
time.sleep(6)
Import_To_Sql(table_name)
print("************************Import Data Complete********************")