#!/usr/bin/env python
# coding: utf-8

# In[1]:


#скрипт для инкрементной загрузки данных

import os
import requests
import csv
import pandas
import time
import datetime
import sqlalchemy


# In[ ]:


#задаем данные для подключения к облачному серверу ibm -  Db2 hostname, username, and password
#задаем dsn connection string
#создаем соединение с базой данных


# In[2]:


import ibm_db
import ibm_db_dbi

dsn_hostname = "0c77d6f2-5da9-48a9-81f8-86b520b87518.bs2io90l08kqb1od8lcg.databases.appdomain.cloud" 
dsn_uid = "spz10162"        
dsn_pwd = "*****"      

dsn_driver = "{IBM DB2 ODBC DRIVER}"
dsn_database = "BLUDB"            
dsn_port = "31198"                 
dsn_protocol = "TCPIP"           
dsn_security = "SSL"             

dsn = (
    "DRIVER={0};"
    "DATABASE={1};"
    "HOSTNAME={2};"
    "PORT={3};"
    "PROTOCOL={4};"
    "UID={5};"
    "PWD={6};"
    "SECURITY={7};").format(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd,dsn_security)

try:
    conn = ibm_db.connect(dsn, "", "")
    print ("Connected to database: ", dsn_database, "as user: ", dsn_uid, "on host: ", dsn_hostname)

except:
    print ("Unable to connect: ", ibm_db.conn_errormsg() )


# In[ ]:


# загружаем Python Magic for SQL (если не установлен необходимо выполнить !pip install ipython-sql, !pip install sqlalchemy )
# загружаем библиотеку для работы с облачной базой данных ibm
# загружаем IBM_DB_SA adapter для Python/SQLAlchemy interface to IBM Data Servers 
# если не установлен необходимо выполнить !pip install ibm_db_sa ( https://github.com/ibmdb/python-ibmdbsa)   


# In[3]:


get_ipython().run_line_magic('load_ext', 'sql')

import ibm_db_sa
from sqlalchemy import *

get_ipython().run_line_magic('sql', 'ibm_db_sa://spz10162:****@0c77d6f2-5da9-48a9-81f8-86b520b87518.bs2io90l08kqb1od8lcg.databases.appdomain.cloud:31198/BLUDB;security=SSL;')

SQLALCHEMY_DATABASE_URI = 'ibm_db_sa://spz10162:*****@0c77d6f2-5da9-48a9-81f8-86b520b87518.bs2io90l08kqb1od8lcg.databases.appdomain.cloud:31198/BLUDB;security=SSL;'
engine = create_engine(SQLALCHEMY_DATABASE_URI, echo=False)
                        


# In[4]:


# загружаем отслеживаемые финансовые иснтрументы

get_ipython().run_line_magic('sql', 'select * from finance_instrument;')


# In[5]:


#создаем датафрейм (df) из финансовых инструментов 

df_finance_instrument = get_ipython().run_line_magic('sql', 'select * from finance_instrument')

df_finance_instrument = df_finance_instrument.DataFrame()

print(df_finance_instrument)


# In[6]:


#лист тикеров акций
list_tiker_finance_instrument=list(df_finance_instrument.ticker_symbol.tolist())
print (list_tiker_finance_instrument)


# In[7]:


#скачиваем данные с www.alphavantage.co сохраняем их в csv файлы на диске - создаем сырой слой данных

def download_data(csv_url, csv_filename):
    ''' Downloads file from the url and save it as filename '''
    # check if file already exists
    if not os.path.isfile(csv_filename):
        print('Загрузка началась, ожидайте...')
        response = requests.get(csv_url)
        # Check if the response is ok (200)
        if response.status_code == 200:
            # Open file and write the content
            with open(csv_filename, 'wb') as file:
                for line in response:
                    file.write(line)
            print(f'Файл сохранен' )
        else:
            print('Ошибка подключения')
    else:
        print('Файл с таким именем уже существует')


# In[8]:


today_date = datetime.date.today()

for api_tiker in list_tiker_finance_instrument:
    csv_url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={api_tiker}&interval=1min&apikey=HIOUN8XSLYYD0JP9&datatype=csv&outputsize=full'
    csv_filename = f'/home/anatoliy/data_store/{api_tiker}_data_day_{today_date}.csv'
    
    download_data(csv_url, csv_filename)
    #ожидание в 70 секунд между запросами, чтобы не попасть на ограничение
    time.sleep(70)
    
       


# In[ ]:


#на выходе получили слой сырых данных содержащий:
# - полученные и сохранненые данные за последний месяц для заданных акций в виде: 
#   BABA_data_day_2022-12-25.csv


# In[ ]:


# ниже создаем ETL слой данных


# In[12]:


# загружаем данные из хранилища

for api_tiker in list_tiker_finance_instrument:
    
    tiker_frame_increment = pandas.read_csv(f'/home/anatoliy/data_store/{api_tiker}_data_day_{today_date}.csv')
    
    # обогощаем данные, добавляя тикер для индетификации инструмента
    tiker_frame_increment.insert(0, 'tiker', f'{api_tiker}')
    
    # заменяем название столбцов, чтобы не совпадали с ситемными именами 
    tiker_frame_increment = tiker_frame_increment.rename(columns={"timestamp": "tiker_timestamp", "open": "tiker_open", "high": "tiker_high", "low": "tiker_low", "close": "tiker_close", "volume": "tiker_volume"})
    
    # смотрим, что в фреймах правильные данные
    print ('Фрейм с полуенными данными', tiker_frame_increment.head(1), end='\n')
    
    # преобразуем формат данных в столбце с датой во временной
    tiker_frame_increment['tiker_timestamp'] = pandas.to_datetime(tiker_frame_increment['tiker_timestamp'])
      
    pconn = ibm_db_dbi.Connection(conn)
    
    # проверям максимальную дату данных на сервере
    max_save_timestamp  = pandas.read_sql(f'SELECT MAX(tiker_timestamp) FROM data_tiker_{api_tiker}', pconn)

    max_save_timestamp = max_save_timestamp['1'].iloc[0]
 
    # удаляем из фрема ранее полученные данные, которые уже есть на сервере 

    mask_timestamp = (tiker_frame_increment['tiker_timestamp'] > max_save_timestamp) 
    tiker_frame_increment = tiker_frame_increment.loc[mask_timestamp]
 
    # добавляем новые данные к существующей таблице, для примера - в таблица data_tiker_baba

    tiker_frame_increment.to_sql(f'DATA_TIKER_{api_tiker}'.lower(), con = engine, index=False, if_exists='append')
    
    # проверям максимальную дату данных после обновления на сервере
    max_save_timestamp_now  = pandas.read_sql(f'SELECT MAX(tiker_timestamp) FROM data_tiker_{api_tiker}', pconn)

    max_save_timestamp_now  = max_save_timestamp_now ['1'].iloc[0]
    
    print ('Текущее последнее значение даты и времени у сохраненных данных для',  api_tiker, max_save_timestamp_now, end='\n\n')
     


# In[ ]:


# создаем витрину данных, в которой таблицы вида:
# baba_day_data_temp - содержит данные по финансовому инструменту baba за предыдущий день
# baba_day_data - содержит данные, сформированные для витрины, по финансовому инструменту baba за предыдущий день
# baba_day_data_mart - содержит данные витрины финансового инструмента baba за предыдущий день


# In[13]:


list_tiker_finance_instrument = [item.lower() for item in list_tiker_finance_instrument]
print (list_tiker_finance_instrument)


# In[14]:


try:
    for api_tiker in list_tiker_finance_instrument:
        sqlStatement = f'DROP TABLE {api_tiker}_day_data_mart'
        resultSet = ibm_db.exec_immediate(conn, sqlStatement)
        
        sqlStatement = f'DROP TABLE {api_tiker}_day_data'
        resultSet = ibm_db.exec_immediate(conn, sqlStatement)  
        
        sqlStatement = f'DROP TABLE {api_tiker}_day_data_temp'   
        resultSet = ibm_db.exec_immediate(conn, sqlStatement)  
    print ('дневные таблицы удалены' )

except Exception as drop_table_error: 
        print (drop_table_error )


# In[15]:


# агрегируем данные для витрины

try:
    for api_tiker in list_tiker_finance_instrument:
        sqlStatement =  f""" 
        CREATE TABLE {api_tiker}_day_data_temp (
        tiker VARCHAR (50),
        tiker_timestamp DATETIME,
        tiker_open DECIMAL(15,2),
        tiker_high DECIMAL(15,2),
        tiker_low DECIMAL(15,2),
        tiker_close DECIMAL(15,2),
        tiker_volume INTEGER
        ) ;
        """

        preparedStmt = ibm_db.prepare(conn, sqlStatement)
        returnCode = ibm_db.execute(preparedStmt)

        sqlStatement =  f""" 
        INSERT INTO {api_tiker}_day_data_temp  
        SELECT * FROM data_tiker_{api_tiker}
        WHERE DATE(tiker_timestamp) = (CURRENT_DATE - 1 DAY);  
        """

        preparedStmt = ibm_db.prepare(conn, sqlStatement)
        returnCode = ibm_db.execute(preparedStmt)


        sqlStatement =  f"""
        CREATE TABLE {api_tiker}_day_data (
        tiker VARCHAR (50),
        day_volume INTEGER,
        day_open DECIMAL(15,2),
        day_close DECIMAL(15,2),
        max_volume_time DATETIME,
        high_price_time DATETIME,
        low_price_time DATETIME
        ) ;
         """

        preparedStmt = ibm_db.prepare(conn, sqlStatement)
        returnCode = ibm_db.execute(preparedStmt)

        sqlStatement =  f"""
        INSERT INTO {api_tiker}_day_data (tiker, day_volume, day_open, day_close, max_volume_time,
        high_price_time, low_price_time )

        SELECT
        (SELECT tiker FROM {api_tiker}_day_data_temp  
        WHERE tiker_timestamp = (SELECT MIN(tiker_timestamp) FROM {api_tiker}_day_data_temp)) ,


        SUM(tiker_volume) AS tiker_day_volume,

        (SELECT tiker_open FROM {api_tiker}_day_data_temp
             WHERE tiker_timestamp = (SELECT MIN(tiker_timestamp) FROM {api_tiker}_day_data_temp)) AS tiker_day_open ,

        (SELECT tiker_close FROM {api_tiker}_day_data_temp
             WHERE tiker_timestamp = (SELECT MAX(tiker_timestamp) FROM {api_tiker}_day_data_temp)) AS tiker_day_close ,

        (SELECT MIN(tiker_timestamp) FROM {api_tiker}_day_data_temp
        WHERE  tiker_volume = (SELECT MAX(tiker_volume)
        FROM {api_tiker}_day_data_temp)) AS tiker_max_volume_time,

        (SELECT MIN(tiker_timestamp) FROM {api_tiker}_day_data_temp
        WHERE  tiker_high = (SELECT MAX(tiker_high)
        FROM {api_tiker}_day_data_temp)) AS tiker_high_price_time,

        (SELECT MIN(tiker_timestamp) FROM {api_tiker}_day_data_temp
        WHERE  tiker_low = (SELECT MIN(tiker_low)
        FROM {api_tiker}_day_data_temp)) AS tiker_low_price_time

        FROM {api_tiker}_day_data_temp;
         """

        preparedStmt = ibm_db.prepare(conn, sqlStatement)
        returnCode = ibm_db.execute(preparedStmt)

        sqlStatement =  f""" 
        CREATE TABLE {api_tiker}_day_data_mart
        (
        company_name VARCHAR (50),
        tiker VARCHAR (50),
        day_volume INTEGER,
        day_open DECIMAL(15,2),
        day_close DECIMAL(15,2),
        delta_percent DECIMAL(15,2),
        max_volume_time DATETIME,
        high_price_time DATETIME,
        low_price_time DATETIME
        ) ;
        """

        preparedStmt = ibm_db.prepare(conn, sqlStatement)
        returnCode = ibm_db.execute(preparedStmt)

        sqlStatement =  f""" 
        INSERT INTO {api_tiker}_day_data_mart (company_name, tiker, day_volume, day_open, day_close,
        delta_percent, max_volume_time, high_price_time, low_price_time )

        SELECT n.company_name, d.tiker, d.day_volume, d.day_open, d.day_close,  
          2*100*(d.day_close - d.day_open)/(d.day_close + d.day_open) AS delta_percent,
        d.max_volume_time, d.high_price_time, d.low_price_time
                FROM {api_tiker}_day_data AS d
                LEFT JOIN finance_instrument AS n ON d.tiker=n.ticker_symbol;
        """

        preparedStmt = ibm_db.prepare(conn, sqlStatement)
        returnCode = ibm_db.execute(preparedStmt)    
    
        print (api_tiker, 'mart done')
    
except Exception as table_creat_error: 
        print (table_creat_error)


# In[16]:


# проверка витрины данных
get_ipython().run_line_magic('sql', 'SELECT * FROM baba_day_data_mart')


# In[17]:


get_ipython().run_line_magic('sql', 'SELECT * FROM vlo_day_data_mart')


# In[18]:


get_ipython().run_line_magic('sql', 'SELECT * FROM save_day_data_mart')


# In[19]:


get_ipython().run_line_magic('sql', 'SELECT * FROM aapl_day_data_mart')


# In[20]:


get_ipython().run_line_magic('sql', 'SELECT * FROM goog_day_data_mart')


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[172]:





# In[ ]:




