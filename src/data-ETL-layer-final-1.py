#!/usr/bin/env python
# coding: utf-8

# In[1]:


#скрипт для начальной загрузки данных

import os
import requests
import csv
import pandas
import time
import datetime
import sqlalchemy


# In[ ]:


#если не установлен адаптер ibm_db необходимо выполнить !pip install ibm_db 
#задаем данные для подключения к облачному серверу ibm -  Db2 hostname, username, and password
#задаем dsn connection string
#создаем соединение с базой данных


# In[2]:


import ibm_db
import ibm_db_dbi

dsn_hostname = "0c77d6f2-5da9-48a9-81f8-86b520b87518.bs2io90l08kqb1od8lcg.databases.appdomain.cloud" 
dsn_uid = "*****"        
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

get_ipython().run_line_magic('sql', 'ibm_db_sa://spz10162:*****@0c77d6f2-5da9-48a9-81f8-86b520b87518.bs2io90l08kqb1od8lcg.databases.appdomain.cloud:31198/BLUDB;security=SSL;')

SQLALCHEMY_DATABASE_URI = 'ibm_db_sa://spz10162:*****@0c77d6f2-5da9-48a9-81f8-86b520b87518.bs2io90l08kqb1od8lcg.databases.appdomain.cloud:31198/BLUDB;security=SSL;'
engine = create_engine(SQLALCHEMY_DATABASE_URI, echo=False)
                        


# In[4]:


get_ipython().run_line_magic('sql', 'DROP TABLE finance_instrument;')


# In[ ]:


#создаем таблицу финансовых инструментов, необходимых для сбора данных и построения витрины


# In[6]:


get_ipython().run_cell_magic('sql', '', "\nCREATE TABLE finance_instrument (\n ticker_symbol VARCHAR(50) NOT NULL PRIMARY KEY,\n company_name VARCHAR(80) NOT NULL\n);\nINSERT INTO finance_instrument (ticker_symbol, company_name)\nVALUES\n('BABA', 'Alibaba'),\n('VLO', 'Valero Energy'),\n('SAVE', 'Spirit Airlines'),\n('AAPL', 'Apple'),\n('GOOG', 'Alphabet')\n;")


# In[7]:


get_ipython().run_line_magic('sql', 'select * from finance_instrument;')


# In[8]:


#создаем датафрейм (df) из финансовых инструментов 

df_finance_instrument = get_ipython().run_line_magic('sql', 'select * from finance_instrument')

df_finance_instrument = df_finance_instrument.DataFrame()

print(df_finance_instrument)

#сохраняем данные в csv файл для бекапа

try:
    df_finance_instrument.to_csv('/home/anatoliy/data_store/finance_instrument.csv')
    print ("file saved")

except:
    print ("Unable to save")


# In[9]:


#лист тикеров акций
list_tiker_finance_instrument=list(df_finance_instrument.ticker_symbol.tolist())
print (list_tiker_finance_instrument)


# In[10]:


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


# In[11]:


for api_tiker in list_tiker_finance_instrument:
    csv_url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol={api_tiker}&interval=1min&slice=year1month1&apikey=HIOUN8XSLYYD0JP9'
    csv_filename = f'/home/anatoliy/data_store/{api_tiker}_data.csv'
    
    download_data(csv_url, csv_filename)
    #ожидание в 70 секунд между запросами, чтобы не попасть на ограничение
    time.sleep(70)


# In[ ]:


#на выходе получили слой сырых данных содержащий:
# - список акций для отслеживания - сохранен в файле finance_instrument.csv
# - полученные и сохранненые данные за последний месяц для заданных акций в файлах: 
#    BABA_data.csv, VLO_data.csv, SAVE_data.csv, AAPL_data.csv, GOOG_data.csv


# In[ ]:


# ниже создаем ETL слой данных


# In[12]:


# загружаем данные из хранилища

tiker_frame_dict = {}

for api_tiker in list_tiker_finance_instrument:
    
    tiker_frame = tiker_frame_dict [f'{api_tiker}'] = pandas.read_csv(f'/home/anatoliy/data_store/{api_tiker}_data.csv')
    # обогощаем данные, добавляя тикер для индетификации инструмента
    tiker_frame.insert(0, 'tiker', f'{api_tiker}')
    
    # заменяем название столбцов, чтобы не совпадали с ситемными именами 
    tiker_frame = tiker_frame.rename(columns={"time": "tiker_timestamp", "open": "tiker_open", "high": "tiker_high", "low": "tiker_low", "close": "tiker_close", "volume": "tiker_volume"})
    
    print (tiker_frame.head(1))
    # преобразуем формат данных в столбце с датой во временной
    tiker_frame['tiker_timestamp'] = pandas.to_datetime(tiker_frame['tiker_timestamp'])
      
    # создаем таблицу с данными в базе
    tiker_frame.to_sql(f'DATA_TIKER_{api_tiker}'.lower(), con = engine, index=False)
              


# In[14]:


# проверка созданых таблиц 
get_ipython().run_line_magic('sql', 'SELECT * FROM data_tiker_baba LIMIT 3')


# In[15]:


get_ipython().run_line_magic('sql', 'SELECT * FROM data_tiker_vlo LIMIT 3')


# In[16]:


get_ipython().run_line_magic('sql', 'SELECT * FROM data_tiker_save LIMIT 3')


# In[17]:


get_ipython().run_line_magic('sql', 'SELECT * FROM data_tiker_aapl LIMIT 3')


# In[18]:


get_ipython().run_line_magic('sql', 'SELECT * FROM data_tiker_goog LIMIT 3')

