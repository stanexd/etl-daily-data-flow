import psycopg2
import json
import pandas as pd
from sqlalchemy import create_engine

#Подключение к БД
##################################################################################################################################################################

with open('config.json', encoding='UTF-8') as f:
    db_config = json.load(f)

conn = psycopg2.connect(**db_config)
cursor = conn.cursor()

sql_alch_conn = create_engine(f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}")

#Создание схемы BANK
##################################################################################################################################################################

cursor.execute('drop schema if exists bank cascade;')
conn.commit()
cursor.execute('create schema if not exists bank;')
conn.commit()

cursor.execute('set search_path to bank;')

with open('sql_scripts/ddl_dml.sql', 'r', encoding='UTF-8') as f:
    loading_script = f.read()

replacements = {
    "cards": "DWH_DIM_CARDS",
    "clients": "DWH_DIM_CLIENTS",
    "accounts": "DWH_DIM_ACCOUNTS"    
}

for old, new in replacements.items():
    loading_script = loading_script.replace(old, new)

for part in loading_script.split(';'):
    part = part.strip()
    if part:
        cursor.execute(part)
        conn.commit()

'''
Далее, поскольку предполагается, что на вход нам подаются лишь сущности 
terminals, transactions и passport_blacklist,
считаю логичным принимать за SCD2 таблицы passport_blacklist и terminals.
(transactions не принимаем за SCD2, поскольку транзакция - "моментальное" 
событие, у которого, по сути, start_dttm = end_dttm, a deleted_flg не может быть 1).
Пусть в условии и сказано, что "passport_blacklist - это список паспортов, включенных в «черный список» 
с накоплением с начала месяца.", предположим, что в какой-то очередной выгрузке может не оказаться паспорта, 
который был в предыдущей выгрузке (пользователя разбанили), или же в очередной выгрузке некоторые
паспорта изменились (допустим коллеги из соседнего отдела умеют отслеживать этот момент),
что соответствует смене паспорта пользователем.
'''

#Загружка данных в STG
##################################################################################################################################################################

def text_csv_2sql(path, name, con=sql_alch_conn, schema='bank', if_exists='replace', index=False):
    if '.txt' in path:
        df = pd.read_csv(path, sep=';')
        df.to_sql(name=name, con=con, schema=schema, if_exists=if_exists, index=index)
    if '.xlsx' in path:
        df = pd.read_excel(path)
        df.to_sql(name=name, con=con, schema=schema, if_exists=if_exists, index=index)

text_csv_2sql('transactions_01032021.txt', 'stg_transaction')
text_csv_2sql('terminals_01032021.xlsx', 'stg_terminals')
text_csv_2sql('passport_blacklist_01032021.xlsx', 'stg_passport_blacklist')