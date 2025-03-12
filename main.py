import psycopg2
import json
import re
import shutil
import pandas as pd
import py_scripts.fraud_table as ft
import py_scripts.hist_tables as ht
import py_scripts.stg_tables as st
from pathlib import Path
from sqlalchemy import create_engine, DECIMAL

#Подключение к БД
##################################################################################################################################################################

with open('config.json', encoding='UTF-8') as f:
    db_config = json.load(f)

conn = psycopg2.connect(**db_config)
cursor = conn.cursor()

sql_alch_conn = create_engine(f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}")

#Получение даты входящих файлов
##################################################################################################################################################################

def extract_date_from_filename():

    filename = list(Path('incoming_files').iterdir())[0].name
    match = re.search(r"_(\d{2})(\d{2})(\d{4})\.", filename)

    if match:
        day = match.group(1)
        month = match.group(2)
        year = match.group(3)
        return f"{year}-{month}-{day}"  
    else:
        return None

#Загрузка данных в STG
#Для корректной работы скрипта, перед запуском в incoming_files необходимо загружать по три входящих файла
##################################################################################################################################################################

def txt_xlsx_2_sql(path, name, con=sql_alch_conn, schema='bank', if_exists='replace', index=False):

    cursor.execute(f'drop table if exists {name};')
    conn.commit()

    if '.txt' in path:
        df = pd.read_csv(path, sep=';',  decimal=',')
    if '.xlsx' in path:
        df = pd.read_excel(path)

    #приведение типов
    for col in df.columns:
        if 'date' in col.lower():
            df[col] = pd.to_datetime(df[col], errors='coerce')
        if 'amount' in df.columns:
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        
    dtype = {'amount': DECIMAL(10, 2)}

    df.to_sql(name=name, con=con, schema=schema, if_exists=if_exists, index=index, dtype=dtype)

#Перенос файлов в архив (не добавлял расширение backup, чтобы при отладке проще было их перебрасывать снова в каталог incoming_files)
##################################################################################################################################################################

def files_to_archive():
    for file_path in Path('incoming_files').iterdir():
        destination_path = Path('archive') / file_path.name
        shutil.move(str(file_path), str(destination_path))

##################################################################################################################################################################

if __name__ == '__main__':

    date = extract_date_from_filename()

    #создание схемы BANK

    if '2021-03-01' in date:
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

    cursor.execute('set search_path to bank;')

    '''
    Далее, поскольку предполагается, что на вход нам подаются лишь сущности 
    terminals, transactions и passport_blacklist,
    считаю логичным принимать за SCD2 таблицы passport_blacklist и terminals.
    (transactions не принимаем за SCD2, поскольку транзакция - "моментальное" 
    событие, у которого, по сути, start_dttm = end_dttm, a deleted_flg не может быть 1).
    Пусть в условии и сказано, что "passport_blacklist - это список паспортов, включенных в «черный список» 
    с накоплением с начала месяца.", предположим, что в какой-то очередной выгрузке может не оказаться паспорта, 
    который был в предыдущей выгрузке (пользователя разбанили).
    '''

    #загружаем в stg таблицы из файлов
    for file in Path('incoming_files').iterdir():
        if 'transaction' in file.name:
            txt_xlsx_2_sql(f'incoming_files/{file.name}', 'stg_new_rows_transactions')
        if 'terminals' in file.name:
            txt_xlsx_2_sql(f'incoming_files/{file.name}', 'stg_terminals')  
        if 'passport_blacklist' in file.name:
            txt_xlsx_2_sql(f'incoming_files/{file.name}', 'stg_passport_blacklist')      

    #обработанные файлы в отправляются в архив
    files_to_archive()

    #создаем исторические таблицы
    ht.create_hist_passport_blacklist(conn, cursor, date)
    ht.create_hist_terminals(conn, cursor, date)
    ht.create_hist_transactions(conn, cursor)

    #создаем временные таблицы для новых строк (таблицы транзакций тут нет, потому что транзакции не SCD2)
    st.create_new_rows_terminals(conn, cursor)
    st.create_new_rows_passport_blacklist(conn, cursor)

    #создаем временные таблицы для удаленных строк
    st.create_deleted_rows_terminals(conn, cursor)
    st.create_deleted_rows_passport_blacklist(conn, cursor)

    '''
    Будем считать, что обновленнных строк в загружаемом черном списке паспортов нет,
    т.е. если вдруг пользователь поменяет паспорт, отдел 'черного списка паспортов'
    просто добавит новый паспорт данного гражданина в очередной выгрузке, а старый, например, удалит.
    Поэтому в исторической таблице мы закроем запись о старом паспорте и добавим новый в рамках
    той логики, которая у нас уже есть.
    '''

    #создаем временную таблицу с обновленными строками
    st.create_updated_rows_terminals(conn, cursor)

    #обновляем исторические таблицы
    ht.update_transactions_hist_table(conn, cursor)
    ht.update_terminals_hist_table(conn, cursor, date)
    ht.update_passport_blacklist_hist_table(conn, cursor, date)

    #создаем таблицу отчета
    ft.create_rep_fraud(conn, cursor)
    ft.update_rep_fraud(conn, cursor)

    #удаляем временные таблицы
    st.del_stg_tables(conn, cursor)