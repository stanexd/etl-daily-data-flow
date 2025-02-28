import psycopg2
import json
import pandas as pd
from sqlalchemy import create_engine, DECIMAL

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
который был в предыдущей выгрузке (пользователя разбанили).
'''

#Загрузка данных в STG
##################################################################################################################################################################

def text_csv_2_sql(path, name, con=sql_alch_conn, schema='bank', if_exists='replace', index=False):

    cursor.execute(f'drop table if exists {name};')
    conn.commit()

    if '.txt' in path:
        df = pd.read_csv(path, sep=';',  decimal=',')
    if '.xlsx' in path:
        df = pd.read_excel(path)

    for col in df.columns:
        if 'date' in col.lower():
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    if 'amount' in df.columns:
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    
    dtype = {'amount': DECIMAL(10, 2)}

    df.to_sql(name=name, con=con, schema=schema, if_exists=if_exists, index=index, dtype=dtype)

text_csv_2_sql('transactions_01032021.txt', 'stg_new_rows_transactions')
text_csv_2_sql('terminals_01032021.xlsx', 'stg_terminals')
text_csv_2_sql('passport_blacklist_01032021.xlsx', 'stg_passport_blacklist')

#Создание исторических таблиц
##################################################################################################################################################################

def create_hist_passport_blacklist(date):
    cursor.execute(
        '''
        create table if not exists DWH_DIM_PASSPORT_BLACKLIST_HIST(
        id serial primary key,
        effective_from timestamp default to_timestamp(%s, 'YYYY-MM-DD'),
        effective_to timestamp default (to_timestamp('2999-12-31', 'YYYY-MM-DD')),
        delete_flg integer default 0,
        passport varchar(128)
        )
        ''', [date]
    )
    conn.commit()
    
def create_hist_terminals(date):
    cursor.execute(
        '''
        create table if not exists DWH_DIM_TERMINALS_HIST(
        id serial primary key,
        terminal_id varchar(128),
        terminal_type varchar(128),
        terminal_city varchar(128),
        terminal_address varchar(128),
        effective_from timestamp default to_timestamp(%s, 'YYYY-MM-DD'),
        effective_to timestamp default (to_timestamp('2999-12-31', 'YYYY-MM-DD')),
        delete_flg integer default 0
        )
        ''', [date]
    )
    conn.commit()

def create_hist_transactions():
    cursor.execute(
        '''
        create table if not exists DWH_DIM_TRANSACTIONS_HIST(
		transaction_id int8,
		transaction_date timestamp,
		amount numeric(10, 2),
		card_num varchar(128),
		oper_type varchar(128),
		oper_result varchar(128),
		terminal varchar(128)
        )
        '''
    )
    conn.commit()

date = '2021-03-01 00:00:00'
create_hist_passport_blacklist(date)
create_hist_terminals(date)
create_hist_transactions()

#Создание временных таблиц для новых строк
##################################################################################################################################################################

def create_new_rows_terminals():

    cursor.execute('drop table if exists stg_new_rows_terminals;')
    conn.commit()

    cursor.execute(
        '''
        create table stg_new_rows_terminals as
            select t1.terminal_id,
                   t1.terminal_type,
                   t1.terminal_city,
                   t1.terminal_address
            from stg_terminals as t1
            left join DWH_DIM_TERMINALS_HIST as t2
            on t1.terminal_id = t2.terminal_id
            where t2.terminal_id is null
        ''')
    conn.commit()

def create_new_rows_passport_blacklist():

    cursor.execute('drop table if exists stg_new_rows_passport_blacklist')

    cursor.execute(
        '''
        create table stg_new_rows_passport_blacklist as
            select t1.date,
                   t1.passport
            from stg_passport_blacklist as t1
            left join DWH_DIM_PASSPORT_BLACKLIST_HIST as t2
            on t1.passport = t2.passport
            where t2.passport is null
        ''')
    conn.commit()

create_new_rows_terminals()
create_new_rows_passport_blacklist()

#Создание временных таблиц для удаленных строк
##################################################################################################################################################################

def create_deleted_rows_terminals():

    cursor.execute('drop table if exists stg_deleted_rows_terminals;')
    conn.commit()

    cursor.execute(
        '''
        create table stg_deleted_rows_terminals as
            select t2.terminal_id,
                   t2.terminal_type,
                   t2.terminal_city,
                   t2.terminal_address
            from stg_terminals as t1
            right join DWH_DIM_TERMINALS_HIST as t2
            on t1.terminal_id = t2.terminal_id
            where t1.terminal_id is null
        ''')
    conn.commit()

def create_deleted_rows_passport_blacklist():

    cursor.execute('drop table if exists stg_deleted_rows_passport_blacklist')

    cursor.execute(
        '''
        create table stg_deleted_rows_passport_blacklist as
            select t2.effective_from,
                   t2.passport
            from stg_passport_blacklist as t1
            right join DWH_DIM_PASSPORT_BLACKLIST_HIST as t2
            on t1.passport = t2.passport
            where t1.passport is null
        ''')
    conn.commit()

create_deleted_rows_terminals()
create_deleted_rows_passport_blacklist()

#Создание временных таблиц для измененных строк
##################################################################################################################################################################

def create_updated_rows_terminals():

    cursor.execute('drop table if exists stg_updated_rows_terminals;')
    conn.commit()

    cursor.execute(
        '''
        create table stg_updated_rows_terminals as
            select t1.terminal_id,
                   t1.terminal_type,
                   t1.terminal_city,
                   t1.terminal_address
            from stg_terminals as t1
            inner join DWH_DIM_TERMINALS_HIST as t2
            on t1.terminal_id = t2.terminal_id
            and (t1.terminal_type <> t2.terminal_type or
                 t1.terminal_city <> t2.terminal_city or
                 t1.terminal_address <> t2.terminal_address)
        ''')
    conn.commit()

'''
Будем считать, что обновленнных строк в загружаемом черном списке паспортов нет,
т.е. если вдруг пользователь поменяет паспорт, отдел 'черного списка паспортов'
просто добавит новый паспорт данного гражданина в очередной выгрузке, а старый, например, удалит.
Поэтому в исторической таблице мы закроем запись о старом паспорте и добавим новый в рамках
той логики, которая у нас уже есть.
'''

create_updated_rows_terminals()

#Изменение исторических таблиц
##################################################################################################################################################################

def update_transactions_hist_table():

    cursor.execute(
                '''
                insert into DWH_DIM_TRANSACTIONS_HIST
                select * from stg_new_rows_transactions;
                ''')
    conn.commit()

def update_terminals_hist_table():

    cursor.execute(
                '''
                insert into DWH_DIM_TERMINALS_HIST (
                terminal_id,
                terminal_type,
                terminal_city,
                terminal_address,
                effective_from
                )
                select terminal_id,
                terminal_type,
                terminal_city,
                terminal_address,
                %s 
                from stg_new_rows_terminals;
                ''', [date])
    conn.commit()

def update_passport_blacklist_hist_table():

    cursor.execute(
                '''
                insert into DWH_DIM_PASSPORT_BLACKLIST_HIST (passport, effective_from)
                select passport, date
                from stg_new_rows_passport_blacklist;
                ''')
    conn.commit()

update_transactions_hist_table()
update_terminals_hist_table()
update_passport_blacklist_hist_table()