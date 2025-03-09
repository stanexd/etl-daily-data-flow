import psycopg2
import json
import shutil
import pandas as pd
from pathlib import Path
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

#получаем дату входящих файлов
filename = list(Path('incoming_files').iterdir())[0].name
dot_index = filename.find('.')
date_string = filename[dot_index - 8:dot_index]
year = date_string[-4:]
month = date_string[-6:-4]
day = date_string[-8:-6]

date = f"{year}-{month}-{day} 00:00:00"

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

#Загрузка данных в STG
#(для корректной работы скрипта, перед запуском в incoming_files необходимо загружать по три входящих файла,
#после чего они будут отправляться в archive)
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

txt_xlsx_2_sql(f'incoming_files/transactions_{date_string}.txt', 'stg_new_rows_transactions')
txt_xlsx_2_sql(f'incoming_files/terminals_{date_string}.xlsx', 'stg_terminals')
txt_xlsx_2_sql(f'incoming_files/passport_blacklist_{date_string}.xlsx', 'stg_passport_blacklist')

#Перенос файлов в архив (не добавлял расширение backup, чтобы при отладке проще было их перебрасывать снова в каталог incoming_files)
##################################################################################################################################################################

for file_path in Path('incoming_files').iterdir():
    destination_path = Path('archive') / file_path.name
    shutil.move(str(file_path), str(destination_path)) 

#Создание исторических таблиц
##################################################################################################################################################################

def create_hist_passport_blacklist(date):
    
    cursor.execute(
        '''
        create table if not exists DWH_DIM_PASSPORT_BLACKLIST_HIST(
            id serial primary key,
            effective_from timestamp default to_timestamp(%s, 'YYYY-MM-DD'),
            effective_to timestamp default (to_timestamp('2999-12-31', 'YYYY-MM-DD')),
            deleted_flg integer default 0,
            passport varchar(128)
        )
        ''', [date])
    conn.commit()

    cursor.execute('drop view if exists v_passport_blacklist')
    conn.commit()

    cursor.execute(
        '''
        create view v_passport_blacklist as 
            select 
                effective_from, 
                passport
            from DWH_DIM_PASSPORT_BLACKLIST_HIST
            where effective_to = to_timestamp('2999-12-31', 'YYYY-MM-DD')
            and deleted_flg = 0
        ''')
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
            deleted_flg integer default 0
        )
        ''', [date])
    conn.commit()

    cursor.execute('drop view if exists v_terminals')
    conn.commit()

    cursor.execute(
        '''
        create view v_terminals as 
            select 
                terminal_id,
                terminal_type,
                terminal_city,
                terminal_address
            from DWH_DIM_TERMINALS_HIST
            where effective_to = to_timestamp('2999-12-31', 'YYYY-MM-DD')
            and deleted_flg = 0
        ''')
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
        ''')
    conn.commit()

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
            select
                t1.terminal_id,
                t1.terminal_type,
                t1.terminal_city,
                t1.terminal_address
            from stg_terminals as t1
            left join v_terminals as t2
            on t1.terminal_id = t2.terminal_id
            where t2.terminal_id is null
        ''')
    conn.commit()

def create_new_rows_passport_blacklist():

    cursor.execute('drop table if exists stg_new_rows_passport_blacklist')
    conn.commit()
    
    cursor.execute(
        '''
        create table stg_new_rows_passport_blacklist as
            select
                t1.date,
                t1.passport
            from stg_passport_blacklist as t1
            left join v_passport_blacklist as t2
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
            select
                t2.terminal_id,
                t2.terminal_type,
                t2.terminal_city,
                t2.terminal_address
            from stg_terminals as t1
            right join v_terminals as t2
            on t1.terminal_id = t2.terminal_id
            where t1.terminal_id is null
        ''')
    conn.commit()

def create_deleted_rows_passport_blacklist():

    cursor.execute('drop table if exists stg_deleted_rows_passport_blacklist')
    conn.commit()

    cursor.execute(
        '''
        create table stg_deleted_rows_passport_blacklist as
            select
                t2.effective_from,
                t2.passport
            from stg_passport_blacklist as t1
            right join v_passport_blacklist as t2
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
            select
                t1.terminal_id,
                t1.terminal_type,
                t1.terminal_city,
                t1.terminal_address
            from stg_terminals as t1
            inner join v_terminals as t2
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

    #просто добавляем в историческую таблицу новые записи
    cursor.execute(
        '''
        insert into DWH_DIM_TRANSACTIONS_HIST
        select 
            t2.transaction_id,
            t2.transaction_date,
            t2.amount,
            t2.card_num,
            t2.oper_type,
            t2.oper_result,
            t2.terminal
        from DWH_DIM_TRANSACTIONS_HIST as t1
        right join stg_new_rows_transactions as t2
        on t1.transaction_id = t2.transaction_id
        where t1.transaction_id is null
        ''')
conn.commit()

def update_terminals_hist_table():

    #добавляем в историческую таблицу новые записи
    cursor.execute(
        '''
        insert into DWH_DIM_TERMINALS_HIST (
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            effective_from
        )
        select 
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            %s 
        from stg_new_rows_terminals;
        ''', [date])
    conn.commit()

    #закрываем end_dttm у старых записей
    cursor.execute(
        '''
        update DWH_DIM_TERMINALS_HIST
        set effective_to = date_trunc('second', %s::date - interval '1 second')
        where terminal_id in (select terminal_id from stg_updated_rows_terminals)
        ''', [date])
    conn.commit()
    
    #и добавляем обновленные
    cursor.execute(
        '''
        insert into DWH_DIM_TERMINALS_HIST (
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            effective_from
        )
        select 
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            %s 
        from stg_updated_rows_terminals;
        ''', [date])
    conn.commit()

    #закрываем удаленные записи
    cursor.execute(
        '''
        update DWH_DIM_TERMINALS_HIST
        set effective_to = date_trunc('second', %s::date - interval '1 second')
        where terminal_id in (select terminal_id from stg_deleted_rows_terminals)
        ''', [date])
    conn.commit()

    #и добавляем информацию о том, что они удалены
    cursor.execute(
        '''
        insert into DWH_DIM_TERMINALS_HIST (
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            effective_from,
            deleted_flg
        )
        select 
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            %s, 
            1 
        from stg_deleted_rows_terminals;
        ''', [date])
    conn.commit()

def update_passport_blacklist_hist_table():

    #добавляем новые записи
    cursor.execute(
        '''
        insert into DWH_DIM_PASSPORT_BLACKLIST_HIST (passport, effective_from)
        select passport, date
        from stg_new_rows_passport_blacklist;
        ''')
    conn.commit()

   #закрываем end_dttm у удаленых записей
    cursor.execute(
        '''
        update DWH_DIM_PASSPORT_BLACKLIST_HIST
        set effective_to = date_trunc('second', %s::date - interval '1 second')
        where passport in (select passport from stg_deleted_rows_passport_blacklist)
        ''', [date])
    conn.commit()

    #и добавляем информацию о том, что они удалены
    cursor.execute(
        '''
        insert into DWH_DIM_PASSPORT_BLACKLIST_HIST (passport, effective_from, deleted_flg)
        select passport, %s, 1
        from stg_deleted_rows_passport_blacklist;
        ''', [date])
    conn.commit()

update_transactions_hist_table()
update_terminals_hist_table()
update_passport_blacklist_hist_table()

#Создание таблицы с отчетом
##################################################################################################################################################################

def create_rep_fraud():
    
    cursor.execute(
        '''
        create table if not exists REP_FRAUD(
            event_dt date,
            passport varchar(128),
            fio varchar(128),
            phone varchar(128),
            event_type varchar(128),
            report_dt date
        )
        ''')
    conn.commit()

create_rep_fraud()

#Заполнение отчетной таблицы
##################################################################################################################################################################

def update_rep_fraud():
    
    #заполнение просроченными, заблокированными паспортами и недействующими договорами
    cursor.execute(
        '''
        insert into REP_FRAUD (
            event_dt,
            passport,
            fio,
            phone,
            event_type,
            report_dt
        )
        select distinct
            to_char(tr.transaction_date, 'YYYY-MM-DD')::date as event_dt,
            cl.passport_num,
            concat_ws(' ', cl.last_name, cl.first_name, cl.patronymic) as fio,
            cl.phone,
            case
                when cl.passport_valid_to + interval '23 hours 59 minutes 59 seconds' < tr.transaction_date then 'expired passport'
                when vpb.passport is not null then 'passport in black list'
                when a.valid_to < tr.transaction_date then 'invalid contract'
            end as event_type,
            to_char(tr.transaction_date, 'YYYY-MM-DD')::date as report_dt
        from stg_new_rows_transactions as tr
        left join dwh_dim_cards as ca 
        on tr.card_num = ca.card_num
        left join dwh_dim_accounts as a 
        on ca.account = a.account
        left join dwh_dim_clients as cl 
        on a.client = cl.client_id
        left join v_passport_blacklist as vpb 
        on vpb.passport = cl.passport_num
        where
            case
                when cl.passport_valid_to + interval '23 hours 59 minutes 59 seconds' < tr.transaction_date then 'expired passport'
                when vpb.passport is not null then 'passport in black list'
                when a.valid_to < tr.transaction_date then 'invalid contract'
            end is not null;
        ''')
    conn.commit()

    #заполнение мошенниками с операциями из разных городов
    cursor.execute(
        '''
        insert into REP_FRAUD (
            event_dt,
            passport,
            fio,
            phone,
            event_type,
            report_dt
        )
        with hourgroups as (
            select
                transaction_date,
                concat_ws(' ', cl.last_name, cl.first_name, cl.patronymic) as fio,
                terminal_id,
                terminal_city,
                passport_num,
                phone,
                min(transaction_date) over (partition by passport_num, date_trunc('hour', transaction_date)) as hour_group
            from stg_new_rows_transactions as th
            left join v_terminals vt 
            on th.terminal = vt.terminal_id
            left join dwh_dim_cards as ca 
            on th.card_num = ca.card_num
            left join dwh_dim_accounts as a 
            on ca.account = a.account
            left join dwh_dim_clients as cl 
            on a.client = cl.client_id),
        citycounts as (
            select
                passport_num,
                hour_group,
                count(distinct terminal_city) as count_cities
            from hourgroups
            where transaction_date < hour_group + interval '1 hour'
            group by passport_num, hour_group)
        select distinct
            hg.transaction_date::date as event_dt,
            hg.passport_num,
            hg.fio,
            hg.phone,
            'different cities' as event_type,
            hg.transaction_date::date as report_dt
        from hourgroups as hg
        left join citycounts as cc 
        on hg.passport_num = cc.passport_num and hg.hour_group = cc.hour_group
        where coalesce(cc.count_cities, 1) > 1;
        ''')

update_rep_fraud()

#Удаление временных таблиц
##################################################################################################################################################################

def del_stg_tables():
    cursor.execute('''drop table if exists stg_deleted_rows_passport_blacklist''')
    cursor.execute('''drop table if exists stg_deleted_rows_terminals''')
    cursor.execute('''drop table if exists stg_new_rows_passport_blacklist''')
    cursor.execute('''drop table if exists stg_new_rows_terminals''')
    cursor.execute('''drop table if exists stg_new_rows_transactions''')
    cursor.execute('''drop table if exists stg_passport_blacklist''')
    cursor.execute('''drop table if exists stg_terminals''')
    cursor.execute('''drop table if exists stg_updated_rows_terminals''')

    conn.commit()    

del_stg_tables()