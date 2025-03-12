#Создание исторических таблиц
##################################################################################################################################################################

def create_hist_passport_blacklist(conn, cursor, date):
    
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
    
def create_hist_terminals(conn, cursor, date):
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

def create_hist_transactions(conn, cursor):
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

#Изменение исторических таблиц
##################################################################################################################################################################

def update_transactions_hist_table(conn, cursor):

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

def update_terminals_hist_table(conn, cursor, date):

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

def update_passport_blacklist_hist_table(conn, cursor, date):

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
