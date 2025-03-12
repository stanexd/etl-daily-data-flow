#Создание временных таблиц для новых строк
##################################################################################################################################################################

def create_new_rows_terminals(conn, cursor):

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

def create_new_rows_passport_blacklist(conn, cursor):

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

#Создание временных таблиц для удаленных строк
##################################################################################################################################################################

def create_deleted_rows_terminals(conn, cursor):

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

def create_deleted_rows_passport_blacklist(conn, cursor):

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

#Создание временных таблиц для измененных строк
##################################################################################################################################################################

def create_updated_rows_terminals(conn, cursor):

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

#Удаление временных таблиц
##################################################################################################################################################################

def del_stg_tables(conn, cursor):
    cursor.execute('''drop table if exists stg_deleted_rows_passport_blacklist''')
    cursor.execute('''drop table if exists stg_deleted_rows_terminals''')
    cursor.execute('''drop table if exists stg_new_rows_passport_blacklist''')
    cursor.execute('''drop table if exists stg_new_rows_terminals''')
    cursor.execute('''drop table if exists stg_new_rows_transactions''')
    cursor.execute('''drop table if exists stg_passport_blacklist''')
    cursor.execute('''drop table if exists stg_terminals''')
    cursor.execute('''drop table if exists stg_updated_rows_terminals''')

    conn.commit()    