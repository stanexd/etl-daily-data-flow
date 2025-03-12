#Создание таблицы с отчетом
##################################################################################################################################################################

def create_rep_fraud(conn, cursor):
    
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

#Заполнение отчетной таблицы
##################################################################################################################################################################

def update_rep_fraud(conn, cursor):
    
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
    conn.commit()