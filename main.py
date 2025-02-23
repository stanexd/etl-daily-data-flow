import psycopg2
import json

#Подключение к БД
#######################################################################################################

with open('config.json', encoding='UTF-8') as f:
    db_config = json.load(f)

conn = psycopg2.connect(**db_config)
cursor = conn.cursor()

#Создание схемы BANK
#######################################################################################################

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