import psycopg2
import json

with open('config.json', encoding='UTF-8') as f:
    db_config = json.load(f)

conn = psycopg2.connect(**db_config)
cursor = conn.cursor()

cursor.execute('drop schema if exists BANK cascade;')
conn.commit()
cursor.execute('create schema if not exists BANK;')
conn.commit()

cursor.execute('set search_path to bank;')

with open('sql_scripts/ddl_dml.sql', 'r', encoding='UTF-8') as f:
    loading_script = f.read()

for part in loading_script.split(';'):
    part = part.strip()
    if part:
        cursor.execute(part)
        conn.commit()