from datetime import datetime as dt
import pandas as pd
import os
import csv
import mysql.connector

dbname = os.environ.get('DB_NAME')
dbuser = os.environ.get('DB_USER')
dbpassword = os.environ.get('DB_PASSWORD')
dbhost = os.environ.get('DB_HOST')
dbport = os.environ.get('DB_PORT')

conn = mysql.connector.connect(dbname=dbname, user=dbuser, password=dbpassword, host=dbhost, port=dbport)
cur = conn.cursor()

def fetch_data():
    with open('query.sql', 'r') as f:
        sql = f.read()
    cur.execute(sql)
    rows = cur.fetchall()

    with open(f'{stats_path}{curr_time.strftime("%d-%m-%Y").lower()}' , 'w') as f:
        writer = csv.DictWriter(f)
        writer.writerows(rows)
    
    project_labels = {
        'Wp': 'Wikipedia',
        'Wt': 'Wiktionary',
        'Wq': 'Wikiquote',
        'Wb': 'Wikibooks',
        'Wy': 'Wikivoyage',
        'Wn': 'Wikinews'
    }

    stats_path = 'stats/'
    curr_time = dt.now()
    curr_file_path = f'{stats_path}{curr_time.strftime("%d-%m-%Y").lower()}.tsv'
    df = pd.read_csv(curr_file_path, sep='\t')

    df['Project'] = df.prefix.apply(lambda x:project_labels[x.split('/')[0]])
    df['Language Code'] = df.prefix.apply(lambda x:x.split('/')[1])
    df.drop('prefix', axis=1, inplace=True)

    column_labels = {
        'edit_count': 'Edits (all time)',
        'actor_count': 'Editors (all time)',
        'pages_count': 'Pages (all time)',
        'bytes_removed_30D': 'Bytes removed (previous month)',
        'bytes_added_30D':  'Bytes added (previous month)',
        'avg_edits_3M': 'Average Edits per Month',
        'avg_editors_3M': 'Average Editors per Month'}

    df.rename(column_labels, axis=1, inplace=True)

    df = df[['Project', 'Language Code', 
            'Average Edits per Month', 'Average Editors per Month', 
            'Edits (all time)', 'Editors (all time)', 
            'Pages (all time)', 'Bytes added (previous month)',
            'Bytes removed (previous month)']]

    df.to_csv(curr_file_path, sep='\t', index=False)