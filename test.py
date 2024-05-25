from datetime import datetime as dt
import pandas as pd
import os
import csv

csv_data = [("Wy/cs",4925,178,26234.0,-569.0,735,311,7), ("Wn/shn",4484,11,403697.0,-9295.0,2340,351,6), ("Wp/btm",6110,195,1294.0,-35.0,2480,72,5)]

csv_headers = ("prefix","edit_count","actor_count","bytes_added_30D","bytes_removed_30D","pages_count","avg_edits_3M","avg_editors_3M")

stats_path = 'stats/'
curr_time = dt.now()
curr_file_path = f'{stats_path}{curr_time.strftime("%d-%m-%Y").lower()}.tsv'

f = open(curr_file_path, 'w+')
writer = csv.writer(f)
writer.writerow(csv_headers)
writer.writerows(csv_data)
f.close()

project_labels = {
    'Wp': 'Wikipedia',
    'Wt': 'Wiktionary',
    'Wq': 'Wikiquote',
    'Wb': 'Wikibooks',
    'Wy': 'Wikivoyage',
    'Wn': 'Wikinews'}

df = pd.read_csv(curr_file_path, sep=',')

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