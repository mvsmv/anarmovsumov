import pandas as pd
import numpy as np 
import pandahouse
import datetime
from datetime import datetime, timedelta

import matplotlib.pyplot as plt
import seaborn as sns

import telegram
import io

# устанавливаем соеднинение с базой данных

connection = {
    'host': '___',
    'password': '___',
    'user': '___',
    'database': '___'
    }

# задаем бота, дату и chat_id

bot = telegram.Bot(token='___')
date = pd.to_datetime('today').date() - pd.to_timedelta('1 day')
chat_id = ___

# создаем таблицы для графиков в отчет

q = ''' SELECT toDate(time) as date, count(distinct user_id) as DAU 
            FROM {db}.feed_actions 
            WHERE toDate(time) BETWEEN today() - 7 AND today() -1
            GROUP BY toDate(time)'''
dau_7 = pandahouse.read_clickhouse(q, connection=connection)

q = ''' SELECT toDate(time) as date, count(action) as views
                FROM {db}.feed_actions 
                WHERE toDate(time) BETWEEN today() - 7 AND today() -1
                    AND action = 'view'
                GROUP BY toDate(time)'''
views_7 = pandahouse.read_clickhouse(q, connection=connection)

q = ''' SELECT toDate(time) as date, count(action) as likes
                FROM {db}.feed_actions 
                WHERE toDate(time) BETWEEN today() - 7 AND today() -1
                    AND action = 'like'
                GROUP BY toDate(time)'''
likes_7 = pandahouse.read_clickhouse(q, connection=connection)

q = ''' SELECT toDate(time) as date, countIf(action='like') / countIf(action='view') as conversion
                FROM {db}.feed_actions 
                WHERE toDate(time) BETWEEN today() - 7 AND today() -1
                GROUP BY toDate(time)
                '''
conv_7 = pandahouse.read_clickhouse(q, connection=connection)

# пишем функцию, которая собирает значения метрик и дату, записывает их и отправляет одним сообщением

def test_report(chat_id=chat_id):
    #chat_id = -715060805
    bot = telegram.Bot(token='5479925884:AAFGUrNFnbvfxi0KfEQgT2VrXVPnQk6F9OY')
    #date = pd.to_datetime('today').date() - pd.to_timedelta('1 day')

    q = ''' SELECT toDate(time), count(distinct user_id) as DAU 
            FROM {db}.feed_actions 
            WHERE toDate(time) = today() - 1
            GROUP BY toDate(time)
        '''

    DAU_table = pandahouse.read_clickhouse(q, connection=connection)
    DAU = DAU_table.iloc[0, 1]

    q = ''' SELECT toDate(time), count(action) as views
                FROM {db}.feed_actions 
                WHERE toDate(time) = today() - 1
                    AND action = 'view'
                GROUP BY toDate(time)
        '''

    views_table = pandahouse.read_clickhouse(q, connection=connection)
    views = views_table.iloc[0,1]

    q = ''' SELECT toDate(time), count(action) as likes
                FROM {db}.feed_actions 
                WHERE toDate(time) = today() - 1
                    AND action = 'like'
                GROUP BY toDate(time)
        '''

    likes_table = pandahouse.read_clickhouse(q, connection=connection)
    likes = likes_table.iloc[0,1]

    q = ''' SELECT toDate(time), countIf(action='like') / countIf(action='view') as conversion
                FROM {db}.feed_actions 
                WHERE toDate(time) = today() - 1
                GROUP BY toDate(time)
        '''
    conv_table = pandahouse.read_clickhouse(q, connection=connection)
    conv = conv_table.iloc[0,1]

    msg = f'''
    Metrcis for Date: {date}
    DAU: {DAU}
    Views: {views}
    Likes: {likes}
    Conversion : {round(conv, 2)}'''

    bot.sendMessage(chat_id=chat_id, text = msg)

test_report(chat_id)

# пишем функцию, которая строит графики на основе таблиц и отправляет их в чат

def send_plot(table):
    col=table.columns[1]   
    sns.set_style('whitegrid')
    plt.figure(figsize=(10,6))
    ax = sns.lineplot(data=table, x='date', y=col, palette='Blues', linewidth = 3, color='red')
    sns.despine()
    for _,s in ax.spines.items():
        s.set_linewidth(2.2)
        s.set_color('black')
    plt.title(f'{date} - {col} 7 days'.upper(), y=1.07, fontsize = 16)
    plt.tight_layout()

    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.name = f'{col}-7_days.png'
    plot_object.seek(0)
    plt.close()

    bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    

df_list = [dau_7, views_7, likes_7, conv_7]

df_list = [df.pipe(send_plot) for df in df_list]
