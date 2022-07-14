import pandas as pd
import numpy as np 
import pandahouse
from datetime import date

import matplotlib.pyplot as plt
import seaborn as sns

import sys
import os 

import io
import telegram


chat = ___

connection = {
    'host': '___',
    'password': '___',
    'user': '___',
    'database': '___'
}

# напишем функцию для удобных запросов к базе данных

def select(sql):
    
    table = pandahouse.read_clickhouse(sql, connection=connection)
    return table

# напишем функцию проверки аномалий
  
def check_anomaly(df, metric, a=3, n=5):
    ''' Функция предлагает алгоритм поиска аномалий в данных - межквартильный размах '''
    
    df['q25'] = df[metric].shift().rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift().rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    df['up'] = df['q75'] + a * df['iqr']
    df['low'] = df['q25'] - a * df['iqr']
    
    # сглаживаем границы
    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()
    
    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
    
    return is_alert, df

# напишем систему предупреждений

def run_alerts(chat_id=None):
    ''' Система алертов'''
    
    chat_id = chat_id or 171427171
    bot = telegram.Bot(token='___')
    
    sql = '''
        SELECT  toStartOfFifteenMinutes(time) as ts,
                toDate(ts) as date,
                formatDateTime(ts,'%R') as hm,
                uniqExact(user_id) as users_feed,
                countIf(user_id, action='view') as views,
                countIf(user_id, action='like') as likes
        FROM {db}.feed_actions
        WHERE ts >= today()-1 AND ts < toStartOfFifteenMinutes(now())
        GROUP BY ts, date, hm
        ORDER BY ts
        '''
    data = select(sql)
    
    print(data) 
        
    metrics_list = ['users_feed', 'views', 'likes']
    
    for metric in metrics_list:
        print(metric)
        df = data[['ts', 'date', 'hm', metric]].copy()
        is_alert, df = check_anomaly(df, metric)
        current_val = df[metric].iloc[-1]
        last_val_diff = 1 - df[metric].iloc[-1] / df[metric].iloc[-2]
    
        if is_alert == 1 or True:
            msg = f'''Метрика {metric}:
            Текущее значение: {current_val:.2f}
            Отклонение от предыдущего значения: {last_val_diff:.2%} '''

        sns.set(rc={'figure.figsize' : (16, 10)})
        plt.tight_layout()
        ax = sns.lineplot(x=df['ts'], y=df[metric], label='metric', linewidth=2)
        ax = sns.lineplot(x=df['ts'], y=df['up'], label='up', linestyle='--')
        ax = sns.lineplot(x=df['ts'], y=df['low'], label='low', linestyle='--')

        for ind, label in enumerate(ax.get_xticklabels()):
            if ind % 2 == 0:
                label.set_visible(True)
            else:
                label.set_visible(False)

        ax.set(xlabel='time', ylabel=metric)
        ax.set_title(metric)      
        ax.set(ylim=(0, None))

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.name = '7_days.png'
        plot_object.seek(0)
        plt.close()

        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        bot.sendMessage(chat_id=chat_id, text = msg)
            
    return

run_alerts(chat)
