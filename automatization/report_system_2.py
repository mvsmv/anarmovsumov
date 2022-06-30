# импортируем библиотеки

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
    'user': 'student',
    'database': '___'
    }

# пишем функцию, преобразующую запрос к БД в pandas-датафрейм 

def select(sql):
    
    table = pandahouse.read_clickhouse(sql, connection=connection)
    return table

# задаем бота, дату и chat_id

bot = telegram.Bot(token='___')
date = pd.to_datetime('today').date() - pd.to_timedelta('1 day')
chat_id = ___

# собираем метрики


def test_report_2(chat_id=chat_id):
    
    '''
    Dockstring
    
    Функция подключается к базе данных и собирает все метрики
    за предыдущий день и за тот же день на предыдущей неделе, 
    чтобы учесть недельную сезонность. В итоговом сообщении
    значение метрики за предыдущую неделю выводится в скобках.
    '''
    
    # DAU feed
    sql = '''
                SELECT count(distinct user_id)
                FROM {db}.feed_actions
                WHERE toDate(time) = today() - 1
        '''
    dau_feed = select(sql).reset_index().iloc[0,1]
    
    sql = '''
        SELECT count(distinct user_id)
        FROM {db}.feed_actions
        WHERE toDate(time) = today() - 8
        '''
    dau_feed_7 = select(sql).reset_index().iloc[0,1]
    
    # определяем разницу с показателем неделю назад и записываем в отчет с нужным знаком
    if dau_feed < dau_feed_7:
        dau_diff = '-' + str(abs(round(100 - (dau_feed / dau_feed_7) * 100, 2)))
    else:
        dau_diff = '+' + str(abs(round(100 - (dau_feed / dau_feed_7) * 100, 2)))
    
    # DAU messenger
    sql = '''
                SELECT count(distinct user_id)
                FROM {db}.message_actions
                WHERE toDate(time) = today() - 1
        '''
    dau_message = select(sql).reset_index().iloc[0,1]
    sql = '''
        SELECT count(distinct user_id)
        FROM {db}.message_actions
        WHERE toDate(time) = today() - 8
        '''
    dau_message_7 = select(sql).reset_index().iloc[0,1]


    if dau_message < dau_message_7:
        dau_m_diff = '-' + str(abs(round(100 - (dau_message / dau_message_7) * 100, 2)))
    else:
        dau_m_diff = '+' + str(abs(round(100 - (dau_message / dau_message_7) * 100, 2)))

    # DAU feed & messemger
    sql = '''   SELECT uniq(user_id) as user
                FROM {db}.message_actions 
                WHERE user_id IN
                                (SELECT distinct user_id
                                FROM {db}.feed_actions
                                WHERE toDate(time) = today() - 8
                                )
                        AND toDate(time) = today() - 8
        '''
    dau_fm = select(sql).reset_index().iloc[0,1]
    
    sql = '''   SELECT uniq(user_id) as user
                FROM {db}.message_actions 
                WHERE user_id IN
                                (SELECT distinct user_id
                                FROM {db}.feed_actions
                                WHERE toDate(time) = today() - 8
                                )
                        AND toDate(time) = today() - 8
        '''

    dau_fm_7 = select(sql).reset_index().iloc[0,1]


    if dau_fm < dau_fm_7:
        dau_fm_diff = '-' + str(abs(round(100 - (dau_fm / dau_fm_7) * 100, 2)))
    else:
        dau_fm_diff = '+' + str(abs(round(100 - (dau_fm / dau_fm_7) * 100, 2)))
    
    # views total
    sql = '''
                SELECT countIf(action='view') as views
                FROM {db}.feed_actions 
                WHERE toDate(time) = today() - 1
                AND action='view'
        '''
    views = select(sql).reset_index().iloc[0,1]
    sql = ''' 
                SELECT countIf(action='view') as views
                FROM {db}.feed_actions 
                WHERE toDate(time) = today() - 8
                AND action='view'
                
                '''

    views_7 = select(sql).reset_index().iloc[0,1]

    if views < views_7:
        views_diff = '-' + str(abs(round(100 - (views / views_7) * 100, 2)))
    else:
        views_diff = '+' + str(abs(round(100 - (views / views_7) * 100, 2)))
    
    
    # likes total
    sql = '''
                SELECT countIf(action='like') as likes
                FROM {db}.feed_actions 
                WHERE toDate(time) = today() - 1
                AND action='like'
         '''
    likes = select(sql).reset_index().iloc[0,1]
    
    sql = '''     SELECT countIf(action='like') as likes
                FROM {db}.feed_actions 
                WHERE toDate(time) = today() - 8
                AND action='like'
                '''
    likes_7 = select(sql).reset_index().iloc[0,1]

    if likes < likes_7:
        likes_diff = '-' + str(abs(round(100 - (likes / likes_7) * 100, 2)))
    else:
        likes_diff = '+' + str(abs(round(100 - (likes / likes_7) * 100, 2)))
    likes_diff
    
    # messages total
    sql = '''      SELECT count(user_id) as messages
                FROM {db}.message_actions 
                WHERE toDate(time) = today() - 1
                '''
    messages = select(sql).reset_index().iloc[0,1]

    sql = '''      SELECT count(user_id) as messages
                    FROM {db}.message_actions 
                    WHERE toDate(time) = today() - 8
                    '''
    messages_7 = select(sql).reset_index().iloc[0,1]

    if messages < messages_7:
        messages_diff = '-' + str(abs(round(100 - (messages / messages_7) * 100, 2)))
    else:
        messages_diff = '+' + str(abs(round(100 - (messages / messages_7) * 100, 2)))
    messages_diff
    
    # views per user
    sql = '''   SELECT countIf(action='view') / uniq(user_id) as views_per_user
                FROM {db}.feed_actions 
                WHERE toDate(time) = today() - 1
         '''
    vpu = round(select(sql).reset_index().iloc[0,1], 2)
    
    sql = '''      SELECT countIf(action='view') / uniq(user_id) as views_per_user
                FROM {db}.feed_actions 
                WHERE toDate(time) = today() - 8
                '''
    vpu_7 = round(select(sql).reset_index().iloc[0,1], 2)

    if vpu < vpu_7:
        vpu_diff = '-' + str(abs(round(100 - (vpu / vpu_7) * 100, 2)))
    else:
        vpu_diff = '+' + str(abs(round(100 - (vpu / vpu_7) * 100, 2)))
    
    
    # likes per user
    sql = '''   SELECT countIf(action='like') / uniq(user_id) as like_per_user
                FROM {db}.feed_actions 
                WHERE toDate(time) = today() - 1
                '''
    lpu = round(select(sql).reset_index().iloc[0,1], 2)
    
    sql = '''      SELECT countIf(action='like') / uniq(user_id) as views_per_user
                FROM {db}.feed_actions 
                WHERE toDate(time) = today() - 8
                '''
    lpu_7 = round(select(sql).reset_index().iloc[0,1], 2)

    if lpu < lpu_7:
        lpu_diff = '-' + str(abs(round(100 - (lpu / lpu_7) * 100, 2)))
    else:
        lpu_diff = '+' + str(abs(round(100 - (lpu / lpu_7) * 100, 2)))
    
    
    # messages per user
    sql = '''   SELECT count(user_id) / uniq(user_id) as messages_per_user
                FROM {db}.message_actions 
                WHERE toDate(time) = today() - 1
                '''
    mpu = round(select(sql).reset_index().iloc[0,1], 2)
    
    sql = '''      SELECT count(user_id) / uniq(user_id) as messages_per_user
                FROM {db}.message_actions 
                WHERE toDate(time) = today() - 8
                '''
    mpu_7 = round(select(sql).reset_index().iloc[0,1], 2)

    if mpu < mpu_7:
        mpu_diff = '-' + str(abs(round(100 - (mpu / mpu_7) * 100, 2)))
    else:
        mpu_diff = '+' + str(abs(round(100 - (mpu / mpu_7) * 100, 2)))
    mpu_diff

    msg = f'''
    Metrcis for: *{date}* (compared to week ago)
    
    DAU feed: {dau_feed}  ({dau_diff}%)
    DAU msg: {dau_message}  ({dau_m_diff}%)
    DAU feed&msg: {dau_fm}  ({dau_fm_diff}%)
    
    Views: {views}  ({views_diff}%)
    Views per user: {vpu}  ({vpu_diff}%)
    
    Likes: {likes}  ({likes_diff}%)
    Like per user: {lpu}  ({lpu_diff}%)
    
    Messages : {messages}  ({messages_diff}%)
    Msg per user: {mpu}  ({mpu_diff}%)
    
    *Dashboards*:
    Feed main: https://clck.ru/pkinE
    Feed today: https://clck.ru/pkimU
    App total: https://clck.ru/pkijY
    '''

    bot.sendMessage(chat_id=chat_id, text = msg, parse_mode=telegram.ParseMode.MARKDOWN)

# Подготовим таблицы для графических отчетов

sql = '''
        SELECT toDate(time) as date,
               uniq(user_id) as users
        FROM {db}.feed_actions
        WHERE date != today()
        GROUP BY date
        '''

dau_feed = select(sql)
dau_feed['rolling'] = dau_feed['users'].rolling(7).mean()
dau_feed.name = 'dau_feed'

sql = '''
        SELECT toDate(time) as date,
               uniq(user_id) as users
        FROM {db}.message_actions
        WHERE date BETWEEN today() -30 AND today() - 1
        GROUP BY date
        '''
dau_messenger = select(sql)
dau_messenger['rolling'] = dau_messenger['users'].rolling(7).mean()
dau_messenger.name = 'dau_messenger'

sql = '''
        SELECT toDate(time) as date,
               count(action) as actions
        FROM {db}.feed_actions
        WHERE date BETWEEN today() -30 AND today() - 1
        GROUP BY date
        '''
actions_total = select(sql)
actions_total['rolling'] = actions_total['actions'].rolling(7).mean()
actions_total.name = 'actions total'

sql = '''
        SELECT toDate(time) as date,
               count(action) / uniq(user_id) as action_per_user
        FROM {db}.feed_actions
        WHERE date BETWEEN today() -30 AND today() - 1
        GROUP BY date
        '''
action_pu = select(sql)
action_pu['rolling'] = action_pu['action_per_user'].rolling(7).mean()
action_pu.name = 'action per user'

sql = '''      SELECT   toDate(time) as date,
                        uniq(post_id) as posts
                FROM {db}.feed_actions 
                WHERE toDate(time) != today()
                GROUP BY date
                '''
posts_total = select(sql)
posts_total['rolling'] = posts_total['posts'].rolling(7).mean()
posts_total.name = 'posts total'

sql = '''      SELECT   toDate(time) as date,
                        count(post_id) / uniq(user_id) as posts_per_user
                FROM {db}.feed_actions 
                WHERE toDate(time) != today()
                GROUP BY date
                '''
posts_pu = select(sql)
posts_pu['rolling'] = posts_pu['posts_per_user'].rolling(7).mean()
posts_pu.name = 'posts per user'

sql = '''      SELECT   toDate(time) as date,
                        count(user_id) as messages
                FROM {db}.message_actions 
                WHERE toDate(time) != today()
                GROUP BY date
                '''
messages_total = select(sql)
messages_total['rolling'] = messages_total['messages'].rolling(7).mean()
messages_total.name = 'messages total'

sql = '''      SELECT   toDate(time) as date,
                        count(user_id) / uniq(user_id) as messages_per_user
                FROM {db}.message_actions 
                WHERE toDate(time) != today()
                GROUP BY date
                '''
messages_pu = select(sql)
messages_pu['rolling'] = messages_pu['messages_per_user'].rolling(7).mean()
messages_pu.name = 'message per user'

# Напишем функцию, которая создает отчет из двух графиков по двум таблицам и отправляет их в телеграм
def send_plot_ma(table, table2):
    
    plt.figure(figsize= (10,12))

    sns.set_style('whitegrid')
    
    ax = plt.subplot(2, 1, 1)
    g = sns.lineplot(data=table, x='date', y=table.iloc[:,1], linewidth = 2.5, palette='BluesD', label = f'{table.iloc[:,1].name.title().replace("_"," ")}')
    g = sns.lineplot(data=table, x='date', y=table.iloc[:,2], linewidth = 2.5, color='orange', label='7 Day Moving Average')
    plt.title(table.name.upper().replace('_', ' '), y=1.07, fontsize = 16)
    
    ax2 = plt.subplot(2, 1, 2)
    g2 = sns.lineplot(data=table2, x='date', y=table2.iloc[:,1], linewidth = 2.5, palette='BluesD', label = f'{table2.iloc[:,1].name.title().replace("_"," ")}')
    g2 = sns.lineplot(data=table2, x='date', y=table2.iloc[:,2], linewidth = 2.5, color='orange', label='7 Day Moving Average')
    plt.title(table2.name.upper().replace('_', ' '), y=1.07, fontsize = 16)
    
    sns.despine()
           
    for _,s in ax.spines.items():
        s.set_linewidth(2.2)
        s.set_color('black')
        
    for _,s in ax2.spines.items():
        s.set_linewidth(2.2)
        s.set_color('black')
    
    plt.legend()
    plt.tight_layout()
    
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.name = 'dau.png'
    plot_object.seek(0)
    plt.close()

    bot.sendPhoto(chat_id=chat_id, photo=plot_object, caption=f'Report date : {date}')
    

# отправляем отчет в чат в телеграме

test_report_2(chat_id)
send_plot_ma(dau_feed, dau_messenger)
send_plot_ma(actions_total, action_pu)
send_plot_ma(posts_total, posts_pu)
send_plot_ma(messages_total, messages_pu)

# end
