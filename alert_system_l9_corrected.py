import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
from io import StringIO
import requests
import pandas as pd
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


my_token = "my_token" #здесь секретный токен бота в кавычках, который отправляет отчет
bot = telegram.Bot(token=my_token)

chat_id = ###### id чата, в который будет отправляться отчет

def ch_get_df(query='Select 1', host='http://clickhouse.lab.karpov.courses:8123', user='student', password='#######'):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result


default_args = {
    'owner': 'e.hozhaj',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime.combine(datetime.now()-timedelta(days=1), datetime.min.time()),
}


schedule_interval = '58 07 * * *' 

    
# статистический подход - методы межквартильного размаха и правило 3 сигм
# методом межквартильного размаха
def check_anomaly_iqr(df, metric, a=4, n=5, upper_threshold=0.3):
    df['q25'] = df[metric].shift(96).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(96).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    df['up'] = df['q75'] + a*df['iqr']
    df['low'] = df['q25'] - a*df['iqr']
    # необходимо сравнить с предыдущим днем (24 часа назад)
    # это 24 * 4 = 96 периодов по 15 минут назад
    df['prev_day'] = df[metric].shift(96)

    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()

    # будет также отображаться информация о том, за какую границу выходят значения
    if df[metric].iloc[-1] < df['low'].iloc[-1] or (df[metric].iloc[-1] > df['up'].iloc[-1] 
                                                and (df[metric].iloc[-1]/df['up'].iloc[-1] - 1) == upper_threshold):
        is_alert = True
    else:
        is_alert = False

    return is_alert, df
    
    # правило сигм

    
def check_sigma(df_2, metric, a=3, n=5, upper_threshold=0.3)):
    df_2['rolling_mean'] = df_2[metric].shift(96).rolling(n, center=True, min_periods=1).mean()
    df_2['sigma'] = df_2[metric].shift(96).rolling(n).std()
    df_2['up_level'] = df_2['rolling_mean'] + a*df_2['sigma']
    df_2['low_level'] = df_2['rolling_mean'] - a*df_2['sigma']

    if df_2[metric].iloc[-1] < df_2['low_level'].iloc[-1] or (df_2[metric].iloc[-1] > df_2['up_level'].iloc[-1]
                                                              and (df_2[metric].iloc[-1]/df_2['up'].iloc[-1] - 1) == upper_threshold):
        is_alert_2 = True
    else:
        is_alert_2 = False

    return is_alert_2, df_2
    
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def alert_report_khozhay():
    
    @task()
    def extract_feed():
        
        query = """SELECT
                        toStartOfFifteenMinutes(time) as ts,
                        toDate(time) AS date,
                        formatDateTime(ts, '%R') as hm,
                        count(DISTINCT user_id) as users_feed,
                        sum(action = 'view') AS views,
                        sum(action = 'like') AS likes,
                        ROUND(sum(action = 'like') / sum(action = 'view') * 100, 2) AS CTR
                    FROM simulator_20260620.feed_actions
                    WHERE time >= today() - 1 and time < toStartOfFifteenMinutes(now())
                    GROUP BY ts, date, hm
                    ORDER BY ts
                    format TSVWithNames"""
        feed_metrics = ch_get_df(query=query)
        return feed_metrics
    
    
    
    @task()
    def extract_messanger():
        
        query = """SELECT
                        toStartOfFifteenMinutes(time) as ts,
                        toDate(time) AS date,
                        formatDateTime(ts, '%R') as hm,
                        count(DISTINCT user_id) as DAU_messanger,
                        count(*) as sent_messages
                    FROM simulator_20260620.message_actions
                    WHERE time >= today() - 1 and time < toStartOfFifteenMinutes(now())
                    GROUP BY ts, date, hm
                    ORDER BY ts
                    format TSVWithNames"""
        messanger_metrics = ch_get_df(query=query)
        return messanger_metrics
    
    @task()
    def transform_data(feed_metrics, messanger_metrics):
        # чтобы преобразовать время в отдельные столбцы, которые сбиваются
        
        feed_metrics = feed_metrics.groupby(['ts', 'date', 'hm'])\
        .max()\
        .reset_index()
        
        messanger_metrics = messanger_metrics.groupby(['ts', 'date', 'hm'])\
        .max()\
        .reset_index()
        
        df_total = feed_metrics.merge(messanger_metrics, how='inner', on=['ts', 'date', 'hm'])
        
        # преобразование в формат datetime
        df_total['ts'] = pd.to_datetime(df_total['ts'])
        df_total['date'] = pd.to_datetime(df_total['date'])
        df_total['hm'] = pd.to_datetime(df_total['hm'])
        
        return df_total
    
    
    @task()
    def run_alerts(df_total, chat=None):
        chat_id = chat or -1002614297220 
        df_total = pd.DataFrame(df_total)
        metrics = ['users_feed', 'views', 'likes', 'CTR', 'DAU_messanger', 'sent_messages']  
        for metric in metrics:
            df = df_total[['ts', metric]].copy()

            # применяю функции распознания случаев алертов
            is_alert, df = check_anomaly_iqr(df, metric)
            
            if df[metric].iloc[-1] < df['low'].iloc[-1]:
                text = "Нарушение по нижней границе"
                diff = df[metric].iloc[-1] - df['low'].iloc[-1]
                
            else:
                text = "Превышение по верхней границе"
                diff = df[metric].iloc[-1] - df['up'].iloc[-1]
                   
            is_alert_2, df_2 = check_sigma(df, metric)
            
            if df_2[metric].iloc[-1] < df_2['low_level'].iloc[-1]:
                text_sigma = "Нарушение по нижней границе"
                diff_sigma = df_2[metric].iloc[-1] - df_2['low_level'].iloc[-1]
            else:
                text_sigma = "Превышение по верхней границе"
                diff_sigma = df_2[metric].iloc[-1] - df_2['up_level'].iloc[-1]
                
            current = df[metric].iloc[-1]
            yesterday = df['prev_day'].iloc[-1]

            if is_alert == True:
                msg = (f"Метрика {metric}:\n"
                f"- Текущее значение {current}. Отклонение более {(1-current/yesterday):.2f}% по сравнению с этим период в предыдущий день\n"
                f"- Alert: выход за границы межквартильного размаха. Характер нарушения: {text}, величина различия: {diff}\n")

                if is_alert == True and is_alert_2 == True:
                    msg += f"- Alert: выход за границы 3 сигм. Характер нарушения: {text_sigma}, величина различия: {diff_sigma}\n"

                msg += f"- Ссылка на дашборд:[https://superset.lab.karpov.courses/superset/dashboard/8936/]"
                
                print(msg) # вместо отправки в тг
                # bot.sendMessage(chat_id=chat_id, text=msg_report) - для отправки в тг

                # графики по квартилям
                plt.figure(figsize=(16, 10))
                plt.tight_layout()
                ax = sns.lineplot(x=df['ts'], y=df[metric], label=metric)
                ax = sns.lineplot(x=df['ts'], y=df['up'], label='upper', color='g')
                ax = sns.lineplot(x=df['ts'], y=df['low'], label='lower', color='r')

                for ind, label in enumerate(ax.get_xticklabels()):
                    if ind % 2 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)
                ax.set(xlabel='time')
                ax.set(ylabel=metric)
                ax.set(ylim=(0, None))
                ax.set_title(metric)
                ax.grid(True, alpha=0.5)    
                
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'alert_report.png'
        plt.close()
        #bot.sendPhoto(chat_id=chat_id, photo=plot_object) - для отправки в тг

        return plot_object
        
    feed_metrics = extract_feed()
    messanger_metrics = extract_messanger()
    df_total = transform_data(feed_metrics, messanger_metrics)
    run_alerts = run_alerts(df_total)

alert_report_khozhay = alert_report_khozhay()
