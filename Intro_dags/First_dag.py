import requests
import pandas as pd
import re
import numpy as np
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

def get_data():
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def top_domains():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE,names=['rank', 'domain'])
    df["3_letters"] = df["domain"].apply(lambda x : re.findall(r'\..*',x))
    df["3_letters"] = df["3_letters"].apply(', '.join)
    df["3_letters"] = df["3_letters"].astype("str")
    top_domains10 = df.groupby("3_letters",as_index = False).agg({"domain":"count"})\
    .sort_values("domain",ascending=False).head(10)
    with open('top_domains10.csv', 'w') as f:
        f.write(top_domains10.to_csv(index=False, header=False))

def max_length():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE,names=['rank', 'domain'])
    lengths = df["domain"].str.len()
    argmax = np.where(lengths == lengths.max())[0]
    max_length = df.iloc[argmax]
    with open('max_length.csv', 'w') as f:
        f.write(max_length.to_csv(index=False, header=False))
        
        
def airflow():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE,names=['rank', 'domain'])
    if len(df.query("domain == 'airflow.com'")) !=1:
        answer = "Такого домена не существует"
    else:
        answer = df.query("domain == 'airflow.com'")["rank"]
    with open('airflow_answer.txt', 'w') as f:
        f.write(answer)   
        
        
def print_data(ds):
    with open('top_domains10.csv', 'r') as f:
        zone_data = f.read()
    with open('max_lenght.csv', 'r') as f:
        lenght_data = f.read()
    with open('airflow_answer.txt', 'r') as f:
        position_data = f.read()   
    date = ds

    print(f'Top domains zones for date {date}')
    print(zone_data)

    print(f'Top domain name lenght for date {date}')
    print(lenght_data)
    
    print(f'For {date}')
    print(position_data)

    
default_args = {
    'owner': 'v-saharov-20',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'start_date': datetime(2022, 6, 2),
}
schedule_interval = '0 12 * * *'
dag = DAG('v-saharov-20', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)
                    
t2 = PythonOperator(task_id='top_domains',
                    python_callable=top_domains,
                    dag=dag)

t3 = PythonOperator(task_id='max_length',
                    python_callable=max_length,
                    dag=dag)

t4 = PythonOperator(task_id='airflow',
                    python_callable=airflow,
                    dag=dag)  

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5


