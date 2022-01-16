
import json, requests
import os
import pandas as pd
from pandas.core.frame import DataFrame
from datetime import datetime, date, timedelta

from airflow import DAG
from airflow.models.connection import Connection
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


# Create connection using airflow env variable created by docker-compose and my_pg_conn > postgres_init.py
c = Connection(
    uri=os.environ["AIRFLOW__CORE__SQL_ALCHEMY_CONN"],
    conn_id="my_pg_conn"
)

default_args = {
    'owner': 'Diego T',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}


# define the function for downloading data from API
def _find_news(word_lookup):
    #return x + " is a must have tool for Data Engineers."
    # API access
    url_newsapi = 'https://newsapi.org/v2/everything?'
    api_key_newsapi='bd1be1e252e344fbbcff52479eeece88'
    
    # Limited to 1000 Api calls : need to track amount used
    max =1000

    parameters_headlines = {
        'q': word_lookup,
        'sortBy':'popularity',
        'pageSize': 100,
        'apiKey': api_key_newsapi,
        'language': 'en',
        'from' : date.today() - timedelta(days=30)
    }    
    # making the API call
    try:
        response_headline = requests.get(url_newsapi, params = parameters_headlines)
        print("Retrived news successfully from newsapi.org from "+str(from_delta)+"to"+date.today())
    except:
        print("Error during API call, can not be completed")
    response_json_headline = response_headline.json()  
    
    if round(response_json_headline['totalResults']/100) > 0 :
        print("more calls to perform")

    # Return will store into xcom default key='return_value', task_ids='find_news' (ti.xcom_push(key='', value=variable) can be used instead)
    # operator has parm do_xcom_push = 'False'
    return response_json_headline    

def _process(ti):
    #Pull xcom with data from task find_news
    response_json = ti.xcom_pull(key='return_value', task_ids='find_news')
    print("Proccessing data...")

    try:
        #Extract articles
        file = response_json["articles"]
        #Sort article results
        article_results = []
        for i in range(len(file)):
            article_dict = {}
            article_dict['title'] = file[i]['title']
            article_dict['author'] = file[i]['author']
            article_dict['source'] = file[i]['source']
            article_dict['description'] = file[i]['description']
            article_dict['content'] = file[i]['content']
            article_dict['pub_date'] = file[i]['publishedAt']
            article_dict['url'] = file[i]["url"]
            article_dict['photo_url'] = file[i]['urlToImage']
            article_results.append(article_dict)
    # transform the data from JSON dictionary to a pandas data frame
        news_articles_df = pd.DataFrame(article_results)
        print("Data successfully parsed")
        print(news_articles_df)
        return news_articles_df.to_dict()
    except:
        print("Error reading response_json, articles node not found, json contents unavailable in response")    
        return "Error reading response_json, articles node not found, json contents unavailable in response"

def _transform_to_sql(ti):
    print("Transforming  data into sql...")
    data_load = ti.xcom_pull(key='return_value', task_ids='proccess_response')
    n=0
    k=str(n)
    mydict = [  data_load['title'][k] , data_load['author'][k] , data_load['source'][k]['id'] ,data_load['source'][k]['name'], data_load['description'][k] , 
                data_load['content'][k] , data_load['pub_date'][k] , data_load['url'][k] ,data_load['photo_url'][k] 
                ] 
    # Remove ' from values              
    columns = 'title, author, source_id, source_name, description, content, pub_date, url, photo_url'
    values = ', '.join("'" + str(x).replace("'", '') + "'" for x in mydict)
    sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % ('newsapi', columns, values)

    sql2=''
    for n in range(1,100):
        k=str(n)
        mydict = [data_load['title'][k], data_load['author'][k], data_load['source'][k]['id'],
                data_load['source'][k]['name'], data_load['description'][k], data_load['content'][k],
                data_load['pub_date'][k], data_load['url'][k], data_load['photo_url'][k]]
        values2 = ', '.join("'" + str(x).replace("'", '') + "'" for x in mydict)
        sql_k = ",( %s )" % (values2)
        sql2=sql2+sql_k
    
    print(sql2)
    return sql+sql2+';'
        
    

# define the DAG
dag = DAG(
    'api_news_etl_v2',
    default_args=default_args,
    description='Retrieve news based on lookup words from API news provider1',
    schedule_interval=timedelta(days=1),
)

t1 = PythonOperator(
    task_id='find_news',
    python_callable= _find_news,
    op_kwargs = {"x" : "coronavirus"},
    dag=dag,
    )
t1

t2 = PythonOperator(
    task_id='proccess_response',
    python_callable= _process,
    dag=dag,
    )
t2

t3 = PythonOperator(
    task_id='transform_to_sql',
    python_callable= _transform_to_sql,
    dag=dag,
    )
t3

t4 = PostgresOperator(
    task_id="populate_news_api_table",
    postgres_conn_id='my_pg_conn',
    
    sql= '''{{ ti.xcom_pull(task_ids='transform_to_sql', key='return_value') }}'''
    ,
    # """
    # SELECT * FROM newsapi
    # """
    # INSERT INTO newsapi (title, author, source, description, content, pub_date, url, photo_url)
    # VALUES ( 'Test', 'Test', 'Test', 'Test', 'Test', '2018-07-05', 'Test', 'Test');
    dag=dag,
    )
t4    


t1>>t2>>t3>>t4