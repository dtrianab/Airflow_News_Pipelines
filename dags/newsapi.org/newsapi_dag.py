
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

def call_news_api(word_lookup,page,begin_date):
    # API access
    url_newsapi = 'https://newsapi.org/v2/everything?'
    api_key_newsapi='bd1be1e252e344fbbcff52479eeece88'    

    parameters_headlines = {
        'q': word_lookup,
        'sortBy':'popularity',
        'pageSize': 100,
        'apiKey': api_key_newsapi,
        'language': 'en',
        'from' : begin_date,
        'page':page
    }   
    response_headline = requests.get(url_newsapi, params = parameters_headlines)
    response_json_headline = response_headline.json()  
    
    if(response_json_headline['status']=="ok"):
        print("Retrived news successfully from newsapi.org from "+str(begin_date) + "page #" + str(page))
        return response_json_headline
    else:
        print("Error, retrieved " + str(response_headline))
        return None

#Store json file
def store_file(file_name, data):
    print("Store data to json file "+ file_name)
    file_store = os.path.join(os.path.dirname(__file__),"json_cache",file_name)
    with open(file_store+".json", 'w', encoding='utf-8') as f:
         json.dump(data, f, ensure_ascii=False, indent=4)

# define the function for downloading data from API
def _find_news(word_lookup):
    # Limited to 1000 Api calls : need to track amount used
    max =1000
    page=1
    start_date=date.today() 
    delta_date=timedelta(days=30)
    begin_date=start_date - delta_date

    # making first API call, 3 tries
    t=3
    response_json_headline = {}
    
    while(t>0):
        response_json_headline = call_news_api(word_lookup, page, begin_date)
        if(response_json_headline != None):    
            t=0 # To break loop 3 tries
            #Store first file            
            #file_name = "".join(word_lookup) + "_from_" + str(begin_date.strftime('%Y-%m-%d')) + "_page_" + str(page)     
            #store_file(file_name, response_json_headline)    
        else:
            print("Error during first API call, call can not be completed, try # "+str(t))
            t-=1
    
    ## Total number of calls to complete
    ## Full amount >> total_pages= round(response_json_headline['totalResults']/100)
    # total_pages= 4
    # while(page < total_pages):
    #     page+=1        
    #     response_json_headline = {}
    #     # 3 tries
    #     t=3
    #     while(t>0):
    #         response_json_headline = call_news_api(word_lookup, page, begin_date)
    #         if(response_json_headline != None):    
    #             t=0 # To break loop 3 tries
    #             #Store first file            
    #             file_name = "".join(word_lookup) + "_from_" + str(begin_date.strftime('%Y-%m-%d')) + "_page_" + str(page)     
    #             store_file(file_name, response_json_headline)    
    #         else:
    #             print("Error during subsequest API call, call can not be completed, try # "+str(t))
    #             t-=1

    #     print(file_name)
        
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
    op_kwargs = {"word_lookup" : "coronavirus"},
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
    dag=dag,
    )
t4    


t1>>t2>>t3>>t4