
import json, requests
import os
import pandas as pd
from pandas.core.frame import DataFrame
from datetime import datetime, date, timedelta

from airflow.models.connection import Connection
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import dag, task


# Create connection using airflow env variable created by docker-compose and my_pg_conn > postgres_init.py
c = Connection(
    uri=os.environ["AIRFLOW__CORE__SQL_ALCHEMY_CONN"],
    conn_id="my_pg_conn"
)

@dag(schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False, tags=['newsapi'])
def api_etl_newsapi():
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple ETL data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)
    """
    @task()
    def extract():
        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data
        pipeline. In this case, getting data is simulated by reading from a
        hardcoded JSON string.
        """
        # API access
        url_newsapi = 'https://newsapi.org/v2/everything?'
        api_key_newsapi='bd1be1e252e344fbbcff52479eeece88'
        word_lookup = 'coronavirus'
        from_delta = date.today() - timedelta(days=30)
        parameters_headlines = {
            'q': word_lookup,
            'sortBy':'popularity',
            'pageSize': 100,
            'apiKey': api_key_newsapi,
            'language': 'en',
            'from' : from_delta   
        }
        # making the API call
        try:
            response_headline = requests.get(url_newsapi, params = parameters_headlines)
            print("Retrived news successfully from newsapi.org from "+str(from_delta)+"to"+date.today())
        except:
            print("Error during API call, can not be completed")
        response_json_headline = response_headline.json()  

        return response_json_headline

    @task(multiple_outputs=True)
    def transform(response_json: dict):
        """
        #### Transform task
        A simple Transform task which takes in the collection of order data and
        computes the total order value.
        """
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
            return news_articles_df.to_dict()
        except:
            print("Error reading response_json, articles node not found, json contents unavailable in response")    


    @task()
    def load(data_load: dict):
        """
        #### Load task
        A simple Load task which takes in the result of the Transform task and
        instead of saving it to end user review, just prints it out.
        """
        #data_load.to_sql(name='newsapi', con=c)

        print(f"DF loaded")
        print(data_load)
    order_data = extract()
    order_summary = transform(order_data)
    load(order_summary)
tutorial_etl_dag = api_etl_newsapi()
