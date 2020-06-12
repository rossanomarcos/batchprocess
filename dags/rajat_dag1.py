import boto3, json, pprint, requests, textwrap, time, logging, requests

import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


dag = DAG('rajat1_movielens', schedule_interval=None, start_date=datetime.datetime.now())


# Creates an interactive scala spark session.
# Python(kind=pyspark), R(kind=sparkr) and SQL(kind=sql) spark sessions can also be created by changing the value of kind.
def create_spark_session(master_dns, kind='spark'):
    # 8998 is the port on which the Livy server runs
    host = 'http://' + master_dns + ':8998'
    data = {'kind': kind}
    headers = {'Content-Type': 'application/json'}
    response = requests.post(host + '/sessions', data=json.dumps(data), headers=headers)
    logging.info(response.json())
    return response.headers


def wait_for_idle_session(master_dns, response_headers):
    # wait for the session to be idle or ready for job submission
    status = ''
    host = 'http://' + master_dns + ':8998'
    session_url = host + response_headers['location']
    while status != 'idle':
        time.sleep(3)
        status_response = requests.get(session_url, headers=response_headers)
        status = status_response.json()['state']
        logging.info('Session status: ' + status)
    return session_url

def kill_spark_session(session_url):
    requests.delete(session_url, headers={'Content-Type': 'application/json'})

# Submits the scala code as a simple JSON command to the Livy server
def submit_statement(session_url, statement_path):
    statements_url = session_url + '/statements'
    with open(statement_path, 'r') as f:
        code = f.read()
    data = {'code': code}
    response = requests.post(statements_url, data=json.dumps(data), headers={'Content-Type': 'application/json'})
    logging.info(response.json())
    return response

# Function to help track the progress of the scala code submitted to Apache Livy
def track_statement_progress(master_dns, response_headers):
    statement_status = ''
    host = 'http://' + master_dns + ':8998'
    session_url = host + response_headers['location'].split('/statements', 1)[0]
    # Poll the status of the submitted scala code
    while statement_status != 'available':
        # If a statement takes longer than a few milliseconds to execute, Livy returns early and provides a statement URL that can be polled until it is complete:
        statement_url = host + response_headers['location']
        statement_response = requests.get(statement_url, headers={'Content-Type': 'application/json'})
        statement_status = statement_response.json()['state']
        logging.info('Statement status: ' + statement_status)

        #logging the logs
        lines = requests.get(session_url + '/log', headers={'Content-Type': 'application/json'}).json()['log']
        for line in lines:
            logging.info(line)

        if 'progress' in statement_response.json():
            logging.info('Progress: ' + str(statement_response.json()['progress']))
        time.sleep(10)
    final_statement_status = statement_response.json()['output']['status']
    if final_statement_status == 'error':
        logging.info('Statement exception: ' + statement_response.json()['output']['evalue'])
        for trace in statement_response.json()['output']['traceback']:
            logging.info(trace)
        raise ValueError('Final Statement Status: ' + final_statement_status)
    logging.info('Final Statement Status: ' + final_statement_status)



# Converts each of the movielens datafile to parquet
def transform_movies_to_parquet(**kwargs):
    cluster_dns = 'ec2-3-87-47-88.compute-1.amazonaws.com'
    headers = create_spark_session(cluster_dns, 'spark')
    session_url = wait_for_idle_session(cluster_dns, headers)
    statement_response = submit_statement(session_url,'/root/airflow/dags/transform/movies.scala')
    track_statement_progress(cluster_dns, statement_response.headers)
    kill_spark_session(session_url)

def transform_tags_to_parquet(**kwargs):
    cluster_dns = 'ec2-3-87-47-88.compute-1.amazonaws.com'
    headers = create_spark_session(cluster_dns, 'spark')
    session_url = wait_for_idle_session(cluster_dns, headers)
    statement_response = submit_statement(session_url,'/root/airflow/dags/transform/tags.scala')
    track_statement_progress(cluster_dns, statement_response.headers)
    kill_spark_session(session_url)

def transform_ratings_to_parquet(**kwargs):
    cluster_dns = 'ec2-3-87-47-88.compute-1.amazonaws.com'
    headers = create_spark_session(cluster_dns, 'spark')
    session_url = wait_for_idle_session(cluster_dns, headers)
    statement_response = submit_statement(session_url,'/root/airflow/dags/transform/ratings.scala')
    track_statement_progress(cluster_dns, statement_response.headers)
    kill_spark_session(session_url)

def transform_links_to_parquet(**kwargs):
    cluster_dns = 'ec2-3-87-47-88.compute-1.amazonaws.com'
    headers = create_spark_session(cluster_dns, 'spark')
    session_url = wait_for_idle_session(cluster_dns, headers)
    statement_response = submit_statement(session_url,'/root/airflow/dags/transform/links.scala')
    track_statement_progress(cluster_dns, statement_response.headers)
    kill_spark_session(session_url)

transform_movies = PythonOperator(
    task_id='transform_movies',
    python_callable=transform_movies_to_parquet,
    dag=dag)

transform_ratings = PythonOperator(
    task_id='transform_ratings',
    python_callable=transform_ratings_to_parquet,
    dag=dag)

transform_tags = PythonOperator(
    task_id='transform_tags',
    python_callable=transform_tags_to_parquet,
    dag=dag)

transform_links = PythonOperator(
    task_id='transform_links',
    python_callable=transform_links_to_parquet,
    dag=dag)    

transform_movies