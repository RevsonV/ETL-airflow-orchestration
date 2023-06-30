from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd


def consume_api():

    """ 
    Extract JSON data by API request,
    Transform to pandas dataframe,
    Load as CSV within a pre defined folder.
    """
    
    base_url = 'https://api.organizze.com.br/rest/v2'
    username = 'my-address@email.com'
    password = 'mypassword'

    # Configure HTTP Basic authentication
    auth = (username, password)

    # Configure the header User-Agent
    user_agent = 'Revson (my-address@email.com)'
    headers = {'User-Agent': user_agent}

    # Base url + endpoint
    endpoint = '/transactions'
    url = base_url + endpoint

    # GET method instantiation
    response = requests.get(url, auth=auth, headers=headers)

    # Checks the response status
    if response.status_code == 200:
        # Successfull request
        data = response.json()
    else:
        # Failed request
        print(f'{response.status_code} error: {response.text}')

    # Transform data response into a pandas dataframe
    df = pd.DataFrame(data)
    df.to_csv('/home/revson/Documents/airflow_organizze/dags/transactions.csv', index=False)


with DAG(
    'organizze_api',
    start_date=datetime(2023, 6, 28),
    schedule_interval='@daily'
) as dag:
    task = PythonOperator(
        task_id='extract_data',
        python_callable=consume_api
    )

    task
