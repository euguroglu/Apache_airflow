from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.email import EmailOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from datetime import datetime, timedelta
import csv
import requests
import json

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retry": 1,
    "retry_delay": timedelta(minutes=5)
}

#Function to download file and create json file
def download_rates():
    BASE_URL = "https://gist.githubusercontent.com/marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b/raw/"
    ENDPOINTS = {
        'USD': 'api_forex_exchange_usd.json',
        'EUR': 'api_forex_exchange_eur.json'
    }
    with open('/home/enes/airflow2/dags/files/forex_currencies.csv') as forex_currencies:
        reader = csv.DictReader(forex_currencies, delimiter=',')
        for idx, row in enumerate(reader):
            base = row['base']
            with_pairs = row['with_pairs'].split(' ')
            indata = requests.get(f"{BASE_URL}{ENDPOINTS[base]}").json()
            outdata = {'base': base, 'rates': {}, 'last_update': indata['date']}
            for pair in with_pairs:
                outdata['rates'][pair] = indata['rates'][pair]
            with open('/home/enes/airflow2/dags/files/forex_rates.json', 'a') as outfile:
                json.dump(outdata, outfile)
                outfile.write('\n')

def _get_message() -> str:
    return "Hi from forex_data_pipeline"

with DAG("forex_data_pipeline",start_date=datetime(2021, 1, 1),
         schedule_interval="@daily", default_args=default_args, catchup=False) as dag:

         #HTTP sensor example
         is_forex_rates_available = HttpSensor(
            task_id = "is_forex_rates_available",
            http_conn_id = "forex_api",
            endpoint = "marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b",
            response_check = lambda response: "rates" in response.text,
            poke_interval = 5,
            timeout = 20
         )

         #File sensor example
         is_file_available = FileSensor(
            task_id = "is_file_available",
            fs_conn_id = "forex_path",
            filepath = "forex_currencies.csv",
            poke_interval = 5,
            timeout = 20
         )

         #Python operator example
         downloading_rates = PythonOperator(
            task_id = "downloading_rates",
            python_callable = download_rates
         )

         #Bash operator to put file into HDFS
         saving_rates = BashOperator(
            task_id = "saving_rates",
            bash_command = """
                hdfs dfs -mkdir -p /forex && \
                hdfs dfs -put -f /home/enes/airflow2/dags/files/forex_rates.json /forex
            """
         )

         #Hive operator
         creating_forex_rates_table = HiveOperator(
            task_id="creating_forex_rates_table",
            hive_cli_conn_id="hive_conn",
            hql="""
                CREATE EXTERNAL TABLE IF NOT EXISTS forex_rates(
                    base STRING,
                    last_update STRING,
                    eur DOUBLE,
                    usd DOUBLE,
                    nzd DOUBLE,
                    gbp DOUBLE,
                    jpy DOUBLE,
                    cad DOUBLE
                    )
                ROW FORMAT DELIMITED
                FIELDS TERMINATED BY ','
                STORED AS TEXTFILE
            """
         )

         #Spark operator
         forex_processing = SparkSubmitOperator(
            task_id = "forex_processing",
            application = "/home/enes/airflow2/dags/scripts/forex_processing.py",
            conn_id = "spark_conn",
            verbose = False
         )

         #Email operator
         send_email_notification = EmailOperator(
            task_id = "send_email_notification",
            to = "airflow_course@yopmail.com",
            subject = "forex_data_pipeline",
            html_content = "<h3>forex_data_pipeline</h3>"
         )

         #Slack notification operator
         send_slack_notification = SlackWebhookOperator(
            task_id = "send_slack_notification",
            http_conn_id = "slack_conn",
            message = _get_message(),
            channel = "#monitoring"
         )

         #Define dependencies
         is_forex_rates_available >> is_file_available >> downloading_rates >> saving_rates
         saving_rates >> creating_forex_rates_table >> forex_processing >> send_email_notification
         send_email_notification >> send_slack_notification
