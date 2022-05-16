from operators.twitter_operator import TwitterOperator
from airflow.plugins_manager import AirflowPlugin

from hooks.twitter_hook import TwitterHook

class AluraAirflowPlugin(AirflowPlugin):
    name = 'alura'
    operators = [TwitterOperator]
    # hooks = [TwitterHook]


#/home/william/Documentos/ESTUDOS/ALURA-ENGENHARIA_DADOS/datapipeline/ -- Diretório onde ta vitual
#python3 - m venv .env
#source .env/bin/activate
#export AIRFLOW_HOME=$(pwd)/airflow
#pip install apache-airflow==1.10.14 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-1.10.14/constraints-3.7.txt"
#airflow initdb
#airflow webserver

#-- Outro terminal
#airflow scheduler

# airflow/plugins/operators
#export BEARER_TOKEN=AAAAAAAAAAAAAAAAAAAAAHUEYQEAAAAAjQdylpkuRxSGst860iNnYKbfzuw%3DqWyk9fitbG8udwp4uUeVULFu0i9ELt5ikBzwDW22lejGr9bgOC


#./bin/spark-submit -c spark.driver.bindAddress=127.0.0.1 examples/src/main/python/pi.py 10
#pyspark - c spark.driver.bindAddress = 127.0.0.1
