import requests
import os
import json


#/home/william/Documentos/ESTUDOS/ALURA-ENGENHARIA_DADOS/datapipeline/ -- Diretório onde ta vitual
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


def auth():
    return os.environ.get("BEARER_TOKEN")

def create_url():
    query = "AluraOnline"
    # Tweet fields are adjustable.
    # Options include:
    # attachments, author_id, context_annotations,
    # conversation_id, created_at, entities, geo, id,
    # in_reply_to_user_id, lang, non_public_metrics, organic_metrics,
    # possibly_sensitive, promoted_metrics, public_metrics, referenced_tweets,
    # source, text, and withheld
    tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,text"
    user_fields = "expansions=author_id&user.fields=id,name,username,created_at"
    filters = "start_time=2022-05-02T00:00:00.00Z&end_time=2022-05-06T00:00:00.00Z"
    url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}&{}&{}".format(
        query, tweet_fields, user_fields, filters
    )
    return url

def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def connect_to_endpoint(url, headers):
    response = requests.request("GET", url, headers=headers)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def paginate(url, headers, next_token=""):
    if next_token:
        full_url = f"{url}&next_token={next_token}"
    else:
        full_url = url
    data = connect_to_endpoint(full_url, headers)
    yield data
    if "next_token" in data.get("meta", {}):
        yield from paginate(url, headers, data['meta']['next_token'])


def main():
    bearer_token = auth()
    url = create_url()
    headers = create_headers(bearer_token)
    for json_response in paginate(url, headers):
        print(json.dumps(json_response, indent=4, sort_keys=True))


if __name__ == "__main__":
    main()
