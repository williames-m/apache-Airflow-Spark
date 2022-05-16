from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import when, col, sum, to_date, date_format, countDistinct

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("twitter_insight_tweet")\
        .getOrCreate()

    tweet = spark.read.json(
        "/Users/rbottega/Documents/alura/datapipeline/"
        "datalake/silver/twitter_aluraonline/tweet"
    )

    alura = tweet\
        .where("author_id = '1566580880'")\
        .select("author_id", "conversation_id")

    tweet = tweet.alias("tweet")\
        .join(
            alura.alias("alura"),
            [
                alura.author_id != tweet.author_id,
                alura.conversation_id == tweet.conversation_id
            ],
            'left')\
        .withColumn(
            "alura_conversation",
            when(col("alura.conversation_id").isNotNull(), 1).otherwise(0)
    ).withColumn(
            "reply_alura",
            when(col("tweet.in_reply_to_user_id") == '1566580880', 1).otherwise(0)
    ).groupBy(to_date("created_at").alias("created_date"))\
        .agg(
            countDistinct("id").alias("n_tweets"),
            countDistinct("tweet.conversation_id").alias("n_conversation"),
            sum("alura_conversation").alias("alura_conversation"),
            sum("reply_alura").alias("reply_alura")
    ).withColumn("weekday", date_format("created_date", "E"))\

    tweet.coalesce(1)\
        .write\
        .mode("overwrite")\
        .json("/home/william/Documentos/ESTUDOS/ALURA-ENGENHARIA_DADOS/datapipeline/datalake/gold/twitter_insight_tweet")
    
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
