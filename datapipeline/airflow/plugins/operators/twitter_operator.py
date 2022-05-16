import json
from datetime import datetime, timedelta
from pathlib import Path
from os.path import join

from airflow.models import BaseOperator, DAG, TaskInstance
from airflow.utils.decorators import apply_defaults

from hooks.twitter_hook import TwitterHook

#/home/william/Documentos/ESTUDOS/ALURA-ENGENHARIA_DADOS/datapipeline/ -- Diretório onde ta vitual
#source .env/bin/activate
#export AIRFLOW_HOME=$(pwd)/airflow
#airflow initdb
#airflow webserver

#-- Outro terminal
#airflow scheduler

# airflow/plugins/operators
#export BEARER_TOKEN=AAAAAAAAAAAAAAAAAAAAAHUEYQEAAAAAjQdylpkuRxSGst860iNnYKbfzuw%3DqWyk9fitbG8udwp4uUeVULFu0i9ELt5ikBzwDW22lejGr9bgOC

#./bin/spark-submit -c spark.driver.bindAddress=127.0.0.1 examples/src/main/python/pi.py 10
#pyspark - c spark.driver.bindAddress = 127.0.0.1


class TwitterOperator(BaseOperator):

    template_fields = [
        "query",
        "file_path",
        "start_time",
        "end_time"
    ]

    @apply_defaults
    def __init__(
        self,
        query,
        file_path,
        conn_id=None,
        start_time=None,
        end_time=None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.query = query
        self.file_path = file_path
        self.conn_id = conn_id
        self.start_time = start_time
        self.end_time = end_time

    def create_parent_folder(self):
        Path(Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)

    def execute(self, context):
        hook = TwitterHook(
            query=self.query,
            conn_id=self.conn_id,
            start_time=self.start_time,
            end_time=self.end_time
        )
        self.create_parent_folder()
        with open(self.file_path, "w") as output_file:
            for pg in hook.run():
                json.dump(pg, output_file, ensure_ascii=False)
                output_file.write("\n")


if __name__ == "__main__":
    with DAG(dag_id="TwitterTest", start_date=datetime.now()) as dag:
        to = TwitterOperator(
            query="AluraOnline",
            file_path=join(
                "/home/william/Documentos/ESTUDOS/ALURA-ENGENHARIA_DADOS/datapipeline/datalake",
                "twitter_aluraonline",
                "extract_date={{ ds }}",
                "AluraOnline_{{ ds_nodash }}.json"
            ),
            task_id="test_run"
        )
        ti = TaskInstance(
            task=to, execution_date=datetime.now() - timedelta(days=1))
        ti.run()
