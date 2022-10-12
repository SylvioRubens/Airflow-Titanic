import pandas as pd
from tabulate import tabulate
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

default_args = {
    'owner': 'Sylvio',
    'depends_on_past': False,
    'start_date': datetime(2022, 10, 6)
}

dag2 = DAG(
    'exercício_dag2',
    default_args = default_args,
    description = 'Segunda DAG do exercicio de arquitetura de dados',
    schedule_interval='@once', 
    catchup=False,
    tags = ['PUC', 'Titanic', 'trabalho']
)

def trasnform():
    tabela_indicadores = '/tmp/tabela_unica.csv'
    data = pd.read_csv(tabela_indicadores, sep=';')

    indicadores_media = pd.DataFrame(data[['qtd_passageiros', 'tarifa_media', 'sibsp_parch']].mean()).copy()

    print('\n\nTabela resultado com a média dos indicadores:\n', tabulate(indicadores_media, headers="keys", tablefmt="psql"))
    indicadores_media.to_csv('/tmp/resultados.csv')


transform = PythonOperator(
    task_id = "transformar_dados_dag2",
    python_callable = trasnform,
    dag=dag2
)

fim = DummyOperator(
    task_id="fim_dag_2",
    dag=dag2
)

transform >> fim




