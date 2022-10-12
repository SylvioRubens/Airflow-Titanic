import pandas as pd
import numpy as np
from tabulate import tabulate
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

url_file = 'https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv'
nome_arquivo = "/tmp/data.csv"

default_args = {
    'owner': 'Sylvio',
    'dependes_on_past': False,
    'start_date': datetime(2022, 10, 6)
}

dag = DAG(
    'exercício_dag1',
    default_args = default_args,
    description = 'Primeira DAG do exercicio de arquitetura de dados',
    schedule_interval='@once', 
    catchup=False,
    tags = ['PUC', 'Titanic', 'trabalho']
)

def load_data():
    data = pd.read_csv(url_file, sep=';')
    data.to_csv(nome_arquivo, index=False, sep=';')
    print("Dado do trabalho salvo!!!")

def calc_and_save_table(df, first_class, second_class, column_agg, new_column_name, operation, nome_tabela):
    res = df.groupby([first_class, second_class]).agg({column_agg: operation}).rename(columns = {column_agg: new_column_name}).reset_index()
    res.to_csv(nome_tabela, index=False, sep=";")
    print('\n', nome_tabela)
    print(tabulate(res, headers="keys", tablefmt="psql"))

def passag_por_sexo_e_classe():
    NOME_TABELA = '/tmp/passageiros_por_sexo_classe.csv'
    df = pd.read_csv(nome_arquivo, sep=';')
    calc_and_save_table(
        df=df, 
        first_class='Sex', 
        second_class='Pclass', 
        column_agg='PassengerId',
        new_column_name='qtd_passageiros',
        operation='count',
        nome_tabela=NOME_TABELA)


def tarifa_por_sexo_e_classe():
    NOME_TABELA = '/tmp/tarifa_por_sexo_classe.csv'
    df = pd.read_csv(nome_arquivo, sep=';')
    calc_and_save_table(
        df=df, 
        first_class='Sex', 
        second_class='Pclass', 
        column_agg='Fare',
        new_column_name='tarifa_media',
        operation='mean',
        nome_tabela=NOME_TABELA)

def SibspParch_por_sexo_e_classe():

    NOME_TABELA = '/tmp/SibspParch_por_sexo_classe.csv'
    df = pd.read_csv(nome_arquivo, sep=';')

    # Criando a coluna com a soma de SibSp e Parch
    df['sibsp_parch'] = df['SibSp'] + df['Parch']

    calc_and_save_table(
        df=df, 
        first_class='Sex', 
        second_class='Pclass', 
        column_agg='sibsp_parch',
        new_column_name='sibsp_parch',
        operation=np.sum,
        nome_tabela=NOME_TABELA)

def indicadores_por_sexo_e_classe():
    tabela_indicadores = '/tmp/tabela_unica.csv'
    passageiros = '/tmp/passageiros_por_sexo_classe.csv'
    tarifa = '/tmp/tarifa_por_sexo_classe.csv'
    sibs_parch = '/tmp/SibspParch_por_sexo_classe.csv'

    df_passageiros = pd.read_csv(passageiros, sep=';')
    df_tarifa = pd.read_csv(tarifa, sep=';')
    df_sibs_parch = pd.read_csv(sibs_parch, sep=';')

    # print(tabulate(df_passageiros, headers="keys", tablefmt="psql"))
    # print(tabulate(df_tarifa, headers="keys", tablefmt="psql"))
    # print(tabulate(df_sibs_parch, headers="keys", tablefmt="psql"))

    df_merged = df_passageiros.set_index(['Sex', 'Pclass']).join(df_tarifa.set_index(['Sex', 'Pclass']), how='left')
    df_merged = df_merged.join(df_sibs_parch.set_index(['Sex', 'Pclass']), how='left').reset_index(names=['Sex', 'Pclass'])
    print('\n\nTabela resultado:\n', tabulate(df_merged, headers="keys", tablefmt="psql"))

    df_merged.to_csv(tabela_indicadores, index=False, sep=";")



ingestao = PythonOperator(
    task_id = "ingestao_de_dados",
    python_callable = load_data,
    dag=dag
)
calc1 = PythonOperator(
    task_id = "qtd_passageiros_por_sexo_classe",
    python_callable = passag_por_sexo_e_classe,
    dag=dag
)
calc2 = PythonOperator(
    task_id = "tarifa_media_por_sexo_classe",
    python_callable = tarifa_por_sexo_e_classe,
    dag=dag
)
calc3 = PythonOperator(
    task_id = "SibspParch_por_sexo_classe",
    python_callable = SibspParch_por_sexo_e_classe,
    dag=dag
)
uniao_indicadores = PythonOperator(
    task_id = "uniao_indicadores_por_sexo_classe",
    python_callable = indicadores_por_sexo_e_classe,
    dag=dag
)
trigger_dag2 = TriggerDagRunOperator(
    task_id = 'trigger_dag2',
    trigger_dag_id = 'exercício_dag2',
    dag=dag
)



ingestao >> [calc1, calc2, calc3] >> uniao_indicadores >> trigger_dag2