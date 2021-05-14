# -*- coding: utf-8 -*-
"""
created on 13/05/2021

@author: alx_malme
"""

"""
Programa criado como teste para a Wiretrack
"""

from prefect import task, Flow, Parameter
from prefect.schedules import IntervalSchedule

import sqlite3 as sq
import pandas as pd
import json
import datetime as dt
from dateutil import parser


@task
def connector(database_path='db/logradouros.db'):
    """Cria um objeto para conectar à base de dados
    Se a base não existir, esta função criará uma
    e se conectará a ela. Case já exista, será feita
    apenas a conexão
    Args:
        database_path (str, optional): [path to database].
        Defaults to 'db/logradouros.db'.

    Returns:
        [type: sqlite3.Connection]
    """
    return sq.connect(database_path)



# %%
@task
def connector_cursor(connector):
    """Cria o objetor de cursor

    Args:
        connector (sqlite3.Connection): 

    Returns:
        [type: sqlite.Cursor]
    """
    return connector.cursor()


#%%
@task
def create_tables(cursor, table_name="todos", **kwargs):
    """Cria a tabela se não existir

    Args:
        cursor (sqlite3.Cursor)
        table_name (str, optional): Nome da tabela a ser criada. Defaults to "todos".
        ** kwargs (dict): dicionário com os nomes das colunas e tipos
    """
    sql_create_table = f"CREATE TABLE IF NOT EXISTS {table_name}"
    columns_and_types = ", ".join([f"{k} {v}" for k, v in columns.items()])
    s = f"{sql_create_table} ({columns_and_types})"
    cursor.execute(s)


# %%
@task
def pandas_read(file='sources/cep_logradouro_rio_de_janeiro.csv', index_col=None, delimiter=';'):
    df = pd.read_csv(file, delimiter=delimiter, index_col=index_col)
    return df

# %%
@task
def write_data_w_pandas(dataframe, table_name, connection, **kwargs):
    dataframe.to_sql(table_name, connection, kwargs)



# %%
# Ler os inputs dos usuários
# Configurei como se tivessem sido salvos num arquivo json
@task
def read_user_inputs(file: str):
    with open(file, 'r+') as f:
        fi = f.read()
    js = json.loads(fi)
    return js

# %%
@task
def date_parser(dates: str) -> str:
    d = parser.parse(dates).strftime("%d/%m/%Y %H:%M:%S")
    return d
# Definir os intervalos entre execuções
schedule = IntervalSchedule(
    start_date=dt.datetime.utcnow() + dt.timedelta(seconds=1),
    interval=dt.timedelta(minutes=3),
)

# Definir Prefect Flow
with Flow("Wiretrack Desafio", schedule=schedule) as flow:
    columns = Parameter("columns", default={    
        "AnuncioEnderecoCep": "text",
        "FreeformAddress": "text",
        "StreetNumber": "text",
        "StreetName": "text",
        "MunicipalitySubdivision": "text",
        "Municipality": "text",
        "CountrySecondarySubdivision": "text",
        "CountrySubdivision": "text",
        "Country": "text",
        "CountryCode": "text",
        "CountryCodeISO3": "text",
        "PostalCode": "text",
        "Lat": "text",
        "Lon": "text",
        "Score": "text",
        "Type": "text"
    })    
    ind = Parameter("index", default=False)
    ind_col = Parameter("index_col", default=0)
    delim = Parameter("delimiter", default=",")
    file_input_usuarios_ruas = Parameter(
        "inputs_usuarios_ruas", default="sources/inputs_usuarios_ruas.json"
    )
    file_input_usuarios_bairros = Parameter(
        "inputs_usuarios_bairros",
        default="sources/inputs_usuarios_bairros.json",
    )
    connection = connector()
    curs = connection.cursor()
    ct = create_tables(curs, table_name='todos', kwargs=columns)
    # %%
    df = pandas_read(
        file='sources/cep_logradouro_rio_de_janeiro.csv',
        index_col=ind_col,
        delimiter=delim,
    )
    # %%
    write_data_w_pandas(
        df, 'todos', connection, if_exists='replace', index=ind
    )
    # %%
    connection.close()
    
flow.run(delim=";")