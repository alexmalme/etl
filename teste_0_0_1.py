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
from fuzzywuzzy import fuzz, process

import sqlite3 as sq
import pandas as pd
import json
import datetime as dt
from dateutil import parser



def connector(database_path='db/logradouros.db'):
    """
    Cria um objeto para conectar à base de dados
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


#%%
@task
def create_tables(database_path, table_name, colunas):
    """Cria um objeto para conectar à base de dados
    Se a base não existir, esta função criará uma
    e se conectará a ela. Case já exista, será feita
    apenas a conexão. Após criará as colunas
    Args:
        database_path (str, optional): [path to database].
        Defaults to 'db/logradouros.db'
        table_name (str, optional): Nome da tabela a ser criada. Defaults to "todos".
        ** kwargs (dict): dicionário com os nomes das colunas e tipos
    """
    connection = sq.connect(database_path
                            )
    curs = connection.cursor()
    sql_create_table = f"CREATE TABLE IF NOT EXISTS {table_name}"
    columns_and_types = ", ".join([f"{k} {v}" for k, v in colunas.items()])
    s = f"{sql_create_table} ({columns_and_types})"
    curs.execute(s)
    connection.close()


# %%
@task
def pandas_read(file='sources/cep_logradouro_rio_de_janeiro.csv', index_col=None, delimiter=';'):
    df = pd.read_csv(file, delimiter=delimiter, index_col=index_col)
    return df

# %%
@task
def write_data_w_pandas(
    dataframe, database_path, table_name, if_exists="replace", index="False"
):
    connection = sq.connect(database_path)
    dataframe.to_sql(
        table_name, connection, if_exists=if_exists, index=index
    )
    connection.close()



# %%
# Ler os inputs dos usuários
# Configurei como se tivessem sido salvos num arquivo json
def read_user_inputs(file: str):
    with open(file, 'r+') as f:
        fi = f.read()
    js = json.loads(fi)
    return js

# %%
def date_parser(dates: str) -> str:
    d = parser.parse(dates).strftime("%d/%m/%Y %H:%M:%S")
    return d

# %%
def AcertaBairros(entrada: str) -> tuple:
    """
    Função para encontrar nome de bairro mais próximo ao input.
    """
    with open('sources/bairros_rio_de_janeiro.txt', 'r+') as brj:
        brj = brj.read()
    dict_rio = json.loads(brj)
    all_neighborhoods = set(
        logradouro for lista in dict_rio.values() for logradouro in lista
    )
    neighborhood_to_search = str(entrada).strip().lower()
    best = process.extractOne(neighborhood_to_search, all_neighborhoods)
    return best if best[1] >= 95 else (None, None)
    

# %%
def AcertaRuas(entrada: str) -> str:
    """
    Função para encontrar logradouro mais similar ao input
    Retorna logradouro apenas para resultados >= a 95%
    """
    with open('sources/ruas_brasil.txt', 'r+') as ruas:
        rs = ruas.read()
    r = set(rs.replace('\n', '').strip().lower().split(','))
    streets_to_search = str(entrada).strip().lower()
    best = process.extractOne(streets_to_search, r)
    return best if best[1] >= 92 else (None, None)

# %%
def fix_user_input(form, name, d_time, input_string: str) -> tuple:
    f, n, d, i = name, d_time, input_string
    if form == 'forms_bairros':
        i_resp = AcertaBairros(entrada=i)
        formulario = "Bairro"
    elif form == 'forms_ruas':
        i_resp = AcertaRuas(entrada=i)
        formulario = "Rua"
    else:
        i_resp = None
    return (f, n, d, i, i_resp, formulario
            )
        

# Definir os intervalos entre execuções
schedule = IntervalSchedule(
    start_date=dt.datetime.utcnow() + dt.timedelta(seconds=1),
    interval=dt.timedelta(minutes=3),
)

# Definir Prefect Flow
with Flow("Wiretrack Desafio", schedule=schedule) as flow:
    columns_A = {
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
        "Type": "text",
    }
    columns_B = {
        "FormularioJson": "text",
        "Nome": "text",
        "Data": "text",
        "EntradaUsuario": "text",
        "RespostaSistema": "text",
        "Taxa": "integer",
        "Formulario": "text",
    }
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
    create_tables(
        database_path='db/logradouros.db',
        table_name='todos',
        colunas=columns_A,
    )
    # %%
    df = pandas_read(
        file='sources/cep_logradouro_rio_de_janeiro.csv',
        index_col=ind_col,
        delimiter=delim,
    )
    # %%
    write_data_w_pandas(
        df, 'db/logradouros.db', 'todos', if_exists='replace', index="False"
    )
    # %%
    # Criar a tabela B
    create_tables(
        database_path='db/entradas_usuarios.db',
        table_name='todos',
        colunas=columns_B,
    )
    df = pan
    
flow.run(delimiter=";")