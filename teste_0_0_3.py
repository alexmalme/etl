# -*- coding: utf-8 -*-
"""
created on 13/05/2021

@author: alx_malme
"""

"""
Programa criado como teste para a Wiretrack
"""
from prefect.run_configs import LocalRun
from prefect import task, Flow, Parameter
from prefect.schedules import IntervalSchedule
from prefect.tasks.database import SQLiteScript
from fuzzywuzzy import fuzz, process
from prefect.tasks.database import SQLiteScript

import typing
from typing import Iterable, Tuple, Any, Dict

import sqlite3 as sq
import pandas as pd
import json
import datetime as dt
from dateutil import parser



create_db_logradouros = SQLiteScript(
    name="db_logradouros",
    db="/home/alx_malme/GitHub/wiretrack_test/db/logradouros.db",
    script='''CREATE TABLE IF NOT EXISTS todos (AnuncioEnderecoCep text,
        FreeformAddress text,
        StreetNumber text,
        StreetName text,
        MunicipalitySubdivision text,
        Municipality text,
        CountrySecondarySubdivision text,
        CountrySubdivision text,
        Country text,
        CountryCode text,
        CountryCodeISO3 text,
        PostalCode text,
        Lat text,
        Lon text,
        Score text,
        Type text)''',
    tags=["db"],
)

create_db_entrada_usuarios = SQLiteScript(
    name="db_entradas_usuarios",
    db="/home/alx_malme/GitHub/wiretrack_test/db/entradas_usuarios.db",
    script='''CREATE TABLE IF NOT EXISTS todos (Id integer primary key,
        FormularioJson text,
        Nome text,
        Data text,
        EntradaUsuario text,
        RespostaSistema text,
        Taxa text,
        Formulario text)''',
    tags=["db"],
)




# %%
@task
def pandas_read_csv(
    file='/home/alx_malme/GitHub/wiretrack_test/sources/cep_logradouro_rio_de_janeiro.csv',
    index_col=None,
    delimiter=';',
):
    df = pd.read_csv(file, delimiter=delimiter, index_col=index_col)
    return df

@task
def create_dataframe(
    data, index=None, columns=None, dtype=None,
):
    return pd.DataFrame(data, index, columns, dtype)

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
@task
def read_user_inputs(file: str) -> dict:
    with open(file, 'r+') as f:
        fi = f.read()
    js = json.loads(fi)
    return js
    
@task
def user_input_extract(user_inputs):
    """
    Args:
        user_inputs (type: dict): dicionário advindo de arquivo json
        contêm o nome do formulário e uma lista com nome do usuário,
        data de inserção e input(bairro ou rua)
    Returns:
        list_inputs (list): contem lista de dicionários com:[{FormularioJson, Nome, Data, EntradaUsuario}]
    """
    list_inputs = []
    if user_inputs.keys():
        forms = [*user_inputs.keys()]
        for form in forms:
            for index, item in enumerate(user_inputs.get(form)):
                dict_inputs = {}
                it = user_inputs.get(form)[index]
                dict_inputs['FormularioJSon'] = form
                dict_inputs['Nome'] = it.get('user')
                d_ = it.get('datetime')
                dict_inputs['Data'] = parser.parse(d_).strftime("%d/%m/%Y %H:%M:%S")
                if form == 'forms_bairros':
                    input_usuario = it.get('neighborhood')
                    dict_inputs['EntradaUsuario'] = input_usuario
                    with open(
                        'sources/bairros_rio_de_janeiro.txt', 'r+'
                    ) as brj:
                        brj = brj.read()
                    dict_rio = json.loads(brj)
                    all_neighborhoods = set(
                        logradouro.lower().strip()
                        for lista in dict_rio.values()
                        for logradouro in lista
                    )
                    neighborhood_to_search = str(input_usuario
                                                 ).strip().lower()
                    best = process.extractOne(
                        neighborhood_to_search,
                        all_neighborhoods,
                        score_cutoff=90,
                    )
                    if best:
                        dict_inputs['RespostaSistema'] = best[0].title()
                        dict_inputs['Taxa'] = best[1]
                    else:
                        dict_inputs['RespostaSistema'] = None
                        dict_inputs['Taxa'] = None
                    dict_inputs['Formulario'] = "Bairro"
                elif form == 'forms_ruas':
                    input_usuario = it.get('street')
                    dict_inputs['EntradaUsuario'] = input_usuario                    
                    with open('sources/ruas_brasil.txt', 'r+') as ruas:
                        rs = ruas.read()
                    r = set(rs.replace('\n', '').strip().lower().split(','))
                    streets_to_search = str(input_usuario).strip().lower()
                    best = process.extractOne(
                        streets_to_search, r, score_cutoff=90
                    )
                    if best:
                        dict_inputs['RespostaSistema'] = best[0].title()
                        dict_inputs['Taxa'] = best[1]
                    else:
                        dict_inputs['RespostaSistema'] = None
                        dict_inputs['Taxa'] = None
                    dict_inputs['Formulario'] = "Rua"
                list_inputs.append(dict_inputs)
    return list_inputs
    


# Definir os intervalos entre execuções
schedule = IntervalSchedule(
    start_date=dt.datetime.utcnow() + dt.timedelta(seconds=1),
    interval=dt.timedelta(minutes=3),
)

# Definir Prefect Flow
with Flow(
    "Wiretrack Apresentação", run_config=LocalRun(), schedule=schedule
) as flow:
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
        "Id": "integer primary key",
        "FormularioJson": "text",
        "Nome": "text",
        "Data": "text",
        "EntradaUsuario": "text",
        "RespostaSistema": "text",
        "Taxa": "text",
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
    file_logradouros_csv = Parameter(
        "file_logradouros_csv",
        default="sources/cep_logradouro_rio_de_janeiro.csv",
    )
    db_logradouros = create_db_logradouros()
    # %%
    df = pd.read_csv(
        "sources/cep_logradouro_rio_de_janeiro.csv",
        index_col=0,
        delimiter=";",
    )
    # %%
    write_data_w_pandas(
        df, 'db/logradouros.db', 'todos', if_exists='replace', index=False
    )
    # %%
    # Criar a tabela B
    db_entradas_usuarios = create_db_entrada_usuarios()
    read_user_inputs_bairros = read_user_inputs(
        "/home/alx_malme/GitHub/wiretrack_test/sources/inputs_usuarios_bairros.json"
    )
    extract = user_input_extract(read_user_inputs_bairros)
    # df = pd.DataFrame(extract)
    df_ = create_dataframe(data=extract,)
    write_data_w_pandas(
        df_,
        '/home/alx_malme/GitHub/wiretrack_test/db/entradas_usuarios.db',
        'todos',
        if_exists='append',
        index=False,
    )
    
    
    

flow.register(project_name='Wiretrack')
# flow.run()
