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
        colunas (dict): dicionário com os nomes das colunas e tipos
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
def pandas_read_csv(
    file='sources/cep_logradouro_rio_de_janeiro.csv',
    index_col=None,
    delimiter=';',
):
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
@task
def read_user_inputs(file: str) -> dict:
    with open(file, 'r+') as f:
        fi = f.read()
    js = json.loads(fi)
    return js

# %%
@task
def AcertaBairros(entrada: str) -> dict:
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
    return (
        {"RespostaSistema": best[0], "Taxa": best[1]}
        if best[1] >= 95
        else {"RespostaSistema": None, "Taxa": None}
    )
    

# %%
@task
def AcertaRuas(entrada: str) -> dict:
    """
    Função para encontrar logradouro mais similar ao input
    Retorna logradouro apenas para resultados >= a 95%
    """
    with open('sources/ruas_brasil.txt', 'r+') as ruas:
        rs = ruas.read()
    r = set(rs.replace('\n', '').strip().lower().split(','))
    streets_to_search = str(entrada).strip().lower()
    best = process.extractOne(streets_to_search, r)
    return (
        {"RespostaSistema": best[0], "Taxa": best[1]}
        if best[1] >= 92
        else {"RespostaSistema": None, "Taxa": None}
    )

# %%
def fix_user_input(form: str, input_string: str) -> dict:
    dict_return = {}
    i, i_resp, taxa, formulario = input_string, None, None
    if form == 'forms_bairros':
        dict_resposta = AcertaBairros(entrada=i)
        dict_return['Formulario'] = "Bairro"
        dict_return['RespostaSistema'] = dict_resposta['RespostaSistema']
        dict_return['Taxa'] = dict_resposta['Taxa']
    elif form == 'forms_ruas':
        dict_resposta = AcertaRuas(entrada=i)
        dict_return['Formulario'] = "Rua"
        dict_return['RespostaSistema'] = dict_resposta['RespostaSistema']
        dict_return['Taxa'] = dict_resposta['Taxa']        
    return dict_return

@task
def user_transform(x: dict):    
    form = x.get("FormularioJson")
    input_string = x.get("EntradaUsuario")
    formulario_resposta_taxa = fix_user_input(form, input_string)
    x.update(
        {
            'Formulario': formulario_resposta_taxa["Formulario"],
            "RespostaSistema": formulario_resposta_taxa[
                "RespostaSistema"
            ],
            "Taxa": formulario_resposta_taxa['Taxa'],
        }
    )
    return x
    
@task
def user_input_extract(user_inputs: dict) -> list:
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
        dict_inputs = {}
        form = [*user_inputs.keys()][0]
        for item in form:
            it = user_inputs.get(form)[item]
            dict_inputs['FormularioJSon'] = form
            dict_inputs['Nome'] = it.get('user')
            d_ = it.get('datetime')
            dict_inputs['Data'] = parser.parse(d_).strftime("%d/%m/%Y %H:%M:%S")
            if form == 'forms_bairros':
                dict_inputs['EntradaUsuario'] = it.get('neighborhood')
            elif form == 'forms_ruas':
                dict_inputs['EntradaUsuario'] = it.get('street')
            list_inputs.append(dict_inputs)
    return list_inputs

@task            
def user_transform():
    


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
        "Id": "integer primary key",
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
    file_logradouros_csv = Parameter(
        "file_logradouros_csv",
        default="sources/cep_logradouro_rio_de_janeiro.csv",
    )
    create_tables(
        database_path='db/logradouros.db',
        table_name='todos',
        colunas=columns_A,
    )
    # %%
    df = pandas_read_csv(
        file= file_logradouros_csv,
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
    read_user_inputs_streets = file_input_usuarios_bairros
    extract = user_input_extract(read_user_inputs_streets)
    transformed = []
    for x in item:
        transformed.append(user_transform(x))
    
flow.run(delimiter=";")
print(transformed)