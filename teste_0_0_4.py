# -*- coding: utf-8 -*-
"""
created on 13/05/2021

@author: alx_malme
"""

"""
Programa criado como teste para a Wiretrack
"""

#%%
from prefect.run_configs import LocalRun
from prefect.engine.results.local_result import LocalResult
from prefect.engine.results.prefect_result import PrefectResult
from prefect import unmapped
from prefect import task, Flow, Parameter
from prefect.schedules import IntervalSchedule
from prefect.tasks.database import SQLiteScript
from fuzzywuzzy import fuzz, process
from prefect.tasks.database import SQLiteScript
import typing
from typing import Iterable, Tuple, Any, Dict, List
import sqlite3 as sq
import pandas as pd
import json
import datetime as dt
from dateutil import parser


#%%
# with open("/home/alx_malme/GitHub/wiretrack_test/sources/inputs_usuarios_bairros.json", "r+") as file:
#     f = file.read()
# presult = PrefectResult()
# result = presult.read(f)

#%%
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

insert_user_input = SQLiteScript(name="OIsert Entrada Usuarios",
                                 db="/home/alx_malme/GitHub/wiretrack_test/db/entradas_usuarios.db",
                              tags=["db"])

#%%
ins = SQLiteScript(
    name="db_entradas_usuarios",
    db="/home/alx_malme/GitHub/wiretrack_test/db/entradas_usuarios.db",
    script='''INSERT INTO todos ( FormularioJson,
        Nome,
        Data,
        EntradaUsuario,
        RespostaSistema,
        Taxa,
        Formulario) VALUES ("a", "b", "c", "d", "e", "f", "g");''',
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

# %%
@task
def create_dataframe(
    data, index=None, columns=None, dtype=None,
):
    return pd.DataFrame(data, index, columns, dtype)

# %%
# @task

@task
def create_insert_script(dados, tabela, colunas):
    col = ", ".join(colunas)
    # insert_cmd = f'''INSERT INTO {tabela} ({col}) VALUES\n'''
    # v = [str(x) for x in dados.values()]
    # val = ',\n'.join(v)
    (
        FormularioJson,
        Nome,
        Data,
        EntradaUsuario,
        RespostaSistema,
        Taxa,
        Formulario,
    ) = [str(x) for x in dados.values()]
    # final = f"{insert_cmd} ({val});"
    # final = f"""INSERT INTO todos ({col}) VALUES ("Caralho", "nnn", "nnn", "nnnnnn", "nnnnnnn", "n", "tttttttttt");"""
    final = f"""INSERT INTO todos ({col}) VALUES ("{FormularioJson}", "{Nome}", "{Data}", "{EntradaUsuario}", "{RespostaSistema}", "{Taxa}", "{Formulario
        }");"""
    print(final)
    return final


# %%
# Ler os inputs dos usuários
# Configurei como se tivessem sido salvos num arquivo json
@task
def read_user_inputs(location="/home/alx_malme/GitHub/wiretrack_test/sources/inputs_usuarios_bairros.json"):
    with open(location, "r+") as fil:
        f = fil.read()
    presult = PrefectResult()
    result = presult.read(f)
    return result.value

# %%
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
                dict_inputs['Data'] = parser.parse(
                    d_).strftime("%d/%m/%Y %H:%M:%S")
                if form == 'forms_bairros':
                    input_usuario = it.get('neighborhood')
                    dict_inputs['EntradaUsuario'] = input_usuario
                    with open('/home/alx_malme/GitHub/wiretrack_test/sources/bairros_rio_de_janeiro.txt', 'r+'
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
                    with open('/home/alx_malme/GitHub/wiretrack_test/sources/ruas_brasil.txt', 'r +') as ruas:
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
    # print(list_inputs)
    return list_inputs

# %%
# Definir os intervalos entre execuções
schedule = IntervalSchedule(
    start_date=dt.datetime.utcnow() + dt.timedelta(seconds=1),
    interval=dt.timedelta(minutes=3),
)
# %%
# Definir Prefect Flow
with Flow(
    "Wiretrack Apresentação", run_config=LocalRun(), schedule=schedule, result=LocalResult()
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
    #write_data_w_pandas(
    #    df, 'db/logradouros.db', 'todos', if_exists='replace', index=False
    #)
    # %%
    # Criar a tabela B
    #db_entradas_usuarios = create_db_entrada_usuarios()
    read_user_inputs_bairros = read_user_inputs(
        "/home/alx_malme/GitHub/wiretrack_test/sources/inputs_usuarios_bairros.json"
    )
    
    extract = user_input_extract(read_user_inputs_bairros)
    # cib = create_insert_data(
    #     dados=["a", "b", "c", "D", "e", "f"], tabela='todos', colunas=columns_B)
    db = create_db_entrada_usuarios()
    #ins()
    colunas_B = [
        "FormularioJson",
        "Nome",
        "Data",
        "EntradaUsuario",
        "RespostaSistema",
        "Taxa",
        "Formulario",
    ]
    # teste = create_insert_script(
        # colunas=colunas_B, tabela="todos", dados=extract[0],
    # )
    # ins(script=teste)
    teste = create_insert_script.map(
        colunas=unmapped(colunas_B), tabela=unmapped("todos"), dados=extract
    )
    
    final = ins.map(script=teste, upstream_tasks=[unmapped(db)])
    # insert_user_input.map(script=cib, upstream_tasks=[unmapped(db)])
    # create_insert_data.map(
    #     dados= extract, tabela='todos', colunas=columns_B)
    #insert_user_input.map(script=insert_script, upstream_tasks=[unmapped(db)])
    # df = pd.DataFrame(extract)
    #df_ = create_dataframe(data=extract,)
    # write_data_w_pandas(
    #     df_,
    #     '/home/alx_malme/GitHub/wiretrack_test/db/entradas_usuarios.db',
    #     'todos',
    #     if_exists='append',
    #     index=False,
    # )

# %%
# flow.register(project_name='Wiretrack Apresentação')
# flow.visualize()
flow.run()

# %%

# %%
