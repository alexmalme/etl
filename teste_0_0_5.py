# -*- coding: utf-8 -*-
"""
created on 13/05/2021

@author: alx_malme
"""

"""
Programa criado como teste para a Wiretrack
"""

#%%


#%%
# script para criar o banco de dados
from prefect.run_configs import LocalRun
from prefect.engine.results.local_result import LocalResult
from prefect.engine.results.prefect_result import PrefectResult
from prefect import unmapped, task, Flow, Parameter
from prefect.schedules import IntervalSchedule
from prefect.tasks.database import SQLiteScript
from fuzzywuzzy import process
import pandas as pd
import json
import datetime as dt
from dateutil import parser
create_db_logradouros = SQLiteScript(
    name="db_logradouros",
    db="/home/alx_malme/GitHub/wiretrack_test/db/logradouros.db",
    script='''CREATE TABLE IF NOT EXISTS todos (Id integer primary key,
    AnuncioEnderecoCep text,
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
# script para criar o banco de dados
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
# script usado para inserir as informações nos bancos dados
# Deve ser chamado de forma recorrente, pois insere um dado de cada vez
# otimizado para o banco de dados "A", que é o banco de dados com as informações dos logradouros do Brasil
ins_a = SQLiteScript(
    name="db_logradouros",
    db="/home/alx_malme/GitHub/wiretrack_test/db/logradouros.db",
    script='''INSERT INTO todos (AnuncioEnderecoCep,
        FreeformAddress,
        StreetNumber,
        StreetName,
        MunicipalitySubdivision,
        Municipality,
        CountrySecondarySubdivision,
        CountrySubdivision,
        Country,
        CountryCode,
        CountryCodeISO3,
        PostalCode,
        Lat,
        Lon,
        Score,
        Type) VALUES ("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "l", "m", "n", "o", "p", "q");''',
    tags=["db"],
)
#%%
# script usado para inserir as informações nos bancos dados
# Deve ser chamado de forma recorrente, pois insere um dado de cada vez
# otimizado para o banco de dados "B", que é o banco de dados com as os inputs dos usuários, o que eles inseriram e o que o programa retornou
ins_b = SQLiteScript(
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
    """Cria um DataFrame para ler o arquivo em CSV e retorna uma lista de listas com os valores das colunas. Não retorna os nomes das colunas e nem o index

    Args:
        file (str, optional): [arquivo csv]. Defaults to '/home/alx_malme/GitHub/wiretrack_test/sources/cep_logradouro_rio_de_janeiro.csv'.
        index_col ([type], optional): [index]. Defaults to None.
        delimiter (str, optional): [separador do arquivo CSV]. Defaults to ';'.

    Returns:
        [str: list]: [lista contendo listas]
    """
    df = pd.read_csv(file, delimiter=delimiter, index_col=index_col)
    df = df.iloc[:200, :]
    return df.values.tolist()


@task
def create_insert_script_db_a(dados, tabela, colunas):
    """
    Args:
        dados (list): dados para serem inseridos no banco de dados
        tabela (str): Nome da tabela no banco
        colunas (list): nome das colunas do banco
        
    Returns:
        final (str): string pronta para ser passada para o "script" de "INSERT" do SQL
    """
    col = ", ".join(colunas)
    (
        AnuncioEnderecoCep,
        FreeformAddress,
        StreetNumber,
        StreetName,
        MunicipalitySubdivision,
        Municipality,
        CountrySecondarySubdivision,
        CountrySubdivision,
        Country,
        CountryCode,
        CountryCodeISO3,
        PostalCode,
        Lat,
        Lon,
        Score,
        Type,
    ) = dados
    final = f"""INSERT INTO todos ({col}) VALUES ("{AnuncioEnderecoCep}", "{FreeformAddress}", "{StreetNumber
        }", "{StreetName}", "{MunicipalitySubdivision}", "{Municipality}", "{CountrySecondarySubdivision}", "{CountrySubdivision}", "{Country}", "{CountryCode}", "{CountryCodeISO3
        }", "{PostalCode}", "{Lat}", "{Lon}", "{Score}", "{Type}");"""
    return final


@task
def create_insert_script_db_b(dados, tabela, colunas):
    """
    Args:
        dados (list): dados para serem inseridos no banco de dados
        tabela (str): Nome da tabela no banco
        colunas (list): nome das colunas do banco

    Returns:
        final (str): string pronta para ser passada para o "script" de "INSERT" do SQL
    """
    col = ", ".join(colunas)
    (
        FormularioJson,
        Nome,
        Data,
        EntradaUsuario,
        RespostaSistema,
        Taxa,
        Formulario,
    ) = [str(x) for x in dados.values()]
    final = f"""INSERT INTO todos ({col}) VALUES ("{FormularioJson}", "{Nome}", "{Data}", "{EntradaUsuario}", "{RespostaSistema}", "{Taxa}", "{Formulario
        }");"""
    return final


# %%
@task
def read_user_inputs(
    location="/home/alx_malme/GitHub/wiretrack_test/sources/inputs_usuarios_bairros.json",
):
    """Lê o arquivo com os inputs dos usuários
    PrefectResult é usado para ler arquivos json
    Args:
        location (str, optional): [endereço do arquivo]. Defaults to "/home/alx_malme/GitHub/wiretrack_test/sources/inputs_usuarios_bairros.json".

    Returns:
        [result.value: dict]: retorna um dicionário
    """
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
    return list_inputs


# %%
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
# %%
    # criar as listas com os logradouros
    # neste teste limitei aos 200 primeiros
    list_logradouros = pandas_read_csv(
        file='/home/alx_malme/GitHub/wiretrack_test/sources/cep_logradouro_rio_de_janeiro.csv',
        index_col=None,
        delimiter=';',
    )

    colunas_A = [
        "AnuncioEnderecoCep",
        "FreeformAddress",
        "StreetNumber",
        "StreetName",
        "MunicipalitySubdivision",
        "Municipality",
        "CountrySecondarySubdivision",
        "CountrySubdivision",
        "Country",
        "CountryCode",
        "CountryCodeISO3",
        "PostalCode",
        "Lat",
        "Lon",
        "Score",
        "Type",
    ]
    # função para criar o banco de dados vazio
    # banco de dados que eu consideirei como A para o teste pedido
    # banco de dados dos logradouros
    db_logradouros = create_db_logradouros()
    # Essa chamada para a função cria o arquivo de insert
    # Precisa ser chamada com map pois o for não funciona no corpo do flow
    # o unmapped serve para manter as varíaveis estáticas, enquanto os dados são iterados
    create_inserter_a = create_insert_script_db_a.map(
        colunas=unmapped(colunas_A),
        tabela=unmapped("todos"),
        dados=list_logradouros,
    )
    # chamada para a função que vai inserir dos dados no banco de dados
    save_inputs_a = ins_a.map(
        script=create_inserter_a,
        upstream_tasks=[unmapped(db_logradouros)],
    )
    # %%
    # Criar a tabela B
    # extrair os dados do arquivo json
    read_user_inputs_bairros = read_user_inputs(
        "/home/alx_malme/GitHub/wiretrack_test/sources/inputs_usuarios_bairros.json"
    )
    # aqui os dados serão manipulados, será verificada a similaridade do que os usuários
    # inseriram e a resposta do sistema
    # é a principal função de tratamento dos dados
    extract = user_input_extract(read_user_inputs_bairros)
    # função para criar o banco de dados vazio
    db = create_db_entrada_usuarios()
    colunas_B = [
        "FormularioJson",
        "Nome",
        "Data",
        "EntradaUsuario",
        "RespostaSistema",
        "Taxa",
        "Formulario",
    ]
    # Essa chamada para a função cria o arquivo de insert
    # Precisa ser chamada com map pois o for não funciona no corpo do flow
    # o unmapped serve para manter as varíaveis estáticas, enquanto os dados são iterados
    teste = create_insert_script_db_b.map(
        colunas=unmapped(colunas_B), tabela=unmapped("todos"), dados=extract
    )
    # chamada para a função que vai inserir dos dados no banco de dados
    final = ins_b.map(script=teste, upstream_tasks=[unmapped(db)])


# %%
flow.register(project_name='Wiretrack Apresentação')
#flow.visualize()
#flow.run()
