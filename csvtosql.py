# -*- coding: utf-8 -*-
"""
created on 13/05/2021

@author: alx_malme
"""

"""
Programa criado como teste para a Wiretrack
"""
# %%
import sqlite3 as sq
from sqlite3.dbapi2 import connect
import pandas as pd

# %%
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
def connector_cursor(connector):
    """Cria o objetor de cursor
    
    Args:
        connector (sqlite3.Connection): 

    Returns:
        [type: sqlite.Cursor]
    """
    return connector.cursor()

#%%
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
def pandas_read(file='sources/cep_logradouro_rio_de_janeiro.csv', index_col=None, delimiter=';'):
    df = pd.read_csv(file, delimiter=delimiter, index_col=index_col)
    return df

# %%
def write_data_w_pandas(dataframe, table_name, connection, **kwargs):
    dataframe.to_sql(table_name, connection, kwargs)
# %%
connection = connector()
curs = connection.cursor()
# nome das tabelas e tipos para o banco de dados
columns = {
    
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
}
ct = create_tables(curs, table_name='todos', kwargs=columns)
# %%
df = pandas_read(file='sources/cep_logradouro_rio_de_janeiro.csv', index_col=0, delimiter=';')
# %%
write_data_w_pandas(df, 'todos', connection, if_exists='replace', index=False)
# %%
connection.close()