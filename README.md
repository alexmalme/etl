# wiretrack_test
 Desafio para wiretrack

<br>

## __Ambiente de instalação__
> Este programa roda em Python 3.8.  
> Até a presente data 13/05/2021, o Prefect pode apresentar instabilidades no Python 3.9  

<br>

## __Bibliotecas & Dependências__

### __Instalação usando conda:__

Prefect:
```
conda install -c conda-forge prefect
```

Pandas:
```
conda install -c anaconda pandas 
```
  

Numpy:
```
conda install -c conda-forge numpy 
```
  

Sqlite3:
```
conda install -c anaconda sqlite 
```
  

Fuzzywuzzy:
```
 conda install -c conda-forge fuzzywuzzy 
 ```
  


<br>

## __Projeto:__

1.  Criar banco de dados "A" em sqlite3 contendo os logradouros do estado do Rio de Janeiro
    1. 1  O banco será criado à partir dos dados contidos no arquivo csv da pasta "sources"
2. Receber inputs dos usuários, com nomes de ruas e bairros
3. Sanitizar esses inputs e utilizar algoritmo de comparação com os bairros e logradouros contidos no banco de dados e/ou arquivos
4. Salvar os inputs dos usuários em um banco de dados "B", com as colunas: "id"; "nome_usuario"; "entrada"; "retorno_programa"; "taxa_acuracia"


