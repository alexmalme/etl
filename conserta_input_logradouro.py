# -*- coding: utf-8 -*-
"""
created on 13/05/2021

@author: alx_malme
"""

"""
Programa criado como teste para a Wiretrack
"""
#%%
from fuzzywuzzy import fuzz, process
import json

# %%
def AcertaBairros(entrada: str) -> str:
	"""
	Função para encontrar nome de bairro mais próximo ao input.
	"""
	with open('sources/bairros_rio_de_janeiro.txt', 'r+') as brj:
		brj = brj.read()
	dict_rio = json.loads(brj)
	all_neighboors = set(
		logradouro for lista in dict_rio.values() for logradouro in lista
	)
	neighboors_to_search = str(entrada).strip().lower()
	for item in all_neighboors:
		str_similarity = fuzz.partial_ratio(neighboors_to_search.lower(), item.lower())
		if str_similarity > 97:
			return_fuzzy = str(item)
		else:
			return_0 = process.extractOne(entrada, all_neighboors)
			return_fuzzy = return_0[0]
	return return_fuzzy.title()

# %%

def AcertaRuas(entrada: str) -> str:
	"""
	Função para encontrar logradouro mais similar ao input
	Retorna logradouro apenas para resultados >= a 95%
	"""
	with open('sources/ruas_brasil.txt', 'r+') as ruas:
		ruas = ruas.read()
	r = set(ruas.replace('\n', '').strip().lower().split(','))
	streets_to_search = str(entrada).strip().lower()
	return_0 = process.extractOne(streets_to_search, r)
	if return_0[1] >= 95:
		return return_0[0].title()
	else:
		return f"Pontuação inferior a 95%, não encontrei resultado próximo"


