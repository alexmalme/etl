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
import numpy as np
import json

# %%
def AcertaBairros(entrada: str) -> str:
	with open('sources/bairros_rio_de_janeiro.txt', 'r+') as brj:
		brj = brj.read()
	dict_rio = json.loads(brj)
	list_of_lists = [*dict_rio.values()]
	list_all_neighboors = [val for sublist in list_of_lists for val in sublist]
	list_all_neighboors = np.array(list_all_neighboors)
	neighboors_to_search = str(x).strip().lower()
	for item in list_all_neighboors:
		str_similarity = fuzz.partial_ratio(neighboors_to_search.lower(), item.lower())
		if str_similarity > 97:
			return_fuzzy = str(item)
		else:
			return_0 = process.extractOne(x, list_all_neighboors)
			return_fuzzy = return_0[0]
	return return_fuzzy.title()

# %%

def AcertaRuas(entrada: str) -> str:
    