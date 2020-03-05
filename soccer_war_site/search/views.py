from django.shortcuts import render

import json
import traceback
import sys
import csv
import os
from player_info import find_players

NOPREF_STR = 'No preference'
RES_DIR = os.path.join(os.path.dirname(__file__), '..', 'res')
COLUMN_NAMES = dict(age: 'Age', born: 'Born', mp: 'MP', 
starts: 'Starts', mins: 'Min', gls: 'Gls', ast: 'Ast', pk: 'PK', pk_att: 'PKatt', y_crds: 'CrdY', r_crds: 'CrdR', gls_per_game: 'Gls_per_game', ast_per_game: 'Ast_per_game', gls_plus_ast: 'G+A',
gls_minus_pk: 'G-PK', gls_plus_ast_minus_pk: 'G+A-PK', xG: 'xG',
np_xg:'npxG', xA: 'xA', xG_per_game: 'xG_per_game', xA_per_game: 'xA_per_game', xG_plus_xA_per_game: 'xG+xA_per_game', np_xg_per_game: 'npxG_per_game', np_xg_plus_xA_per_game: 'npxG+xA_per_game')

# next thing to do is add load_columns method
# need to add file in res folder that will get distinct values
# for columns s