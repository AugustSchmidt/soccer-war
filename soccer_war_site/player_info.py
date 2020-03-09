# search/query players 
# Gus Schmidt, Isaiah Holquist

import sqlite3
import os

# Use this filename for the database
DATA_DIR = os.path.dirname(__file__)
DATABASE_FILENAME = os.path.join(DATA_DIR, 'player_data.db')

# list of numerical fields
num_fields = ['Age', 'Born', 'MP', 'Starts', 'Min', 'Gls', 'Ast', 'PK', 'PKatt',
'CrdY', 'CrdR', 'Gls_per_game', 'Ast_per_game', 'G+A', 'G-PK', 'G+A-PK', 'xG',
'npxG', 'xA', 'xG_per_game', 'xA_per_game', 'xG+xA_per_game', 'npxG_per_game',
'npxG+xA_per_game']
year = 1992
tables = []
while year <= 2019:
    s = str(year) + '-' + str(year + 1)
    tables.append(s)
    year += 1

def create_connection():
    # create the connection object
    player_data_db = sqlite3.connect(DATABASE_FILENAME)
    # create a cursor object so that we can manipulate the db
    db_cursor = player_data_db.cursor()
    return db_cursor 


def convert_fields(db_cursor):
    '''
    convert relevant text fields to numericals
    '''
    for field in num_fields:
        for table in tables:
            arg = 'SELECT ' + 'CAST(' + field + ' as VARCHAR(100))' + ' FROM ' + table + ';'
            db_cursor.execute(arg)

def find_players(args):
    print(type(('headers', [])))
    return ('headers', [])
    
