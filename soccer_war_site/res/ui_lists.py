import sqlite3
import csv
import os
import pandas as pd

def generate_lists():

    # Use this filename for the database
    DATA_DIR = os.path.dirname(__file__)
    DATABASE_FILENAME = os.path.join(DATA_DIR, 'player_data.db')
    connection = sqlite3.connect(DATABASE_FILENAME)
    c = connection.cursor()

    # get all table names
    year = 1992
    tables = []
    while year <= 2019:
        s = str(year) + '-' + str(year + 1)
        tables.append(s)
        year += 1

    squad = []
    pos = []
    age = []
    starts = []
    for table in tables:
        # get lists of unique values from sql database
        squad.append(c.execute('''SELECT DISTINCT Squad FROM ''' + '"' + table + '"').fetchall())
        pos.append(c.execute('''SELECT DISTINCT Pos FROM ''' + '"' + table + '"').fetchall())
        age.append(c.execute('''SELECT DISTINCT Age FROM ''' + '"' + table + '"').fetchall())

    connection.close()

    # create unique lists of values
    squads = []
    for row in squad:
        i = len(row)
        for index in range(i):
            team = row[index][0]
            if team not in squads:
                squads.append(team)

    positions = []
    for row in pos:
        i = len(row)
        for index in range(i):
            position = row[index][0]
            if position not in positions:
                positions.append(position)

    ages = []
    for row in age:
        i = len(row)
        for index in range(i):
            years_old = row[index][0]
            if years_old != '':
                years_old = int(years_old)
                if years_old not in ages:
                    ages.append(years_old)



    # write lists of unique values to file
    squad_df = pd.DataFrame(squads)
    squad_df.to_csv('squad_list.csv', index = False)

    pos_df = pd.DataFrame(positions)
    pos_df.to_csv('pos_list.csv', index = False)

    ages_df = pd.DataFrame(ages)
    ages_df.to_csv('age_list.csv', index = False)
