# search/query players 
# Gus Schmidt, Isaiah Holquist

import sqlite3
import os

# Use this filename for the database
DATA_DIR = os.path.dirname(__file__)
DATABASE_FILENAME = os.path.join(DATA_DIR, 'player_data.db')

# list of numerical fields
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

def find_players(args_from_ui):
    return build_query(args_from_ui)

def build_query(args_from_ui):
    '''
    build an actual query statement based on user input to be used in the 
    execute statement

    Input:
        args_from_ui: a dictionary of arguments and their corresponding values
    Output:
        string, a query statement to be passed to an execute statement to query the 
        database
    '''
    season = get_table(args_from_ui)
    # create two cases, one for all all seasons and one for single seasons
    query = ''
    args = []
    if args_from_ui['season'] != 'All':
        select_str = get_fields(args_from_ui)
        select_str += ('FROM ' + season + ' ')
        where_clause, args = get_where_clause(args_from_ui)
        query = select_str + where_clause
    else:
        select_str = get_fields(args_from_ui)
        where_str, args = get_where_clause(args_from_ui)
        print('args:', args_from_ui)
        print('where:', where_str)
        # remove semi-colon from where clause
        where_str = where_str[:len(where_str)-1]
        count = 1
        for szn in season:
            szn_str = szn[:10] + '"'
            select_str += ', ' + szn_str + ' as Season '
            from_str = ('FROM ' + szn + ' ')
            if count < len(tables):
                query += (select_str + from_str + where_str + ' UNION ')
                count += 1
            else:
                if args_from_ui['order_by'] == 'None':
                    query += (select_str + from_str + where_str + ';')
                else:
                    query += (select_str + from_str + where_str)
                    query += order_by(args_from_ui) + ';'
            select_str = get_fields(args_from_ui)
        args = args * len(tables)

    cursor = create_connection()

    # return all palyers if no args explicitly selected
    if (args_from_ui['order_by'] == "None" and args_from_ui['season'] == 'All' 
        and 'Squad' not in args_from_ui):
        args = []
        count = 1
        select_str = get_fields(args_from_ui)
        query = ''
        for szn in season:
            szn_str = szn[:10] +'"'
            select_str += ', ' + szn_str + ' as Season '
            from_str = ('FROM ' + szn + ' ')
            if count < len(tables):
                query += select_str + from_str + ' UNION '
                count += 1
            else:
                query += select_str + from_str + 'ORDER BY season DESC' + ';'
            select_str = get_fields(args_from_ui)
        args = args * len(tables)
        # return (['Please select values to '], []) 

    r = cursor.execute(query, args)
    players = r.fetchall()
    headers = clean_header(get_header(r))

    return (headers, players)

def get_table(args_from_ui):
    '''
    given the args from ui, return the correct table (season) that should be quried

    Inputs:
        args_from_ui: a dictionary of arguments and their corresponding values 
    Outputs:
        returns a string to be used as the table in the query
    '''
    season = args_from_ui['season']
    pos = args_from_ui['Pos']
    if season != 'All':
        return '"' + season + '-' + pos + '-' + 'JOIN' + '"' 
    else:
        seasons = []
        for szn in tables:
            full_table = '"' + szn + '-' + pos + '-' + 'JOIN' + '"' 
            seasons.append(full_table)
        return seasons

def get_fields(args_from_ui):
    '''
    Given the dictionary of arguments, get a list of fields to be used in the 
    SELECT statement of the query

    Inputs:
        args_from_ui: a dictionary of arguments and their corresponding values 
    Outputs:
        returns a string of comma separated values to be used in the query statement 
    '''
    hyrbid_pos = ['WB', 'WING']
    added_fields = ['Player', 'order_by']
    select_str = '''SELECT DISTINCT Player, WAR, Squad, '''
    pos = args_from_ui['Pos']
    for arg, val in args_from_ui.items():
        if arg != 'season':
            # check for bounded argument
            if 'upper' in arg or 'lower' in arg:
                if pos != 'GK':
                    if 'gls' in arg:
                        if 'Gls' not in added_fields:
                            select_str += ('Gls' + ', ')
                            added_fields.append('Gls')
                    elif 'ast' in arg:
                        if 'Ast' not in added_fields:
                            select_str += ('Ast' + ', ')
                            added_fields.append('Ast')
                elif 'age' in arg:
                    if 'Age' not in added_fields:
                        select_str += ('Age' + ', ')
                        added_fields.append('Age')
            else:
                if arg not in added_fields:
                    if arg != 'Pos' and arg != 'Squad':
                        select_str += (arg + ', ')
                        added_fields.append(arg)
                    else:
                        if val != 'GK':
                            if val not in hyrbid_pos:
                                select_str += ('Pos_1 as Pos' + ', ')
                            else:
                                select_str += 'Pos_1, Pos_2, '
                        else:
                            select_str += ' Pos, '
                        added_fields.append('Pos')

    # cut off hanging comma
    select_str = select_str[:len(select_str) - 2]
    select_str += ' '
    return select_str


def get_where_clause(args_from_ui):
    '''
    build the where clause for the query statement

    Inputs:
        args_from_ui: a dictionary of arguments and their corresponding values
        added_fields: a list of fields that need to be queried  
    Outputs:
        returns a string of corresponding where conditions based on user input
    '''
    args = []
    first = True
    where_str = ''
    goals_flag = True
    for key, val in args_from_ui.items():
        if first:
            if key == 'Player':
                where_str += 'WHERE Player = ? '
                args.append(val)
                first = False
            elif key == 'age_upper':
                lb = args_from_ui['age_lower']
                where_str += 'WHERE Age <= ? '
                args.append(val)
                where_str += ' AND Age >= ?'
                args.append(lb)
                first = False
            if args_from_ui['Pos'] != 'GK':
                if key == 'gls_upper' or key == 'gls_lower':
                    lb = args_from_ui['gls_lower']
                    ub = args_from_ui['gls_upper']
                    where_str += 'WHERE Gls <= ? '
                    args.append(ub)
                    where_str += ' AND Gls >= ? '
                    args.append(lb)
                    first = False
                    goals_flag = False
                elif key == 'Ast':
                    lb = val[0]
                    ub = val[1]
                    where_str += 'WHERE Ast <= ? AND Ast >= ? '
                    args.append(ub)
                    args.append(lb)
                    first = False
            if key == 'Nation':
                if val != 'All':
                    where_str += 'WHERE Nation = ? '
                    args.append(val)
                    first = False
            if key == 'Squad':
                count = 1
                where_str += 'WHERE ('
                for squad in val:
                    where_str += 'Squad = ?'
                    args.append(squad)
                    if count < len(val):
                        where_str += (' OR ')
                        count += 1
                where_str += ')'
                first = False
        else:
            if key == 'Player':
                where_str += ' AND Player = ? '
                args.append(val)
            elif key == 'age_upper':
                lb = args_from_ui['age_lower']
                where_str += ' AND Age <= ? '
                args.append(val)
                where_str += ' AND Age >= ?'
                args.append(lb)
            if args_from_ui['Pos'] != 'GK':
                if key == 'gls_upper':
                    if goals_flag:
                        ub = args_from_ui['gls_upper']
                        lb = args_from_ui['gls_lower']
                        where_str += ' AND Gls <= ? '
                        args.append(ub)
                        where_str += ' AND Gls >= ? '
                        args.append(lb)
                elif key == 'Ast':
                    lb = val[0]
                    ub = val[1]
                    where_str += ' AND Ast <= ? AND Ast >= ? '
                    args.append(ub)
                    args.append(lb)
            if key == 'Nation':
                if val != 'All':
                    where_str += ' AND Nation = ? '
                    args.append(val)
            if key == 'Squad':
                count = 1
                where_str += ' AND ('
                for squad in val:
                    where_str += ' Squad = ?'
                    args.append(squad)
                    if count < len(val):
                        where_str += (' OR')
                        count += 1
                where_str += ')'

    if args_from_ui['season'] != 'All':
        where_str += order_by(args_from_ui)
    where_str += ';'
    return (where_str, args)

def order_by(args_from_ui):
    '''
    Take in the args and add an ORDER BY statement if needed

    Inputs:
        args_from_ui: dictionary of arguments
    '''
    order_by_dict = {'Goals': 'Gls', 'Assists': 'Ast', 'Position': 'Pos', 
    'Age': 'age_lower', 'Nationality': 'Nation', 'Season': 'season', 'WAR': 'WAR',
    'Squad': 'Squad'}
    order = args_from_ui['order_by']
    order_by = ''
    if args_from_ui['order_by'] != 'None':
        if order_by_dict[order] in args_from_ui:
            if args_from_ui['order_by'] == 'Goals':
                order_by += ' ORDER BY Gls DESC'
            elif args_from_ui['order_by'] == 'Assists':
                order_by += ' ORDER BY Ast DESC'
            elif args_from_ui['order_by'] == 'Nationality':
                order_by += ' ORDER BY Nation'
            elif args_from_ui['order_by'] == 'Season':
                if type(args_from_ui['season']) == list:
                    order_by += 'ORDER BY Season DESC'
            elif args_from_ui['order_by'] != 'Season':
                order_by += (' ORDER BY ' + args_from_ui['order_by'] + ' DESC')
        elif args_from_ui['order_by'] == 'WAR':
                order_by += 'ORDER BY WAR DESC'
    return order_by

########### auxiliary functions #################
def get_header(cursor):
    '''
    Given a cursor object, returns the appropriate header (column names)
    '''
    desc = cursor.description
    header = ()

    for i in desc:
        header = header + (clean_header(i[0]),)

    return list(header)


def clean_header(s):
    '''
    Removes table name from header
    '''
    for i, _ in enumerate(s):
        if s[i] == ".":
            s = s[i + 1:]
            break

    return s
    
