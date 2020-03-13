import scraper as s
import numpy as np
import sklearn
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression

def find_regr():
    '''
    Calculates the linear regression between goal differential and wins
    from the Premier League data scraped from wikipedia.

    Inputs:
        None
    Returns:
        coef (float): the coefficient from the linear regression equation
        intercept (float): the y intercept from the linear regression equation
        score (float): the R^2 of the linear regression
    '''
    goals = s.get_wiki_table()
    X = np.array(goals['GD_per_season']).reshape(-1,1)
    y = np.array(goals['Wins_per_season']).reshape(-1,1)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

    regr = LinearRegression()
    regr.fit(X_train, y_train)

    coef = regr.coef_[0][0]
    intercept = regr.intercept_[0]
    score = regr.score(X_train, y_train)

    return coef, intercept, score

def add_war(df, pos):
    '''
    Takes a joined yearly position table scraped from scraper.join_years()
    and adds the war column to the data before being finally written to the db

    Inputs:
        table (DataFrame): the joined DataFrame for a given position and year
        pos (str): the position that the table is for

    Returns:
        table (DataFrame): the table that includes WAR for each player
    '''
    avg_index = df.index[-2]
    replace_index = df.index[-1]
    positions = ['-DF', '-FW', '-MF', '-WB', '-WING']

    if pos == '-DF':
        df['Raw_GD'] = ((df['Gls']) +
                    (df['SoT'])*0.3 -
                    (df['PK']-df['PKatt']) +
                    (df['Ast'])*5 -
                    (df['CrdY']*0.2+df['CrdY']*0.8))
        df['Adj_GD'] = df['Raw_GD']+((df['Min']/90)*0.1)-2.5
    elif pos == '-FW':
        df['Raw_GD'] = ((df['Gls']) +
                    (df['SoT'])*0.3 -
                    (df['PK']-df['PKatt']) +
                    (df['Ast'])*0.75 -
                    (df['CrdY']*0.2+df['CrdY']*0.8))
        df['Adj_GD'] = df['Raw_GD']+((df['Min']/90)*0.1)-6
    elif pos == '-WING':
        df['Raw_GD'] = ((df['Gls']) +
                    (df['SoT'])*0.3 -
                    (df['PK']-df['PKatt']) +
                    (df['Ast'])*0.9 -
                    (df['CrdY']*0.2+df['CrdY']*0.1))
        df['Adj_GD'] = df['Raw_GD']+((df['Min']/90)*0.1)-4
    elif pos == '-WB':
        df['Raw_GD'] = ((df['Gls']) +
                    (df['SoT'])*0.3 -
                    (df['PK']-df['PKatt']) +
                    (df['Ast'])*5 -
                    (df['CrdY']*0.2+df['CrdY']*0.8))
        df['Adj_GD'] = df['Raw_GD']+((df['Min']/90)*0.1)-3
    elif pos == '-MF':
        df['Raw_GD'] = ((df['Gls']) +
                    (df['SoT'])*0.3 -
                    (df['PK']-df['PKatt']) +
                    (df['Ast'])*0.75 -
                    (df['CrdY']*0.2+df['CrdY']*0.8))
        df['Adj_GD'] = df['Raw_GD']+((df['Min']/90)*0.1)-1
    elif pos=='-GK':
        df['Raw_GD'] = (df['Raw_Save%']-df.at[avg_index, 'Raw_Save%'])*20
        df['Adj_GD'] = df['Raw_GD']+((df['Min']/90)*0.1)+3

    print('The replacements adjusted goal differential is: ',pos, df.at[replace_index, 'Adj_GD'])
    print('The averages adjusted goal differential is: ', pos, df.at[avg_index, 'Adj_GD'])
    print('The mean adjusted goal differential is: ', pos, df['Adj_GD'].mean())

    return df
