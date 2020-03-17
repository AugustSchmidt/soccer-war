import scraper as s
import numpy as np
import sklearn
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn import preprocessing

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

    return (coef, intercept, score)

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
    coef = find_regr()[0]
    min_max_scaler = preprocessing.MinMaxScaler()

    if pos == '-FW':
        df['Raw_GD'] = ((df['Gls']) +
                       (df['SoT'])*0.3 -
                       ((df['PK']-df['PKatt'])*0.75) +
                       (df['Ast']*0.75) -
                       (df['CrdY']*0.2+df['CrdY']*0.8) +
                       ((df['Min']/90)*0.1)-6).values.astype(float)
        df['Raw_WAR'] = coef*(df['Raw_GD']-df.at[replace_index, 'Raw_GD'])
        raw_war = np.array(df['Raw_WAR']).reshape(-1,1)

        df['Normed_WAR'] = min_max_scaler.fit_transform(raw_war)
        df['WAR'] = df['Normed_WAR']*6

    elif pos == '-WING':
        df['Raw_GD'] = ((df['Gls']) +
                       (df['SoT'])*0.3 -
                       ((df['PK']-df['PKatt'])*0.75) +
                       (df['Ast']*0.9) -
                       (df['CrdY']*0.2+df['CrdY']*0.8) +
                       ((df['Min']/90)*0.1)-4).values.astype(float)
        df['Raw_WAR'] = coef*(df['Raw_GD']-df.at[replace_index, 'Raw_GD'])
        raw_war = np.array(df['Raw_WAR']).reshape(-1,1)

        df['Normed_WAR'] = min_max_scaler.fit_transform(raw_war)
        df['WAR'] = df['Normed_WAR']*6

    elif pos == '-MF':
        df['Raw_GD'] = ((df['Gls']) +
                       (df['SoT'])*0.3 -
                       (df['PK']-df['PKatt']) +
                       (df['Ast'])*0.75 -
                       (df['CrdY']*0.2+df['CrdY']*0.8) +
                       ((df['Min']/90)*0.1)+1).values.astype(float)
        df['Raw_WAR'] = coef*(df['Raw_GD']-df.at[replace_index, 'Raw_GD'])
        raw_war = np.array(df['Raw_WAR']).reshape(-1,1)

        df['Normed_WAR'] = min_max_scaler.fit_transform(raw_war)
        df['WAR'] = df['Normed_WAR']*6

    elif pos == '-DF':
        df['Raw_GD'] = ((df['Gls']) +
                       (df['SoT'])*0.3 -
                       (df['PK']-df['PKatt']) +
                       (df['Ast']*3) -
                       (df['CrdY']*0.2+df['CrdY']*0.8) +
                       ((df['Min']/90)*0.1)+1).values.astype(float)

        df['Raw_WAR'] = coef*(df['Raw_GD']-df.at[replace_index, 'Raw_GD'])
        raw_war = np.array(df['Raw_WAR']).reshape(-1,1)

        df['Normed_WAR'] = min_max_scaler.fit_transform(raw_war)
        df['WAR'] = df['Normed_WAR']*6

    elif pos == '-WB':
        df['Raw_GD'] = ((df['Gls']) +
                       (df['SoT'])*0.3 -
                       ((df['PK']-df['PKatt'])*0.75) +
                       (df['Ast']*0.75) -
                       (df['CrdY']*0.2+df['CrdY']*0.8) +
                       ((df['Min']/90)*0.1)+1).values.astype(float)
        df['Raw_WAR'] = coef*(df['Raw_GD']-df.at[replace_index, 'Raw_GD'])
        raw_war = np.array(df['Raw_WAR']).reshape(-1,1)

        df['Normed_WAR'] = min_max_scaler.fit_transform(raw_war)
        df['WAR'] = df['Normed_WAR']*6

    elif pos=='-GK':
        df['Raw_GD'] = (((df['Raw_Save%']-df.at[replace_index, 'Raw_Save%'])*10)+
                       ((df['CS']-df.at[replace_index, 'CS'])*0.9)+
                       ((df['Min']/90)*0.1)-2)
        df['Raw_WAR'] = coef*(df['Raw_GD']-df.at[replace_index, 'Raw_GD'])
        raw_war = np.array(df['Raw_WAR']).reshape(-1,1)

        df['Normed_WAR'] = min_max_scaler.fit_transform(raw_war)
        df['WAR'] = df['Normed_WAR']*6

    return df
