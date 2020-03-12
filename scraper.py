import re
import queue
import sys
import string
import os

import bs4
from bs4 import Comment
import requests
import urllib.parse

import pandas as pd
import sqlite3
import sqlalchemy

###############################################################################
                    # GENERAL PURPOSE WEB SCRAPING FUNCTIONS #
###############################################################################

def get_request(url):
    '''
    Open a connection to the specified URL and read the connection if successful

    Inputs:
        url (str): An absolute url

    Returns:
        Request object or None
    '''

    if url == '':
        r = None
    elif urllib.parse.urlparse(url).netloc != '':
        try:
            r = requests.get(url)
            if r.status_code == 404 or r.status_code == 403:
                r = None
        except Exception:
            r = None
    else:
        r = None

    return r

def read_request(request):
    '''
    Return data from request object.
    Returns result of '' if the read fails
    '''

    try:
        return request.text.encode('utf8')
    except Exception:
        return ''

def get_request_url(request):
    '''
    Extract the true url from a request)
    '''
    return request.url

def is_absolute_url(url):
    '''
    Is the url string an absolute url?
    '''
    if url == '':
        return False
    return urllib.parse.urlparse(url).netloc != ''

def remove_fragment(url):
    '''
    Remove the fragment from a url
    '''
    (url, frag) = urllib.parse.urldefrag(url)
    return url

def convert_if_relative_url(current_url, new_url):
    '''
    Attempt to determine whether new_url is a realtive url.
    If so, use current_url to determine the path and create a new absolute url.
    Will add the protocol, if that is all that is missing.

    Inputs:
        current_url (str): absolute url
        new_url (str): the url we are converting

    Returns:
        new_absolute_url if the new_url can be converted
        None if it cannot determine that new_url is a relative_url
    '''

    if new_url == '' or not is_absolute_url(current_url):
        return None

    if is_absolute_url(new_url):
        return new_url

    parsed_url = urllib.parse.urlparse(new_url)
    path_parts = parsed_url.path.split('/')

    if len(path_parts) == 0:
        return None

    ext = path_parts[0][-4:]
    if ext in ['.edu', '.org', '.com', '.net']:
        return 'http://' + new_url
    elif new_url[:3] == 'www':
        return 'http://' + new_path
    else:
        return urllib.parse.urljoin(current_url, new_url)

def get_soup(url):
    '''
    Takes a url string and returns a bs4 object

    Inputs:
        url (str): a url

    Returns:
        A BeautifulSoup object
    '''

    request = get_request(url)

    if request != None:
        text = read_request(request)
        return bs4.BeautifulSoup(text, 'html.parser')

def queue_links(soup, starting_url, link_q, sub='main'):
    '''
    Given a bs4 object, pull out all the links that need to be crawled

    Inputs:
        soup (bs4): a bs4 objec tat all link tags (a) can be pulled from
        starting_url (str): the initial url that created the soup object
        link_q (Queue): the current queue of links to crawl
        sub (str): the subcrawl

    Returns:
        Updated link_q with all link tags that need to be crawled
    '''

    links = soup.find_all('a', href = True)

    for link in links:
        href = link.get('href')
        no_frag = remove_fragment(href)
        clean_link = convert_if_relative_url(starting_url, no_frag)

        if is_absolute_url(clean_link):
            if okay_url_fbref(clean_link, sub):
                if clean_link != starting_url:
                    if clean_link not in link_q.queue:
                        link_q.put(clean_link)

    return link_q

def to_sql(df, name, db):
    '''
    Converts a pandas DataFrame to an SQLite table and adds it to a database.

    Inputs:
        df (DataFrame): a pandas DataFrame created by a get_tables function
        title (str): the name of the SQL table we're creating
        db (database): a SQL database

    Returns:
        None
    '''

    connection = sqlite3.connect(db)
    cursor = connection.cursor()

    df.to_sql(name, connection, if_exists = "replace")
    print('Wrote ', name, 'to the database')
    cursor.close()
    connection.close()

###############################################################################
                    # SPECIFIC CRAWLERS FOR SITES #
###############################################################################

#################################fbref.com#####################################

def okay_url_fbref(url, sub='main'):
    '''
    Checks if a url is within the limiting_domain of the fbref crawler

    Inputs:
        url (str): an absolute url
        sub (str): which subcrawl are we okaying

    Returns:
        True if the protocol for the url is http(s), the domain is in the
            limiting_domain, and the path is either a directory or a file that
            has no extension or ends in .html.
        False otherwise or if the url includes a '@'
    '''

    limiting_domain = 'fbref.com/en/comps/9/'

    parsed_url = urllib.parse.urlparse(url)
    loc = parsed_url.netloc
    ld = len(limiting_domain)
    trunc_loc = loc[-(ld+1):]

    adv_years = ['2018-2019', '2017-2018']

    if url == None:
        return False

    if 'mailto:' in url or '@' in url:
        return False

    if parsed_url.scheme != 'http' and parsed_url.scheme != 'https':
        return False

    if loc == '':
        return False

    if parsed_url.fragment != '':
        return False

    if parsed_url.query != '':
        return False

    if not (limiting_domain in loc+parsed_url.path):
        return False

    if sub == 'main':
        if not '/stats/' in parsed_url.path:
            return False

    if sub == 'keep_adv':
        if not '/keepersadv' in parsed_url.path:
            return False

        good_year = False

        for year in adv_years:
            if year in parsed_url.path:
                good_year = True
                break

        if good_year == False:
            return False

    if sub == 'keep_basic':
        if not '/keepers/' in parsed_url.path:
            return False

    if sub == 'shooting':
        if not '/shooting/' in parsed_url.path:
            return False

    if sub == 'passing':
        if not '/passing/' in parsed_url.path:
            return False

        good_year = False

        for year in adv_years:
            if year in parsed_url.path:
                good_year = True
                break

        if good_year == False:
            return False

    (filename, ext) = os.path.splitext(parsed_url.path)

    return (ext == '' or ext == '.html')

def get_tables_fbref(soup, db='players.db'):
    '''
    Takes a https://fbref.com/en/comps/9/####/stats/ page and updates the
    players.db sqlite3 database using the tables from the page.

    Inputs:
        soup (bs4): BeautifulSoup for a fbref.com yearly stats page
        db (database): sqlite3 database

    Returns:
        None
    '''
    tables = soup.find_all('div', class_ = "table_outer_container")

     # get players data in commented out table
    players = soup.find_all(text = lambda text: isinstance(text, Comment))

    # commented fbref table is the 11th in the list of comments
    plays = bs4.BeautifulSoup(players[11], 'html.parser').find('tbody')
    table_rows = plays.find_all('tr')

    col_tags = bs4.BeautifulSoup(players[11], 'html.parser').find_all('th', scope = 'col')
    columns = []
    for col in col_tags:
        columns.append(col.get_text())
    columns = columns[1:]

    # get year that data is from on FBref
    year = soup.find('li', class_='full').get_text()[:9]

    # rename columns
    columns[15] = 'Gls_per_game'
    columns[16] = 'Ast_per_game'
    if len(columns) >= 23:
        columns[23] = 'xG_per_game'
        columns[24] = 'xA_per_game'
        columns[25] = 'xG+xA_per_game'
        columns[26] = 'npxG_per_game'
        columns[27] = 'npxG+xA_per_game'

    # construct the player_data DataFrame
    data = []
    for tr in table_rows:
        td = tr.find_all('td')
        row = [tr.text for tr in td]
        data.append(row)
    player_data = pd.DataFrame(data, columns = columns)
    player_data = player_data.dropna()

    # drop matches column beacuse it is just a link to matches played
    if 'Matches' in player_data.columns:
        player_data = player_data.drop(columns = 'Matches')

    # clean and parse position column
    player_data = player_data.rename(columns={'Pos': 'Pos_1'})
    player_data.insert(3, 'Pos_2', None)
    player_data[['Pos_1', 'Pos_2']] = player_data.Pos_1.str.split(',', expand=True)

    # clean nation column
    player_data['Nation'] = player_data['Nation'].str.strip().str[-3:]

    print(player_data)

    # write the main year table to the database
    to_sql(player_data, year, db)

    #  generate 3 additional tables for each year that contain players
    # from each of the major positions
    positions = ['DF', 'FW', 'MF']
    for pos in positions:
        pos_1 = player_data.loc[(player_data['Pos_1']==pos) \
                                & (player_data['Pos_2'].isnull())]
        print('Pos_1 only: ', pos, pos_1)
        title = year + '-' + pos
        to_sql(pos_1, title, db)

    # generate the the wingback table and write to the database
    df_mf = player_data[(player_data['Pos_1'] == 'DF') \
                        & (player_data['Pos_2'] == 'MF')]
    mf_df = player_data[(player_data['Pos_1'] == 'MF') \
                        & (player_data['Pos_2'] == 'DF')]
    wb = pd.concat([df_mf, mf_df])
    print('Wingback table:', wb)
    title = year + '-WB'
    to_sql(wb, title, db)

    # generate the the winger table and write to the database
    fw_mf = player_data[(player_data['Pos_1'] == 'FW') \
                        & (player_data['Pos_2'] == 'MF')]
    mf_fw = player_data[(player_data['Pos_1'] == 'MF') \
                        & (player_data['Pos_2'] == 'FW')]
    wb = pd.concat([fw_mf, mf_fw])
    print('Winger table:', wb)
    title = year + '-WING'
    to_sql(wb, title, db)

    return player_data

def get_keeper_adv_tables(soup, db='players.db'):
    '''
    Takes a https://fbref.com/en/comps/9/##/####/keepersadv/ page and updates
    the players.db sqlite3 database using the tables from the page.

    Inputs:
        soup (bs4): BeautifulSoup for a fbref.com yearly stats page
        db (database): sqlite3 database

    Returns:
        None
    '''
    tables = soup.find_all('div', class_ = "table_outer_container")

     # get players data in commented out table
    players = soup.find_all(text = lambda text: isinstance(text, Comment))

    # commented fbref table is the 11th in the list of comments
    plays = bs4.BeautifulSoup(players[11], 'html.parser').find('tbody')
    table_rows = plays.find_all('tr')

    col_tags = bs4.BeautifulSoup(players[11], 'html.parser').find_all('th', scope = 'col')
    columns = []
    for col in col_tags:
        columns.append(col.get_text())
    columns = columns[1:]
    print(columns)

    # get year that data is from on FBref
    year = soup.find('li', class_='full').get_text()[:9]

    # rename columns
    columns[16] = 'Launched_Cmp'
    columns[17] = 'Launched_Att'
    columns[18] = 'Launched_Cmp%'
    columns[19] = 'Pass_Att'
    columns[23] = 'GK_Att'
    columns[24] = 'GK_Launch%'
    columns[25] = 'GK_AvgLen'
    columns[26] = 'Cross_Att'
    columns[27] = 'Cross_Stp'
    columns[28] = 'Cross_Stp%'
    columns[31] = 'Avg_Def_Dist'

    data = []
    for tr in table_rows:
        td = tr.find_all('td')
        row = [tr.text for tr in td]
        data.append(row)
    keeper_data = pd.DataFrame(data, columns = columns)
    keeper_data = keeper_data.dropna()

    # drop matches column beacuse it is just a link to matches played
    if 'Matches' in keeper_data.columns:
        keeper_data = keeper_data.drop(columns = 'Matches')

    # clean nation column
    keeper_data['Nation'] = keeper_data['Nation'].str.strip().str[-3:]

    print(keeper_data)

    name = year+'-GK-ADV'

    # write the main year table to the database
    to_sql(keeper_data, name, db)

def get_keeper_basic_tables(soup, db='players.db'):
    '''
    Takes a https://fbref.com/en/comps/9/####/keepers/ page and updates
    the players.db sqlite3 database using the tables from the page.

    Inputs:
        soup (bs4): BeautifulSoup for a fbref.com yearly stats page
        db (database): sqlite3 database

    Returns:
        None
    '''
    tables = soup.find_all('div', class_ = "table_outer_container")

     # get players data in commented out table
    players = soup.find_all(text = lambda text: isinstance(text, Comment))

    # commented fbref table is the 11th in the list of comments
    plays = bs4.BeautifulSoup(players[11], 'html.parser').find('tbody')
    table_rows = plays.find_all('tr')

    col_tags = bs4.BeautifulSoup(players[11], 'html.parser').find_all('th', scope = 'col')
    columns = []
    for col in col_tags:
        columns.append(col.get_text())
    columns = columns[1:]
    print(columns)

    # get year that data is from on FBref
    year = soup.find('li', class_='full').get_text()[:9]

    data = []
    for tr in table_rows:
        td = tr.find_all('td')
        row = [tr.text for tr in td]
        data.append(row)
    keeper_data = pd.DataFrame(data, columns = columns)
    keeper_data = keeper_data.dropna()

    # drop matches column beacuse it is just a link to matches played
    if 'Matches' in keeper_data.columns:
        keeper_data = keeper_data.drop(columns = 'Matches')

    # clean nation column
    keeper_data['Nation'] = keeper_data['Nation'].str.strip().str[-3:]

    print(keeper_data)

    name = year+'-GK'

    # write the main year table to the database
    to_sql(keeper_data, name, db)

def get_shooting_tables(soup, db='players.db'):
    '''
    Takes a https://fbref.com/en/comps/9/####/shooting/ page and updates
    the players.db sqlite3 database using the tables from the page.

    Inputs:
        soup (bs4): BeautifulSoup for a fbref.com yearly stats page
        db (database): sqlite3 database

    Returns:
        None
    '''
    tables = soup.find_all('div', class_ = "table_outer_container")

     # get players data in commented out table
    players = soup.find_all(text = lambda text: isinstance(text, Comment))

    # commented fbref table is the 11th in the list of comments
    plays = bs4.BeautifulSoup(players[11], 'html.parser').find('tbody')
    table_rows = plays.find_all('tr')

    col_tags = bs4.BeautifulSoup(players[11], 'html.parser').find_all('th', scope = 'col')
    columns = []
    for col in col_tags:
        columns.append(col.get_text())
    columns = columns[1:]
    print(columns)

    # get year that data is from on FBref
    year = soup.find('li', class_='full').get_text()[:9]

    data = []
    for tr in table_rows:
        td = tr.find_all('td')
        row = [tr.text for tr in td]
        data.append(row)
    shooting_data = pd.DataFrame(data, columns = columns)
    shooting_data = shooting_data.dropna()

    # drop matches column beacuse it is just a link to matches played
    if 'Matches' in shooting_data.columns:
        shooting_data = shooting_data.drop(columns = 'Matches')

    # clean and parse position column
    shooting_data = shooting_data.rename(columns={'Pos': 'Pos_1'})
    shooting_data.insert(3, 'Pos_2', None)
    shooting_data[['Pos_1', 'Pos_2']] = shooting_data.Pos_1.str.split(',', expand=True)

    # clean nation column
    shooting_data['Nation'] = shooting_data['Nation'].str.strip().str[-3:]

    print(shooting_data)

    name = year+'-SHOOT'

    # write the main year table to the database
    to_sql(shooting_data, name, db)

def get_passing_tables(soup, db='players.db'):
    '''
    Takes a https://fbref.com/en/comps/9/##/####/passing/ page and updates
    the players.db sqlite3 database using the tables from the page.

    Inputs:
        soup (bs4): BeautifulSoup for a fbref.com yearly stats page
        db (database): sqlite3 database

    Returns:
        None
    '''
    tables = soup.find_all('div', class_ = "table_outer_container")

     # get players data in commented out table
    players = soup.find_all(text = lambda text: isinstance(text, Comment))

    # commented fbref table is the 11th in the list of comments
    plays = bs4.BeautifulSoup(players[11], 'html.parser').find('tbody')
    table_rows = plays.find_all('tr')

    col_tags = bs4.BeautifulSoup(players[11], 'html.parser').find_all('th', scope = 'col')
    columns = []
    for col in col_tags:
        columns.append(col.get_text())
    columns = columns[1:]
    print(columns)

    # get year that data is from on FBref
    year = soup.find('li', class_='full').get_text()[:9]

    # rename columns
    columns[11] = 'Total_Cmp'
    columns[12] = 'Total_Att'
    columns[13] = 'Total_Cmp%'
    columns[14] = 'Short_Cmp'
    columns[15] = 'Short_Att'
    columns[16] = 'Short_Cmp%'
    columns[17] = 'Med_Cmp'
    columns[18] = 'Med_Att'
    columns[19] = 'Med_Cmp%'
    columns[20] = 'Long_Cmp'
    columns[21] = 'Long_Att'
    columns[22] = 'Long_Cmp%'

    data = []
    for tr in table_rows:
        td = tr.find_all('td')
        row = [tr.text for tr in td]
        data.append(row)
    passing_data = pd.DataFrame(data, columns = columns)
    passing_data = passing_data.dropna()

    # drop matches column beacuse it is just a link to matches played
    if 'Matches' in passing_data.columns:
        passing_data = passing_data.drop(columns = 'Matches')

    # clean and parse position column
    passing_data = passing_data.rename(columns={'Pos': 'Pos_1'})
    passing_data.insert(3, 'Pos_2', None)
    passing_data[['Pos_1', 'Pos_2']] = passing_data.Pos_1.str.split(',', expand=True)

    # clean nation column
    passing_data['Nation'] = passing_data['Nation'].str.strip().str[-3:]

    print(passing_data)

    name = year+'-PASS'

    # write the main year table to the database
    to_sql(passing_data, name, db)

def crawl(link_q, sub, get_tables, pages_crawled):
    '''
    Crawls a link_q using an url checker function and a get tables function

    Inputs:
        link_q (Queue): queue of links to crawl
        sub (str): passed to okay_url_fbref to add additional checks for subcrawlers
        get_tables (function): a function that gets the tables for a soup object
        pages_crawled (list): a list of pages that have been crawled so far

    Outputs:
        None
    '''
    while not link_q.empty():
        year_page = link_q.get()

        request = get_request(year_page)
        if request != None:
            year_page = get_request_url(request)

        if year_page not in pages_crawled:
            pages_crawled.append(year_page)
            year_soup = get_soup(year_page)
            link_q = queue_links(year_soup, year_page, link_q, sub)
            get_tables(year_soup)

def join_years(db='players.db'):
    '''
    Joins all of the year tables scraped from fbref.com and joins them on
    players by position. Adds the 4 new tables per year to the database.

    Inputs:
        db (Database): the database of player tables that contains tables for:
            1. Standard Stats
                ** Available from all years **
                ** Expected stats from 2017-2018 onwards **
                1.1. FW
                1.2. MF
                1.3. DF
            2. Goalkeeping
                ** Available from all years **
                ** No Save% before 2016-2017 **
            3. Advanced Goalkeeping
                ** Available from 2017-2018 onwards **
            4. Shooting
                ** Available from all years **
                ** No FK before 2017-2018 **
                ** Basic only before 2016-2017 **
            5. Passing
                ** Available from 2017-2018 onwards **
    Returns:
        None
    '''
    connection = sqlite3.connect(db)
    c = connection.cursor()

    years = list(range(1992, 2020))
    seasons = []
    for year in years:
        next_year = year+1
        season = str(year)+'-'+str(next_year)
        seasons.append(season)

    # First, we join the tables for the non-goalkeeping positions
    positions = ['DF', 'FW', 'MF']
    table_suffixes = ['-PASS', '-SHOOT']

    c.close()
    connection.close()


def go_helper(sub):
    '''
    Crawls a subset of https://fbref.com and updates the players.db
    '''
    link_q = queue.Queue()

    if sub == 'main':
        starting_url = 'https://fbref.com/en/comps/9/stats/Premier-League-Stats'
    elif sub == 'keep_adv':
        starting_url = 'https://fbref.com/en/comps/9/keepersadv/Premier-League-Stats'
    elif sub == 'keep_basic':
        starting_url = 'https://fbref.com/en/comps/9/keepers/Premier-League-Stats'
    elif sub == 'shooting':
        starting_url = 'https://fbref.com/en/comps/9/shooting/Premier-League-Stats'
    elif sub == 'passing':
        starting_url = 'https://fbref.com/en/comps/9/passing/Premier-League-Stats'

    pages_crawled = [starting_url]
    soup = get_soup(starting_url)
    link_q = queue_links(soup, starting_url, link_q, sub)

    if sub == 'main':
        get_tables_fbref(soup)
        crawl(link_q, 'main', get_tables_fbref, pages_crawled)
    elif sub == 'keep_adv':
        get_keeper_adv_tables(soup)
        crawl(link_q, 'keep_adv', get_keeper_adv_tables, pages_crawled)
    elif sub == 'keep_basic':
        get_keeper_basic_tables(soup)
        crawl(link_q, 'keep_basic', get_keeper_basic_tables, pages_crawled)
    elif sub == 'shooting':
        get_shooting_tables(soup)
        crawl(link_q, 'shooting', get_shooting_tables, pages_crawled)
    elif sub == 'passing':
        get_passing_tables(soup)
        crawl(link_q, 'passing', get_passing_tables, pages_crawled)

def go():
    '''
    Crawl https://fbref.com and update the players.db

    Inputs:
        None

    Outputs:
        None
    '''
    go_helper('main')
    go_helper('keep_adv')
    go_helper('keep_basic')
    go_helper('shooting')
    go_helper('passing')
