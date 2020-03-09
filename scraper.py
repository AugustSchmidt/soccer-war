import re
import queue
import sys
import string
import os

import csv
import json

import bs4
from bs4 import Comment
import requests
import urllib.parse

import pandas as pd
import sqlite3

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
        print('Read failed: ' + request.url)
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
    else:
        print('The request failed')


def queue_links(soup, starting_url, is_okay, link_q):
    '''
    Given a bs4 object, pull out all the links that need to be crawled

    Inputs:
        soup (bs4): a bs4 objec tat all link tags (a) can be pulled from
        starting_url (str): the initial url that created the soup object
        is_okay (function): a helper function for the website we're crawling
        link_q (Queue): the current queue of links to crawl

    Returns:
        Updated link_q with all link tags that need to be crawled
    '''

    links = soup.find_all('a', href = True)

    for link in links:
        href = link.get('href')
        no_frag = remove_fragment(href)
        clean_link = convert_if_relative_url(starting_url, no_frag)

        if is_absolute_url(clean_link):
            if is_okay(clean_link):
                if clean_link != starting_url:
                    if clean_link not in link_q.queue:
                        link_q.put(clean_link)

    return link_q

def to_sql(df, name, db):
    '''
    Converts a pandas DataFrame to an SQLite table and adds it to a database

    Inputs:
        df (DataFrame): a pandas DataFrame created by a get_tables function
        title (str): the name of the SQL table we're creating
        db (database): a SQL database

    Returns:
        Updated database with new table included
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

def okay_url_fbref(url):
    '''
    Checks if a url is within the limiting_domain of the fbref crawler

    Inputs:
        url (str): an absolute url

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

    if not '/stats/' in parsed_url.path:
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
        Updated players.db
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

    # rename columns
    columns[15] = 'Gls_per_game'
    columns[16] = 'Ast_per_game'
    if len(columns) >= 23:
        columns[23] = 'xG_per_game'
        columns[24] = 'xA_per_game'
        columns[25] = 'xG+xA_per_game'
        columns[26] = 'npxG_per_game'
        columns[27] = 'npxG+xA_per_game'

    # get year that data is from on FBref
    year = soup.find('li', class_='full').get_text()[:9]
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

    # write all of the year tables to the database
    to_sql(player_data, year, db)

    # we now generate 4 additional tables for each year that contain players
    # from each of the major positions

    positions = ['DF', 'FW', 'GK', 'MF']
    for pos in positions:
        pos_1 = player_data.loc[player_data['Pos_1']==pos]
        pos_2 = player_data.loc[player_data['Pos_1']==pos]
        all_pos = pd.concat([pos_1, pos_2])
        title = year + '-' + pos
        to_sql(all_pos, title, db)

    return player_data

def go_fbref():
    '''
    Crawl https://fbref.com and update the players.db

    Inputs:
        None

    Outputs:
        None
    '''

    starting_url = 'https://fbref.com/en/comps/9/stats/Premier-League-Stats'

    link_q = queue.Queue()

    pages_crawled = [starting_url]
    soup = get_soup(starting_url)
    link_q = queue_links(soup, starting_url, okay_url_fbref, link_q)

    get_tables_fbref(soup)

    while not link_q.empty():
        year_page = link_q.get()

        request = get_request(year_page)
        if request != None:
            year_page = get_request_url(request)

        if year_page not in pages_crawled:
            pages_crawled.append(year_page)
            year_soup = get_soup(year_page)
            link_q = queue_links(year_soup, year_page, okay_url_fbref, link_q)
            get_tables_fbref(year_soup)
