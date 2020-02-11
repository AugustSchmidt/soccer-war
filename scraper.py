import re
import queue
import sys
import string
import os

import csv
import json

import bs4
import requests
import urllib.parse

import pandas as pd

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

    print(link_q.queue)
    return link_q

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

def go_fbref():
    '''
    Crawl https://fbref.com and generate a two pandas dataframe objects for each
    year of statistics for the Premier League:
    1. squad_standard_stats
    2. player_standard_stats

    Inputs:
        None

    Outputs:
        A dictionary of Pandas dataframes
    '''

    starting_url = 'https://fbref.com/en/comps/9/stats/Premier-League-Stats'

    link_q = queue.Queue()
    squad_standard_stats = {}
    player_standard_stats = {}

    pages_crawled = [starting_url]
    soup = get_soup(starting_url)
    link_q = queue_links(soup, starting_url, okay_url_fbref, link_q)

    get_tables(starting_url, squad_standard_stats, player_standard_stats)

    while not link_q.empty():
        year_page = link_q.get()

        request = get_request(year_page)
        if request != None:
            year_page = get_request_url(request)

        if year_page not in pages_crawled:
            pages_crawled.append(year_page)
            year_soup = get_soup(year_page)
            link_q = queue_links(year_soup, year_page, okay_url_fbref, link_q)
            get_tables(year_soup, squad_standard_stats, player_standard_stats)

    return squad_standard_stats, player_standard_stats

def get_tables(soup, squad_standard_stats, player_standard_stats):
    '''
    Takes a https://fbref.com/en/comps/9/####/stats/ page and updates the
    squad_standard_stats and the player_standard_stats dictionaries using the
    tables from the page.

    Inputs:
        soup (bs4): BeautifulSoup for a fbref.com yearly stats page
        squad_standard_stats (dict): dictionary mapping squads to yearly stats
        player_standard_stats (dict): dictionary mapping players to yearly stats

    Returns:
        Updated squad_standard_stats and player_standard_stats dictionaries
    '''
    tables = soup.find_all('div', class_ = "table_outer_container")
    print(tables)
