# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""


import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin

def get_soup(url):
    reqs = requests.get(url)
    soup = BeautifulSoup(reqs.text, 'html.parser')
    return(soup)
    
def get_links(soup):
    urlDict={}
    for link in soup.find_all('a'):
        urlOnPage=link.get('href')
        text=link.text
        urlDict[text]=urlOnPage
    return(urlDict)
    
def filter_attachments(pair):
    key, value = pair
    if "attachments" in value:
        return True  # keep pair in the filtered dictionary
    else:
        return False  # filter pair out of the dictionary
    
def file_type(link):
    extension=link.split(".")[-1]
    return(extension)

def tryURLResults(url):
    try:
        df=takeUrlGetResults(url)
        return(df)
    except:
        print(url)
    
def takeUrlGetResults(url):
    soup=get_soup(url)
    for item in soup.find_all('title'):
        title=item.get_text()
    content = soup.find('meta', {'name':'description'}).get('content')
    urlDict=get_links(soup)
    filtered_urls= dict(filter(filter_attachments, urlDict.items()))
    df=pd.DataFrame()
    df['Name']=filtered_urls.keys()
    df['Link']=filtered_urls.values()
    df['Title']=title
    df['Extension']=df['Link'].apply(file_type)
    df['Page Content']=content
    return(df)

def crawl_site(url, depth=3):
    # this piece starts at a website and gets the other links on those, and does recursion
    # stop if it's not at dc.gov
    visited_urls = set()
    def crawl(url, depth):
        if depth == 0:
            return
        if url in visited_urls:
            return
        visited_urls.add(url)
        try:
            response = requests.get(url)
        except (requests.exceptions.MissingSchema, requests.exceptions.ConnectionError):
            return
        soup = BeautifulSoup(response.text, 'html.parser')
        for link in soup.find_all('a'):
            try:
                href = link.get('href')
                if href:
                    href = urljoin(url, href)
                    crawl(href, depth - 1)
            except:
                pass
    crawl(url, depth)
    return visited_urls

 
visited_urls=crawl_site("https://dc.gov/directory", depth=4)
just_dc_urls=[i for i in visited_urls if "dc.gov" in i]
subdfs=[tryURLResults(url) for url in just_dc_urls]
df=pd.concat(subdfs)
