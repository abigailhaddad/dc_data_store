# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""


import requests
from bs4 import BeautifulSoup
import pandas as pd

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



url="https://osse.dc.gov/page/2021-22-parcc-and-msaa-results-and-resources"
df= takeUrlGetResults(url)
 
 



    
