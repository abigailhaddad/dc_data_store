import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin
import time
from urllib3.exceptions import InsecureRequestWarning
from urllib3 import disable_warnings
import re
disable_warnings(InsecureRequestWarning)
from typing import Optional, Tuple, List


from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

def search_google_selenium(query: str, domain: str, num_results: int) -> List[str]:
    """
    Search Google for the given query and domain and return the first num_results URLs using Selenium.

    :param query: Search query.
    :param domain: Domain to search within.
    :param num_results: Number of results to return.
    :return: List of URLs matching the query and domain.
    """
    query = f"site:{domain} {query}"
    driver = webdriver.Chrome()
    driver.get("https://www.google.com")
    search_box = driver.find_element(By.NAME, "q")
    search_box.send_keys(query)
    search_box.send_keys(Keys.RETURN)

    urls = []
    max_pages = (num_results // 10) + 1 if num_results % 10 != 0 else num_results // 10
    pages_visited = 0

    while len(urls) < num_results and pages_visited < max_pages:
        time.sleep(2)
        results = driver.find_elements(By.XPATH, "//div[@class='g']//a[not(ancestor::div[@class='xpd'])]")
        urls.extend([result.get_attribute("href") for result in results[:num_results - len(urls)]])
        
        next_button = driver.find_elements(By.CSS_SELECTOR, "#pnnext")
        if not next_button:
            break

        next_button[0].click()
        pages_visited += 1

    driver.quit()
    return urls


def deduplicate_urls(urls: List[str]) -> List[str]:
    """
    Remove duplicate URLs from the list.

    :param urls: List of URLs.
    :return: List of unique URLs.
    """
    return list(set(urls))

def get_soup(url: str) -> BeautifulSoup:
    """
    Get the soup object of the given URL.

    :param url: URL of the webpage.
    :return: Soup object of the webpage.
    """
    reqs = requests.get(url, verify=False)
    soup = BeautifulSoup(reqs.text, 'lxml')
    return soup


def get_links(soup: BeautifulSoup) -> dict[str, str]:
    """
    Extract links from a BeautifulSoup object and return a dictionary.

    :param soup: BeautifulSoup object.
    :return: Dictionary of link text and corresponding URLs.
    """
    url_dict = {}
    for link in soup.find_all('a'):
        url_on_page = link.get('href')
        text = link.text
        if url_on_page is not None:
            url_dict[text] = url_on_page
    return url_dict

def filter_attachments(pair: Tuple[str, Optional[str]]) -> bool:
    """
    Check if the given key-value pair contains "attachments" in the value.

    :param pair: Key-value pair of a dictionary.
    :return: True if "attachments" is in the value, False otherwise.
    """
    key, value = pair
    if value is not None:
        return "attachments" in value
    else:
        return False

def file_type(link: str) -> str:
    """
    Get the file extension from the input link.

    :param link: URL of the file.
    :return: File extension.
    """
    extension = link.split(".")[-1]
    return extension


def take_url_get_results(url: str) -> pd.DataFrame:
    """
    Scrape the input URL and return a dataframe containing the relevant information.

    :param url: URL of the webpage.
    :return: Dataframe containing the scraped information.
    """
    time.sleep(1)
    try:
        soup = get_soup(url)
    except Exception as e:
        print(f"Error for URL {url}: {e}")
        return pd.DataFrame()

    title = soup.find('title').get_text() if soup.find('title') else "missing"
    content = soup.find('meta', {'name': 'description'}).get('content') if soup.find('meta', {'name': 'description'}) else "missing"

    url_dict = get_links(soup)
    filtered_urls = dict(filter(filter_attachments, url_dict.items()))

    df = pd.DataFrame()
    df['Name'] = filtered_urls.keys()
    df['Link'] = filtered_urls.values()
    df['Title'] = title
    df['Extension'] = df['Link'].apply(file_type)
    df['Page Content'] = content
    df['Parent URL']=url
    return df


def crawl_site(url: str, depth: int = 3) -> set:
    """
    Crawl the given website and return the visited URLs.

    :param url: Starting URL.
    :param depth: Depth of the crawl.
    :return: Set of visited URLs.
    """
    visited_urls = set()

    def crawl(url: str, depth: int):
        nonlocal visited_urls
        if depth == 0:
            return
        if url in visited_urls:
            return
        visited_urls.add(url)
        try:
            response = requests.get(url, verify=False)
        except (requests.exceptions.MissingSchema, requests.exceptions.ConnectionError) as e:
            print(f"Error for URL {url}: {e}")
            return
        soup = BeautifulSoup(response.text, 'lxml')
        for link in soup.find_all('a'):
            try:
                href = link.get('href')
                if href:
                    href = urljoin(url, href)
                    crawl(href, depth - 1)
            except Exception as e:
                print(f"Error for link {link}: {e}")

    crawl(url, depth)
    return visited_urls


manual_urls = [
    "https://dcps.dc.gov/service/school-data",
    "https://opendata.dc.gov/",
    "https://octo.dc.gov/service/open-data",
    "https://dchealth.dc.gov/service/data-and-statistics"
    # Add more URLs as needed
]
search_results = manual_urls
# Crawl each site and de-duplicate the results
all_visited_urls = []
for url in search_results:
    visited_urls = crawl_site(url, depth=3)
    all_visited_urls.extend(visited_urls)


unique_visited_urls = deduplicate_urls(all_visited_urls)

# Filter only dc.gov URLs
pattern = re.compile(r"^https:\/\/(?:[a-z0-9-]+\.)?dc\.gov")
just_dc_urls = [url for url in unique_visited_urls if pattern.match(url)]

# Run the existing code on the filtered URLs
sub_dfs = [take_url_get_results(url) for url in just_dc_urls]
df = pd.concat(sub_dfs, ignore_index=True)
df.to_csv("sampleResults.csv")
pd.DataFrame(just_dc_urls).to_csv("urls.csv")
possiblyMissingUrls = [i for i in just_dc_urls if i not in set(df['Parent URL'])]
pd.DataFrame(possiblyMissingUrls).to_csv("urls with no files.csv")

"""

Examples of possibly missing URLs and if there were actually files there:
    
    1. https://dcps.dc.gov/page/health-and-wellness: no files
    2. https://dcps.dc.gov/page/dcps-teachers-and-educators: no files
    3. https://dcps.dc.gov/page/budget-and-finance: no files
    4. https://dcps.dc.gov/page/sustainable-schools: no files
    5. https://dcps.dc.gov/page/school-planning: no files
    6. https://dcps.dc.gov/food


"""