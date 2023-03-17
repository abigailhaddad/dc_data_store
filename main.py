import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin
import time
from urllib3.exceptions import InsecureRequestWarning
from urllib3 import disable_warnings

disable_warnings(InsecureRequestWarning)


def get_soup(url: str) -> BeautifulSoup:
    """
    Get the soup object of the given URL.

    :param url: URL of the webpage.
    :return: Soup object of the webpage.
    """
    reqs = requests.get(url, verify=False)
    soup = BeautifulSoup(reqs.text, 'html.parser')
    return soup


def get_links(soup: BeautifulSoup) -> dict:
    """
    Extract all the links from the soup object and return them in a dictionary.

    :param soup: Soup object of the webpage.
    :return: Dictionary containing the text and link of each <a> element.
    """
    url_dict = {}
    for link in soup.find_all('a'):
        url_on_page = link.get('href')
        text = link.text
        url_dict[text] = url_on_page
    return url_dict


def filter_attachments(pair: tuple) -> bool:
    """
    Check if the "attachments" keyword is present in the value of the input tuple.

    :param pair: Tuple containing key and value.
    :return: True if "attachments" is present in the value, False otherwise.
    """
    _, value = pair
    return "attachments" in value


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
        soup = BeautifulSoup(response.text, 'html.parser')
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


visited_urls = crawl_site("https://dcps.dc.gov/service/school-data", depth=3)
just_dc_urls = [i for i in visited_urls if i.startswith("https://dcps.dc.gov")]
sub_dfs = [take_url_get_results(url) for url in just_dc_urls]
df = pd.concat(sub_dfs, ignore_index=True)
df.to_csv("sampleResults.csv")
pd.DataFrame(just_dc_urls).to_csv("urls.csv")
