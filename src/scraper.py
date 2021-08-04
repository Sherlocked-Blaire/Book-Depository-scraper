import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import math
from typing import Union
import pandas as pd
import re
import datetime
import time
from random import randint
from dateutil import parser

user_agent = UserAgent()
headers = {"User-Agent": user_agent.random}

def count_pages(number_of_items: Union[int,float], keyword=None )-> int: 
    ''' 
    Finds the number of pages   that should be scrapped to get the number of items from the website
    Returns the number of pages that should be scrapped to get the number of items
    ''' 
    
    if number_of_items> find_max_item(keyword):
        no_of_pages = math.ceil(find_max_item(keyword) / 30)
    no_of_pages = math.ceil(abs(number_of_items) / 30)
    return no_of_pages

def find_max_item(keyword: str):
    '''
    Finds the maximal number of records  that can be scraped given the arguments for a particular keyword 
    Returns the  max number of items of the category  that can be scrapped from the website
    '''
    
    url = f"https://www.bookdepository.com/search?searchTerm={keyword}&page=1"
    page = requests.get (url, headers =headers)
    soup = BeautifulSoup(page.content, "html.parser")
    max_soup = soup.find("div", class_ = "search-info")
    max_item =  int([number.text for number in max_soup.find_all("span", class_="search-count")][0].replace(',',''))
    return max_item

def generate_urls(keyword:str, number_of_items:int)-> list:
    """Generates urls for for all the pages to be scraped given
    Returns a list of urls"""
    urls = []
    if  number_of_items >= find_max_item(keyword):
        number_of_pages = count_pages(find_max_item(keyword))
        
        for page_number in range(1,number_of_pages+1):
            url = f"https://www.bookdepository.com/search?searchTerm={keyword}&page={page_number}"
            urls.append(url)       
    else:
        number_of_pages = count_pages(number_of_items)
        for page_number in range(1,number_of_pages+1):
            url = f"https://www.bookdepository.com/search?searchTerm={keyword}&page={page_number}"
            urls.append(url)
    return urls   
   
def extract(url:str)->dict:
    '''Extracts necessary fields from the web page
        Returns dictionary containing scrapped fields '''
    page = requests.get(url, headers=headers)
    soup = BeautifulSoup(page.content, "html.parser")
    book_items = soup.find_all("div", class_="book-item")
    
    urls_to_image = []
    urls_to_item = []
    date_published = []
    titles = []
    prices = []
    
    
    for  item in book_items:
        image = item.find("div", class_="item-img")
        image_url =  image and image.find("img", class_="lazy").get("data-lazy") or ''
        urls_to_image.append(image_url)
        date = item.find("p", class_="published")
        date =  date  and date.text.strip() or ''
        date_published.append(date)
        title = item.find("h3", class_="title")
        title =  title and title.text.strip() or ''
        titles.append(title)
        url_to_item = item.select("h3.title a[href]")
        url_to_item = url_to_item and "https://www.bookdepository.com"+ url_to_item[0]["href"] or ''
        urls_to_item.append(url_to_item)
        price = item.find("p", class_="price")
        price = price and price.text.strip()[0:8] or '0'
        prices.append(price)
      
    dict_of_scrapped_items = {
        'book_title':titles,
        'book_price':prices,
        'book_url':urls_to_item,
        'image_url':urls_to_image,
        'published_date':date_published
    }
    return  dict_of_scrapped_items


def transform(df:pd.DataFrame)->pd.DataFrame:
    '''Cleans price and published date coulmns 
    Returns cleaned dataframe'''
    try:
        df['book_price'] = df['book_price'].apply(lambda x :re.sub("[^0-9 ,.]", "",x).replace(",", "."))
        df['Published_date'] = df['Published_date'].apply(lambda x: parser.parse(x) )
        df['category'] = keyword
    except:
        df['book_price'] = df['book_price']
        df['published_date'] = df['published_date']
       
    return df 

  
if __name__ == '__main__':
    keyword = str(input('what category of books do you want to scrape?\n'))
    number_of_items = int(input("how many items do you want to scrape?\n"))
    result = []
    urls = generate_urls(keyword, number_of_items)

    data = {
        'book_title':[],
        'book_price': [],
        'book_url':[],
        'image_url':[],
        'published_date':[]
    }
    page_number = 1
    for url in urls:
       
        print(f"**********************Scraping page {page_number}***************")
        data_dict = extract(url)
        data['book_title'].extend(data_dict['book_title'])
        data['book_price'].extend(data_dict['book_price'])
        data['book_url'].extend(data_dict['book_url'])
        data['image_url'].extend(data_dict['image_url'])
        data['published_date'].extend(data_dict['published_date'])
        time.sleep(randint(1, 5))
        page_number += 1
    df = pd.DataFrame(data)     
    df = transform(df)
    df['category'] = keyword
    df.to_csv('df.csv', index=False)