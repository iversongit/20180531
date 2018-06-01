import re
from enum import Enum, unique
from random import random
from threading import current_thread, Thread
from time import sleep
from urllib.parse import urlparse

import pymongo
import redis
import requests
from bs4 import BeautifulSoup


def decode_page(page_byte,charsets=('gb2312','utf-8','gbk')):
    html_page = None
    for charset in charsets:
        try:
            html_page = page_byte.decode(charset)
            break
        except UnicodeDecodeError as e:
            print("UnicodeDecodeError: ",e)
    return html_page

class Retry(object): # 装饰器类
    def __init__(self,*,retry_times=3,wait_secs=5,errors=(Exception,)):
        self.retry_times = retry_times
        self.wait_secs = wait_secs
        self.errors = errors

    def __call__(self,fn):
        def wrapper(*args, **kwargs):
            for _ in range(self.retry_times):
                try:
                    return fn(*args, **kwargs)
                except self.errors as e:
                    print("Retry: ",e)
                    sleep(self.wait_secs + random() * self.wait_secs)
        return wrapper

@unique
class SpiderStatus(Enum):
    IDLE = 0 # 空闲
    WORKING = 1 # 忙碌

redis_client = redis.Redis(host='47.106.134.92',port=6379,password='5201314')
mongo_client = pymongo.MongoClient(host='47.106.134.92',port=27017)
db = mongo_client.geyanw
geyanw_data = db.webpages

class Spider(object):
    def __init__(self):
        self.status = SpiderStatus.IDLE

    # 1、抓取页面
    @Retry()
    def fetch(self,current_url,*,charsets=('gb2312','utf-8','gbk'),user_agent=None,proxies=None):
        thread_name = current_thread().name
        # print(f'[{thread_name} Fetch]:{current_url}')
        headers = {'user_agent':user_agent} if user_agent else {}
        resp = requests.get(current_url,headers=headers,proxies=proxies)
        if resp.status_code == 200:
            return decode_page(resp.content,charsets)
        else:
            return None

    # 2、解析页面
    def parse(self,html_page,*,domain='geyanw.com'):
        soup = BeautifulSoup(html_page,'lxml')
        for a_tag in soup.body.findAll(name='a',attrs={"href":re.compile(r'/html/.*\.html$')}):
            parser = urlparse(a_tag.attrs['href'])
            scheme = parser.scheme or 'http'
            netloc = parser.netloc or domain
            path = parser.path
            full_url = f'{scheme}://{netloc}{path}'
            if not redis_client.sismember('visited_urls',full_url):
                redis_client.rpush('geyanw_task',full_url)

    # 3、抽取需要的数据
    def extract(self,html_page):
        soup = BeautifulSoup(html_page, 'lxml')
        title = soup.body.find(name='a', attrs={"href": re.compile(r'/index.html$')})
        title = title.getText()
        sub_title = soup.body.select_one('.title h2')
        sub_title = sub_title.getText()
        content = soup.body.select_one('.content')
        content = content.getText()
        if title and sub_title and content:
            return {'title':title,'sub_title':sub_title,'content':content}


    # 4、存储数据
    def store(self,data_dict):
        # print(data_dict)
        geyanw_data.insert_one({
            'title':data_dict['title'],
            'sub_title':data_dict['sub_title'],
            'content':data_dict['content']
        })
        print("ok")

class SpiderThread(Thread):
    def __init__(self,name,spider):
        super().__init__(name=name,daemon=True)
        self.spider = spider

    def run(self):
        while True:
            current_url = redis_client.lpop('geyanw_task')
            while not current_url:
                current_url = redis_client.lpop('geyanw_task')
            self.spider.status = SpiderStatus.WORKING
            if not redis_client.sismember('visited_urls',current_url):
                redis_client.sadd('visited_urls',current_url)
                html_page = self.spider.fetch(current_url)
                if html_page not in [None,'']:
                    self.spider.parse(html_page)
            visited_url = redis_client.spop('visited_urls')
            if visited_url != 'http://geyanw.com' and visited_url != 'nil':
                html_page = self.spider.fetch(visited_url)
                data_dict = self.spider.extract(html_page)
                self.spider.store(data_dict)
            self.spider.status = SpiderStatus.IDLE

def is_anySpiderAlive(spider_threads):
    return any([spider_thread.spider.status == SpiderStatus.WORKING for spider_thread in spider_threads])

def main():
    if not redis_client.exists('geyanw_task'):
        redis_client.rpush('geyanw_task','http://geyanw.com')
    spider_threads = [SpiderThread('t%d'%i,Spider()) for i in range(10)]
    for spider_thread in spider_threads:
        spider_thread.start()

    while redis_client.llen('geyanw_task') != 0 or is_anySpiderAlive(spider_threads):
        pass

    print("over")
if __name__ == '__main__':
    main()
