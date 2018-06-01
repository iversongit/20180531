import logging

from random import random
from urllib.parse import urlparse
from time import sleep
from enum import Enum, unique
from queue import Queue
from threading import Thread, current_thread
from bs4 import BeautifulSoup

import requests

def decode_page(page_byte,charsets=('utf-8',)):
    html = None
    for charset in charsets:
        try:
            html = page_byte.decode(charset)
            break
        except UnicodeDecodeError as e:
            print("UnicodeDecodeError: ",e)
    return html

class Retry(object): # 装饰器类
    def __init__(self, *, retry_times=3,wait_secs=5,errors=(Exception,)):
        # retry_times - - 重试次数，wait_seconds - - 避让时间
        self.retry_times = retry_times
        self.wait_secs = wait_secs
        self.errors = errors

    # 魔法方法，将类的对象当函数调用 -- object()
    def __call__(self,fn):
        def wrapper(*args, **kwargs):
            for _ in range(self.retry_times):
                try:
                    return fn(*args, **kwargs)
                # 异常可一次跟多个
                except self.errors as e:
                    logging.error(e)
                    logging.info('[Retry]')
                    sleep((random()+1) * self.wait_secs)
        return wrapper

@unique  # 里面的值不能重复，独一无二的
class SpiderStatus(Enum):
    IDLE = 0 # 空闲
    WORKING = 1 # 忙碌

# 定义爬虫类
class Spider(object):
    def __init__(self):
        self.status = SpiderStatus.IDLE # 爬虫状态

    # 1、抓取页面
    @Retry()
    def fetch(self,current_url,*,charsets=('utf-8',),user_agent=None,proxies=None):
        thread_name = current_thread().name
        print(f'[{thread_name} Fetch]:{current_url}')
        # logging.info('[Fetch]:' + current_url)
        headers = {'user_agent':user_agent} if user_agent else {}
        # if current_url.startswith('http://m.sohu.com/'):
        resp = requests.get(current_url,headers=headers,proxies=proxies)
        if resp.status_code == 200:
            return decode_page(resp.content,charsets)
        else:
            return None

    # 2、解析页面
    def parse(self,html_page,*,domain='m.sohu.com'):
        soup = BeautifulSoup(html_page,'lxml')
        link_list = []
        for a_tag in soup.body.select('a[href]'):
            # 对url进行拆解
            parser = urlparse(a_tag.attrs['href'])
            netloc = parser.netloc or domain
            scheme = parser.scheme or 'http'
            if netloc == domain and scheme != 'javascript':
                # 访问不到http会自动定向到https
                path = parser.path
                query = '?' + parser.query if parser.query else ''
                # 格式化
                full_url = f'{scheme}://{netloc}{path}{query}'
                if full_url not in visited_urls:
                    link_list.append(full_url)
        return link_list

    # 3、抽取需要的数据
    def extract(self,html_page):
        pass

    # 4、存储数据
    def store(self,data_dict):
        pass

# target -- 回调函数 args -- 传递给回调函数的参数
# Thread(target=foo,args=(xx,xx,xx)).start()

# 自创的多线程类
class SpiderThread(Thread):
    def __init__(self,name,spider,task_queue):
        # daemon=True -- 守护线程，主程序停，线程停，不设置则线程不会停止
        super().__init__(name=name,daemon=True) #　创建线程必须要调用父类的__init__方法，否则创建失败
        self.spider = spider
        self.task_queue = task_queue # 待爬取的页面

    # 回调方法
    def run(self):
        while True:
            # 阻塞队列 -- 取任务，如果取不到，会阻塞住
            #　　　　　　放任务，如果别人在放，或放满了，会阻塞住
            current_url = self.task_queue.get()
            visited_urls.add(current_url)
            # 将爬虫状态设置为工作状态
            self.spider.status = SpiderStatus.WORKING
            # 爬取当前页面
            html_page = self.spider.fetch(current_url)
            # 如果页面存在，解析页面,将其中的url加入到任务队列中
            if html_page not in [None,'']:
                link_list = self.spider.parse(html_page)
                for link in link_list:
                    self.task_queue.put(link)
            # 任务执行完毕后，爬虫的状态设置为空闲状态
            self.spider.status = SpiderStatus.IDLE

def is_anySpider_alive(spider_threads):
    # any（全局函数） -- 列表有一个true 整个结果就是true,全都是false 则为false
    # all（全局函数）-- 列表有一个false整个结果就是false,全都是true 则为true
    return any([spider_thread.spider.status == SpiderStatus.WORKING
                for spider_thread in spider_threads])

visited_urls = set() # 存放访问过的网址
def main():
    task_queue = Queue() # 队列有锁，线程安全的，而列表没锁
    # 将种子url放到任务队列中
    task_queue.put('http://m.sohu.com/') # FIFO 队首出(get)，队尾放(put)
    # 创建十个线程,每个线程一个spider，所有线程共用一个任务队列
    spider_threads = [ SpiderThread('t%d' % i,Spider(),task_queue) for i in range(10)]
    # 将每个线程启动起来
    for spider_thread in spider_threads:
        spider_thread.start()

    # 队列不是空的或有爬虫还在工作，则不允许停
    while not task_queue.empty() or is_anySpider_alive(spider_threads):
        pass

    print("Over")

if __name__ == '__main__':
    main()