#!/usr/bin/env python
#
# @name:  main.py
# @create: 17 September 2014 (Wednesday)
# @update: 18 September 2014 (Wednesday)
# @author: Z. Huang
import logging
from mongoengine import connect
from crawler import Crawler
from crawler import QueueCrawler
from stackexchange import StackExchangeHandler
from stackexchange import StackExchangeTaskHandler
from stackexchange import task_queue


NUM_OF_THREADS = 4
FORMAT = '[%(levelname)s %(asctime)s] %(threadName)s: %(message)s'
logging.basicConfig(level=logging.NOTSET, format=FORMAT)
logger = logging.getLogger('Crawler')


def main():
    urls = [
        'http://matheducators.stackexchange.com/questions?page=1&sort=newest',
        'http://matheducators.stackexchange.com/questions?page=2&sort=newest',
    ]

    targets = {0: '#questions a.question-hyperlink', 1: 'div.pager-answers a'}

    connect('crawler-testing')

    for url in urls:
        crawler = Crawler(
            url, targets=targets,
            depth=3, handler=StackExchangeTaskHandler)
        crawler.start()
        crawler.join()

    for i in range(NUM_OF_THREADS):
        crawler = QueueCrawler(task_queue, StackExchangeHandler)
        crawler.start()
        crawler.join()

if __name__ == '__main__':
    main()
