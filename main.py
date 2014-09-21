#!/usr/bin/env python
#
# @name:  main.py
# @create: 17 September 2014 (Wednesday)
# @update: 20 September 2014 (Wednesday)
# @author: Z. Huang
import logging
from Queue import Queue
from mongoengine import connect
from crawler import Crawler
from stackexchange import StackExchangeHandler

NUM_OF_THREADS = 4
FORMAT = '[%(levelname)s %(asctime)s] %(threadName)s: %(message)s'
logging.basicConfig(level=logging.NOTSET, format=FORMAT)
logger = logging.getLogger('Crawler')

MATH_URL = 'http://matheducators.stackexchange.com/questions?page={0}&sort=newest'
PAGE_START = 1
PAGE_END = 1


def main():
    connect('crawler-testing')
    urls = []
    targets = {0: '#questions a.question-hyperlink', 1: 'div.pager-answers a'}

    for i in range(PAGE_START, PAGE_END + 1):
        urls.append(MATH_URL.format(i))

    task_threads = []
    queue = Queue()
    output = Queue()
    for url in urls:
        queue.put(url)

    for i in range(NUM_OF_THREADS):
        crawler = Crawler(queue, output, targets=targets, depth=3)
        crawler.start()
        task_threads.append(crawler)

    for task in task_threads:
        task.join()

    task_threads = []
    for i in range(NUM_OF_THREADS):
        crawler = Crawler(output, handler=StackExchangeHandler)
        crawler.start()
        task_threads.append(crawler)

    for task in task_threads:
        task.join()

if __name__ == '__main__':
    main()
