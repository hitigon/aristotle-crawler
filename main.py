#!/usr/bin/env python
#
# @name:  main.py
# @create: 17 September 2014 (Wednesday)
# @update: 22 September 2014 (Wednesday)
# @author: Z. Huang
import argparse
import logging
from Queue import Queue
from mongoengine import connect
from crawler import Crawler, dummy_handler
from stackexchange import StackExchangeHandler


FORMAT = '[%(levelname)s %(asctime)s] %(threadName)s: %(message)s'
logging.basicConfig(level=logging.NOTSET, format=FORMAT)
logger = logging.getLogger('Crawler')

args_parser = argparse.ArgumentParser(description='Aristotle Crawler')
args_parser.add_argument('-t', '--thread', action='store',
                         type=int, default=4,
                         help='number of threads')
args_parser.add_argument('--start', action='store',
                         type=int, default=1,
                         help='number of starting page')
args_parser.add_argument('--end', action='store',
                         type=int, default=1,
                         help='number of ending page')
args_parser.add_argument('-m', '--mongo', action='store',
                         default='crawler-testing',
                         help='mongodb name')
args_parser.add_argument('-u', '--url', action='store',
                         help='targeted url template')
args_parser.add_argument('-a', '--apply', action='store',
                         default='stackexchange',
                         choices=['stackexchange', 'dummy'],
                         help='Apply a crawling task')


def main():
    args = args_parser.parse_args()
    if not args.url:
        print('please use --help for command line help')
        return

    try:
        connect(args.mongo)
    except Exception as e:
        import sys
        logging.error(str(e))
        sys.exit()

    if args.apply == 'stackexchange':
        depth = 3
        targets = {
            0: '#questions a.question-hyperlink',
            1: 'div.pager-answers a'
        }
        handler = StackExchangeHandler
    else:
        depth = 2
        targets = {
            0: 'a'
        }
        handler = dummy_handler

    urls = []
    for i in range(args.start, args.end + 1):
        urls.append(args.url.format(i))

    task_threads = []
    queue = Queue()
    output = Queue()
    for url in urls:
        queue.put(url)

    for i in range(args.thread):
        crawler = Crawler(queue, output, targets=targets, depth=depth)
        crawler.start()
        task_threads.append(crawler)

    for task in task_threads:
        task.join()

    task_threads = []
    for i in range(args.thread):
        crawler = Crawler(output, handler=handler)
        crawler.start()
        task_threads.append(crawler)

    for task in task_threads:
        task.join()

if __name__ == '__main__':
    main()
