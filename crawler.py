#!/usr/bin/env python
#
# @name:  crawler.py
# @create: 13 September 2014 (Saturday)
# @update: 22 September 2014 (Saturday)
# @author: Z. Huang
import logging
import threading
import urllib2
from urlparse import urlparse, urldefrag
from bs4 import BeautifulSoup

logger = logging.getLogger('Crawler')


class Crawler(threading.Thread):

    def __init__(self, queue, output=None, target=None, targets=None, depth=1, handler=None):
        threading.Thread.__init__(self)
        self.queue = queue
        self.target = target
        self.targets = targets
        self.depth = depth
        self.handler = handler
        self.output = output
        self.visited = set()

    def run(self):
        while True:
            if self.queue.empty():
                break
            logger.info('Starting...')
            if self.handler:
                soup, url, depth = self.queue.get()
                self.handler(soup, url, depth)
            else:
                url = self.queue.get()
                self.crawler(url, 0)
            self.queue.task_done()
            logger.info('Stopping...')

    def crawler(self, url, depth):
        if depth == self.depth:
            return
        parsed_url = urlparse(url)
        scheme = parsed_url.scheme
        netloc = parsed_url.netloc
        url = self.unique_url(url)
        if url in self.visited:
            return
        try:
            logger.debug('crawling %s at %d' % (url, depth))
            f = urllib2.urlopen(url)
            self.visited.add(url)
            soup = BeautifulSoup(f)
            if self.output and self.depth >= 1:
                self.output.put((soup, url, depth))
            if self.targets and depth in self.targets:
                urls = soup.select(self.targets[depth])
            elif self.target:
                urls = soup.find_all('a', class_=self.target)
            else:
                urls = soup.find_all('a')
            for u in urls:
                u = self.fix_url(u.get('href'), scheme, netloc)
                self.crawler(u, depth + 1)
        except Exception as e:
            import sys
            exc_type, exc_obj, exc_tb = sys.exc_info()
            logger.error('%s at %r' % (str(e), exc_tb.tb_lineno))
            return

    def fix_url(self, url, scheme, netloc):
        if not url:
            return ''
        if '//' not in url:
            url = ''.join([scheme, '://', netloc, url])
        elif ':' not in url:
            url = ''.join([scheme, ':', url])
        return url

    def unique_url(self, url):
        return urldefrag(url)[0]


def dummy_handler(soup, url, depth):
    logger.info('Handling {0} at {1}'.format(url, depth))
