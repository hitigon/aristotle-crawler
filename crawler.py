#!/usr/bin/env python
#
# @name:  crawler.py
# @create: 13 September 2014 (Saturday)
# @update: 13 September 2014 (Saturday)
# @author: Z. Huang
import logging
import threading
import urllib2
from urlparse import urlparse
from bs4 import BeautifulSoup

FORMAT = '[%(levelname)s %(asctime)s] %(threadName)s: %(message)s'
logging.basicConfig(level=logging.NOTSET, format=FORMAT)
logger = logging.getLogger('Crawler')


class Crawler(threading.Thread):

    def __init__(self, url, target_link=None, depth=2, handler=None):
        threading.Thread.__init__(self)
        self.url = url
        self.target_link = target_link
        self.depth = depth
        self.handler = handler
        parsed_url = self.parse_url()
        self.scheme = parsed_url.scheme
        self.netloc = parsed_url.netloc

    def run(self):
        logger.info('Starting...')
        self.crawler(self.url, self.depth)
        logger.info('Stopping...')

    def crawler(self, url, depth):
        if depth == 0:
            return
        try:
            logger.debug('crawling %s' % url)
            f = urllib2.urlopen(url)
            soup = BeautifulSoup(f)
            if self.handler:
                self.handler(soup, url, depth)
            if self.target_link:
                urls = soup.find_all('a', class_=self.target_link)
            else:
                urls = soup.find_all('a')
            for u in urls:
                u = self.fix_url(u.get('href'))
                self.crawler(u, depth - 1)
        except Exception as e:
            logger.error(e)
            return

    def parse_url(self):
        return urlparse(self.url)

    def fix_url(self, url):
        if not url:
            return ''
        if '//' not in url:
            url = ''.join([self.scheme, '://', self.netloc, url])
        return url


class QuestionObject(object):

    def __init__(self):
        self.id = 0
        self.author = None
        self.title = None
        self.content = None
        self.upvotecount = 0
        self.favoritecount = 0
        self.viewcount = 0
        self.tags = []
        self.comments = []
        self.time = None

    @classmethod
    def create(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
        return self


class AnswerObject(object):

    def __init__(self, _id, qid):
        self.id = _id
        self.qid = qid
        self.author = None
        self.content = None
        self.upvotes = 0
        self.accepted = False
        self.comments = []
        self.time = None


class StackExchangeHandler(object):

    def __init__(self, soup, url, depth):
        self.soup = soup
        self.url = url
        self.depth = depth
        if self.depth == 1:
            self.add_question()

    def add_question(self):
        question_tag = self.soup.select('#question')[0]
        post_tag = self.soup.select('td.postcell div.post-text')[0]
        qid = question_tag.get('data-questionid')
        title = self.soup.select('a.question-hyperlink')[0].string
        content = post_tag.contents
        tags = [
            tag.string for tag in post_tag.find_next_sibling().find_all('a')]
        question = QuestionObject.create(
            id=qid, title=title, content=content, tags=tags)
        logger.info('Added a question:' + question.title)
        return qid

if __name__ == '__main__':

    urls = [
        'http://matheducators.stackexchange.com/questions?page=1&sort=newest',
    ]

    for url in urls:
        crawler = Crawler(
            url, 'question-hyperlink', handler=StackExchangeHandler)
        crawler.start()
        crawler.join()
