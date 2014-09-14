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


class DataObject(object):

    @classmethod
    def create(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
        return self


class UserObject(DataObject):

    def __init__(self):
        self.id = -1
        self.username = None


class QuestionObject(DataObject):

    def __init__(self):
        self.id = -1
        self.author = None
        self.title = None
        self.content = None
        self.upvotecount = 0
        self.favoritecount = 0
        self.viewcount = 0
        self.tags = []
        self.comments = []
        self.askedtime = None
        self.activitytime = None


class AnswerObject(DataObject):

    def __init__(self):
        self.id = -1
        self.qid = -1
        self.author = None
        self.content = None
        self.upvotecount = 0
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
        title = self.soup.select('a.question-hyperlink')[0].string
        qid = question_tag.get('data-questionid')
        post_tag = question_tag.select('div.post-text')[0]
        content = post_tag.contents
        upvotecount = question_tag.select('span.vote-count-post')[0].string
        favoritecount = question_tag.select('div.favoritecount b')[0].string
        owner_tag = question_tag.select('td.owner')[0]
        user_tag = owner_tag.select('div.user-details')[0]
        user_link_tag = user_tag.select('a')

        if user_link_tag:
            tmp = user_link_tag[0].get('href').split('/')
            uid = tmp[2]
            username = tmp[3]
        else:
            uid = -1
            username = user_tag.contents[0].strip()
        author = UserObject.create(id=uid, username=username)
        info_tag = self.soup.select('#qinfo td p.label-key')
        askedtime = info_tag[1].get('title')
        viewcount = info_tag[3].select('b')[0].string.split(' ')[0]
        if len(info_tag) >= 6:
            activitytime = info_tag[5].select('a')[0].get('title')
        else:
            activitytime = 0
        if not favoritecount:
            favoritecount = 0
        tags = [
            tag.string for tag in post_tag.find_next_sibling().find_all('a')]
        question = QuestionObject.create(
            id=qid, title=title, content=content,
            tags=tags, upvotecount=int(upvotecount),
            favoritecount=int(favoritecount), author=author,
            viewcount=viewcount, askedtime=askedtime,
            activitytime=activitytime)
        logger.info('Added a question:' + str(question.title))
        return qid

    def add_answer(self, qid):
        pass

if __name__ == '__main__':

    urls = [
        'http://matheducators.stackexchange.com/questions?page=1&sort=newest',
    ]

    for url in urls:
        crawler = Crawler(
            url, 'question-hyperlink', handler=StackExchangeHandler)
        crawler.start()
        crawler.join()
