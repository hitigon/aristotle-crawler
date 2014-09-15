#!/usr/bin/env python
#
# @name:  crawler.py
# @create: 13 September 2014 (Saturday)
# @update: 14 September 2014 (Saturday)
# @author: Z. Huang
import logging
import threading
import urllib2
from urlparse import urlparse, urldefrag
from bs4 import BeautifulSoup

FORMAT = '[%(levelname)s %(asctime)s] %(threadName)s: %(message)s'
logging.basicConfig(level=logging.NOTSET, format=FORMAT)
logger = logging.getLogger('Crawler')


class Crawler(threading.Thread):

    def __init__(self, url, target=None, targets=None, depth=1, handler=None):
        threading.Thread.__init__(self)
        self.url = url
        self.target = target
        self.targets = targets
        self.depth = depth
        self.handler = handler
        parsed_url = urlparse(self.url)
        self.scheme = parsed_url.scheme
        self.netloc = parsed_url.netloc
        self.visited = set()

    def run(self):
        logger.info('Starting...')
        self.crawler(self.url, 0)
        logger.info('Stopping...')

    def crawler(self, url, depth):
        if depth == self.depth:
            return
        url = self.unique_url(url)
        if url in self.visited:
            return
        try:
            logger.debug('crawling %s at %d' % (url, depth))
            f = urllib2.urlopen(url)
            self.visited.add(url)
            soup = BeautifulSoup(f)
            if self.handler:
                self.handler(soup, url, depth)
            if self.targets and depth in self.targets:
                urls = soup.select(self.targets[depth])
            elif self.target:
                urls = soup.find_all('a', class_=self.target)
            else:
                urls = soup.find_all('a')
            for u in urls:
                u = self.fix_url(u.get('href'))
                self.crawler(u, depth + 1)
        except Exception as e:
            import sys
            exc_type, exc_obj, exc_tb = sys.exc_info()
            logger.error('%s at %r' % (str(e), exc_tb.tb_lineno))
            return

    def fix_url(self, url):
        if not url:
            return ''
        if '//' not in url:
            url = ''.join([self.scheme, '://', self.netloc, url])
        return url

    def unique_url(self, url):
        return urldefrag(url)[0]


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
        self.wiki = False


class AnswerObject(DataObject):

    def __init__(self):
        self.id = -1
        self.qid = -1
        self.author = None
        self.content = None
        self.upvotecount = 0
        self.accepted = False
        self.comments = []
        self.answeredtime = None
        self.wiki = False


class CommentObject(DataObject):

    def __init__(self):
        self.id = -1
        self.content = None
        self.user = None
        self.score = 0
        self.commenttime = None


class StackExchangeHandler(object):

    def __init__(self, soup, url, depth):
        self.soup = soup
        self.url = url
        self.depth = depth
        if self.depth == 1:
            qid = self.add_question()
            self.add_answer(qid)
        elif self.depth == 2:
            qid = int(url.split('/')[4])
            self.add_answer(qid)

    def add_question(self):
        question_tag = self.soup.select('#question')[0]
        title = self.soup.select('a.question-hyperlink')[0].string
        qid = question_tag.get('data-questionid')
        post_tag = question_tag.select('div.post-text')[0]
        content = post_tag.contents
        upvotecount = question_tag.select('span.vote-count-post')[0].string
        favoritecount = question_tag.select('div.favoritecount b')[0].string
        owner_tag = question_tag.select('td.owner')
        wiki = False
        if owner_tag:
            user_tag = owner_tag[0].select('div.user-details')[0]
            user_link_tag = user_tag.select('a')
            if user_link_tag:
                tmp = user_link_tag[0].get('href').split('/')
                uid = tmp[2]
                username = tmp[3]
            else:
                uid = -1
                username = user_tag.contents[0].strip()
        else:
            wiki = True
            wiki_tag = question_tag.select('div.user-info')[-1]
            user_tag = wiki_tag.select('div.user-details')[-1]
            user_link_tag = user_tag.select('a')
            if len(user_link_tag) == 2:
                tmp = user_link_tag[-1].get('href').split('/')
                uid = tmp[2]
                username = user_link_tag[-1].string.strip()
            else:
                uid = -1
                username = user_link_tag[0].string.strip()
        author = UserObject.create(id=int(uid), username=username)
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
        comments_tag = question_tag.select(
            'div.comments')[0].select('tr.comment')
        comments = self.add_comment(comments_tag)
        question = QuestionObject.create(
            id=int(qid), title=title, content=content,
            tags=tags, upvotecount=int(upvotecount),
            favoritecount=int(favoritecount), author=author,
            viewcount=viewcount, askedtime=askedtime,
            activitytime=activitytime, comments=comments,
            wiki=wiki)
        logger.info('Added a question: %r' % question.title)
        return int(qid)

    def add_answer(self, qid):
        answers_tag = self.soup.select('#answers')[0].select('div.answer')
        for answer_tag in answers_tag:
            aid = answer_tag.get('data-answerid')
            post_tag = answer_tag.select('div.post-text')[0]
            content = post_tag.contents
            upvotecount = answer_tag.select('span.vote-count-post')[0].string
            accepted_tag = answer_tag.select('span.vote-accepted-on')
            if accepted_tag:
                accepted = True
            else:
                accepted = False
            # there is a bug for certain posts that cannot find any .user_info
            owner_tag = answer_tag.select('div.user-info')[-1]
            user_action_tag = owner_tag.select('div.user-action-time')
            user_tag = owner_tag.select('div.user-details')[-1]
            user_link_tag = user_tag.select('a')
            answeredtime = None
            wiki = False
            if user_action_tag:
                answeredtime = user_action_tag[
                    -1].select('span')[0].get('title')
                if user_link_tag:
                    tmp = user_link_tag[0].get('href').split('/')
                    uid = tmp[2]
                    username = tmp[3]
                else:
                    uid = -1
                    username = user_tag.contents[0].strip()
            else:
                wiki = True
                if len(user_link_tag) == 2:
                    tmp = user_link_tag[-1].get('href').split('/')
                    uid = tmp[2]
                    username = user_link_tag[-1].string.strip()
                else:
                    uid = -1
                    username = user_link_tag[0].string.strip()
            author = UserObject.create(id=int(uid), username=username)
            comments_tag = answer_tag.select(
                'div.comments')[0].select('tr.comment')
            comments = self.add_comment(comments_tag)
            answer = AnswerObject.create(
                id=int(aid), qid=qid, accepted=accepted,
                content=content, upvotecount=int(upvotecount),
                author=author, answeredtime=answeredtime,
                comments=comments, wiki=wiki)
            logger.info('Added an answer by: %s' % answer.author.username)

    def add_comment(self, comments_tag):
        comments = []
        for comment_tag in comments_tag:
            mid = comment_tag.get('id').split('-')[1]
            comment_body_tag = comment_tag.select('div.comment-body')[0]
            content = comment_body_tag.select('span.comment-copy')[0].contents
            user_tag = comment_body_tag.select('a.comment-user')
            if user_tag:
                tmp = user_tag[0].get('href').split('/')
                uid = tmp[2]
                username = tmp[3]
            else:
                uid = -1
                username = comment_body_tag.select(
                    'span.comment-user')[0].string.strip()
            user = UserObject.create(id=int(uid), username=username)
            score_tag = comment_tag.select('td.comment-score span.cool')
            if score_tag:
                score = score_tag[0].string
            else:
                score = 0
            commenttime = comment_body_tag.select(
                'span.comment-date span')[0].get('title')
            comment = CommentObject.create(
                id=int(mid), user=user, score=int(score),
                content=content, commenttime=commenttime)
            comments.append(comment)
            logger.info('Added a comment by: %s' % comment.user.username)
        return comments


if __name__ == '__main__':

    urls = [
        'http://math.stackexchange.com/questions?page=1&sort=votes',
    ]

    targets = {0: 'a.question-hyperlink', 1: 'div.pager-answers a'}

    for url in urls:
        crawler = Crawler(
            url, targets=targets,
            depth=3, handler=StackExchangeHandler)
        crawler.start()
        crawler.join()
