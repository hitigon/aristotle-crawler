#!/usr/bin/env python
#
# @name:  stackexchange.py
# @create: 17 September 2014 (Wednesday)
# @update: 17 September 2014 (Wednesday)
# @author: Z. Huang
import logging
from mongoengine import Document, EmbeddedDocument
from mongoengine.fields import StringField, IntField
from mongoengine.fields import ReferenceField, ListField
from mongoengine.fields import BooleanField, EmbeddedDocumentField

logger = logging.getLogger('Crawler')


class User(Document):
    uid = IntField()
    username = StringField()


class Comment(EmbeddedDocument):
    cid = IntField()
    user = ReferenceField(User)
    content = StringField()
    score = IntField()
    commenttime = StringField()


class Question(Document):
    qid = IntField()
    author = ReferenceField(User)
    title = StringField()
    content = StringField()
    upvotecount = IntField()
    favoritecount = IntField()
    viewcount = IntField()
    tags = ListField(StringField())
    comments = ListField(EmbeddedDocumentField(Comment))
    askedtime = StringField()
    activitytime = StringField()
    wiki = BooleanField()


class Answer(Document):
    aid = IntField()
    question = ReferenceField(Question)
    author = ReferenceField(User)
    content = StringField()
    upvotecount = IntField()
    accepted = BooleanField()
    comments = ListField(EmbeddedDocumentField(Comment))
    answeredtime = StringField()
    wiki = BooleanField()


class StackExchangeHandler(object):

    def __init__(self, soup, url, depth):
        self.soup = soup
        self.url = url
        self.depth = depth
        if self.depth == 1:
            qid = self.add_question()
            if qid != -1:
                self.add_answer(qid)
        elif self.depth == 2:
            qid = int(url.split('/')[4])
            self.add_answer(qid)

    def add_question(self):
        question_tag = self.soup.select('#question')[0]
        title = self.soup.select('a.question-hyperlink')[0].string
        qid = question_tag.get('data-questionid')
        post_tag = question_tag.select('div.post-text')[0]
        content = str(post_tag)
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
        uid = int(uid)
        username = username.encode('utf-8')
        author = User.objects(uid=uid).first()
        if uid == -1 or not author:
            author = User(uid=uid, username=username).save()
        info_tag = self.soup.select('#qinfo td p.label-key')
        askedtime = info_tag[1].get('title')
        viewcount = info_tag[3].select('b')[0].string.split(' ')[0]
        activitytime = ''
        if len(info_tag) >= 6:
            activitytime = info_tag[5].select('a')[0].get('title')
        if not favoritecount:
            favoritecount = 0
        tags = [
            tag.string for tag in post_tag.find_next_sibling().find_all('a')]
        comments_tag = question_tag.select(
            'div.comments')[0].select('tr.comment')
        comments = self.add_comment(comments_tag)
        if Question.objects(qid=int(qid)):
            logger.debug('Question %s is in the record' % qid)
            return -1
        question = Question(
            qid=int(qid), title=title, content=content,
            tags=tags, upvotecount=int(upvotecount),
            favoritecount=int(favoritecount), author=author,
            viewcount=viewcount, askedtime=askedtime,
            activitytime=activitytime, comments=comments,
            wiki=wiki).save()
        logger.info('Added a question: %r' % question.title)
        return int(qid)

    def add_answer(self, qid):
        answers_tag = self.soup.select('#answers')[0].select('div.answer')
        for answer_tag in answers_tag:
            aid = answer_tag.get('data-answerid')
            post_tag = answer_tag.select('div.post-text')[0]
            content = str(post_tag)
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
            answeredtime = ''
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
            uid = int(uid)
            username = username.encode('utf-8')
            author = User.objects(uid=uid).first()
            if uid == -1 or not author:
                author = User(uid=uid, username=username).save()
            comments_tag = answer_tag.select(
                'div.comments')[0].select('tr.comment')
            comments = self.add_comment(comments_tag)
            question = Question.objects(qid=qid).first()
            if Answer.objects(aid=int(aid)):
                logger.debug('Answer %s is in the record' % aid)
                return
            answer = Answer(
                aid=int(aid), question=question, accepted=accepted,
                content=content, upvotecount=int(upvotecount),
                author=author, answeredtime=answeredtime,
                comments=comments, wiki=wiki).save()
            logger.info('Added an answer by: %s' % answer.author.username)

    def add_comment(self, comments_tag):
        comments = []
        for comment_tag in comments_tag:
            cid = comment_tag.get('id').split('-')[1]
            comment_body_tag = comment_tag.select('div.comment-body')[0]
            content = str(comment_body_tag.select('span.comment-copy')[0])
            user_tag = comment_body_tag.select('a.comment-user')
            if user_tag:
                tmp = user_tag[0].get('href').split('/')
                uid = tmp[2]
                username = tmp[3]
            else:
                uid = -1
                username = comment_body_tag.select(
                    'span.comment-user')[0].string.strip()
            uid = int(uid)
            username = username.encode('utf-8')
            user = User.objects(uid=uid).first()
            if uid == -1 or not user:
                user = User(uid=uid, username=username).save()
            score_tag = comment_tag.select('td.comment-score span.cool')
            if score_tag:
                score = score_tag[0].string
            else:
                score = 0
            commenttime = comment_body_tag.select(
                'span.comment-date span')[0].get('title')
            comment = Comment(
                cid=int(cid), user=user, score=int(score),
                content=content, commenttime=commenttime)
            comments.append(comment)
            logger.info('Added a comment by: %s' % comment.user.username)
        return comments
