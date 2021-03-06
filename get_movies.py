#!/usr/bin/env python
# encoding: utf-8
#
# Copyright (c) 2018 alibaba-inc. All Rights Reserved
#

"""
File: get_movies.py
Date: 2019-01-05 21:22
Author: wang.gaofei@alibaba-inc.com
"""
import os, sys, time, datetime, json
import requests, urlparse, urllib, cookielib
sys.path.append('/Users/wang.gaofei/git/my_code/')
from pyquery import PyQuery as pq
from tools.super_queue import Super_Queue
from tools.log_config import init_log
from my_db import connect_db, close_db, MoviesTable
from config import *

reload(sys)
sys.setdefaultencoding('utf-8')

file_path = os.path.abspath(os.path.dirname(__file__)) + "/logs/"
log = init_log('get_movie',file_path)

class Movies():
    """
    获取电影信息
    """
    def __init__(self):
        self.session = requests.Session()
        self.session.cookies = cookielib.LWPCookieJar()
        self.db_session = connect_db()

    # 提取url参数的函数
    def url_to_Dict(self, url):
        query = urlparse.urlparse(url).query
        return dict([(k, v[0]) for k, v in urlparse.parse_qs(query).items()])

    def get_movie_show_time(self, movie):
        # 获取电影简介，查找上映时间
        html = pq(movie['link'],encoding="utf-8")
        text = html('div').filter('.cont-time').html()
        date = text.split('：')[1].split(' ')[0]
        if len(date) == 7:
            date += "-01"
        if len(date) == 4:
            date += "-01-01"
        return date

    def get_movie_play_number(self, movie,date):
        # 获取电影id，查看比高电影院最近三天看是否有播放；
        # https://dianying.taobao.com/showDetailSchedule.htm?showId=231277&cinemaId=53124&date=2019-01-05&regionName=%E6%9C%9D%E9%98%B3%E5%8C%BA&ts=1546702245251&n_s=new
        show_id = self.url_to_Dict(movie['link']).get('showId')
        show_schedule_url = "https://dianying.taobao.com/showDetailSchedule.htm"
        get_data = {
            'showID': show_id,
            'date': date,
            'cinemaId': 53124,  # 这是比高的id
            'regionName': u'朝阳区',  # 这是区域
            'ts': int(round(time.time() * 1000)),  # 13位的时间戳
            'n_s': 'new',
        }
        show_schedule_url += "?" + urllib.urlencode(get_data)

        html = pq(show_schedule_url,encoding='utf-8')
        movies_divs = html('tbody').eq(0).find('tr')
        movie_number = 0
        while True:
            div = movies_divs.eq(movie_number)
            if not div:
                break
            movie_number += 1

        return movie_number

    def get_movie_hot(self, movie):
        now = datetime.datetime.now()
        play_number = 0
        for day in range(3):
            date = datetime.datetime.strftime(now + datetime.timedelta(days=day), '%Y-%m-%d')
            play_number += self.get_movie_play_number(movie, date)
        return play_number

    def get_movie_score(self, div):
        score = div('span').filter('.bt-r').html()
        if not score:
            score = 0
        return float(score)

    def get_movie_down_status(self, keyword):
        """
        True 代表还在电影院，False 是已经下映了。
        :param keyword:
        :return:
        """
        def _get_baidu_status(keyword):
            headers = {  # 发送HTTP请求时的HEAD信息，用于伪装为浏览器
                'Connection': 'Keep-Alive',
                'Accept': 'text/html, application/xhtml+xml, */*',
                'Accept-Language': 'zh,en;q=0.9,zh-CN;q=0.8,zh-HK;q=0.7',
                'Accept-Encoding': 'gzip, deflate',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36'
            }

            url = u'https://www.baidu.com/baidu?wd=%s&tn=monline_dg&ie=utf-8' % keyword
            ret = ""
            for i in range(10):
                try:
                    ret = requests.get(url,headers=headers)
                    ret.encoding = 'utf-8'
                    break
                except Exception as e:
                    print e.message
            if i == 10:
                return True

            html = pq(ret.content)
            movie_div = html('div').filter('.c-result-content').eq(0)
            # 查看立即播放按钮
            if movie_div:
                button_text = movie_div('div').filter('.dis-overflow-none.c-gap-bottom-small').eq(0).text()
                if u'立即播放' in button_text or u'付费观看' in button_text:
                    # 获取分钟数
                    info = movie_div('span').filter('.c-gray').text()
                    for node in info.split('|'):
                        if u'分钟' in node:
                            minute_number = node.split(u'分')[0]
                            if minute_number > 30:
                                return False
            return True

        def _get_360so_status(keyword):
            headers = {  # 发送HTTP请求时的HEAD信息，用于伪装为浏览器
                'Connection': 'Keep-Alive',
                'Accept': 'text/html, application/xhtml+xml, */*',
                'Accept-Language': 'zh,en;q=0.9,zh-CN;q=0.8,zh-HK;q=0.7',
                'Accept-Encoding': 'gzip, deflate',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36'
            }

            url = u"https://www.so.com/s?ie=utf-8&fr=none&src=360sou_newhome&q=%s" % keyword
            ret = ""
            for i in range(10):
                try:
                    ret = requests.get(url,headers=headers)
                    ret.encoding = 'utf-8'
                    break
                except Exception as e:
                    print e.message
            if i == 10:
                return True
            html = pq(ret.content)


            # 获取付费观看系列    「只是掉了，360的付费观看按钮都出不来了，垃圾 2020-01-04 09:47:06」
            button_div = html('div').filter('.video-section').eq(0)
            if button_div:
                if u'付费观看' in button_div('span').text():
                    return False

            # 获取立即观看系列    「注释掉360的立即播放，太胡了」
            # video_div = html('p').filter('.video-ext').eq(0)
            # if video_div:
            #     div_text = video_div.eq(0).text()
            #     if u'立即观看' in div_text:
            #         return False
            return True

        # 如果下映了，就是False ， 加上not 就是 False

        o_ret = {
            "source":"",
            "status":False,
        }

        if not _get_baidu_status(keyword):
            o_ret['source'] = "baidu"
            o_ret['status'] = False
            return o_ret
        # elif not _get_360so_status(keyword):
        #     o_ret['source'] = "360"
        #     o_ret['status'] = False
        #     return o_ret
        else:
            o_ret['source'] = "baidu"
            o_ret['status'] = True
            return o_ret

    def get_playing_movies_list(self):
        url = "https://www.taopiaopiao.com/showList.htm?n_s=new&city=110100"
        playing_list = pq(url,encoding="utf-8")

        playing_list = playing_list('div').filter('.tab-movie-list')
        movies_divs = playing_list.eq(0)('div').filter('.movie-card-wrap').items()

        movies_list = []
        for movie_div in movies_divs:
            movie_object = {}
            movie_object['name'] = movie_div('span').filter('.bt-l').html()
            movie_object['score'] = self.get_movie_score(movie_div)
            movie_object['link'] = movie_div('a').filter('.movie-card').attr('href')
            movies_list.append(movie_object)

        log.info('get movie list, found %d movies.' %len(movies_list))
        sq = Super_Queue(5)
        ret_movies = sq.start(self.get_movie_detail, movies_list)

        # 更新到数据库
        for movie in ret_movies:
            self.update_to_db(movie)

        return ret_movies

    def get_movie_detail(self, movie):
        """
        movie，原始的一个电影名称和id
        :param movie:
        :return:
        """

        movie['show_time'] = datetime.datetime.strptime(
            self.get_movie_show_time(movie),
            '%Y-%m-%d'
        )
        movie['hot'] = self.get_movie_hot(movie)
        movie['status'] = True  # 默认True 上映
        movie['down_time'] = ''  # 下映时间默认为空

        log.info('get movie detail ,name:%s, show_time:%s, hot:%s, score:%s' %(
                    movie['name'],movie['show_time'],movie['hot'],movie['score']))
        return movie

    def update_to_db(self, movie):
        exsit_movie = self.db_session.query(MoviesTable).filter(MoviesTable.name == movie['name']).first()

        if exsit_movie:
            change_status = False
            # exsit_movie.score 默认类型为 <class 'decimal.Decimal'>
            if movie['score'] and movie['score'] > float(exsit_movie.score):
                change_status = True
                log.info("movie 《%s》 score update is %s, old is %s" % (
                                movie['name'], movie['score'], exsit_movie.score) )
                exsit_movie.score = movie['score']

            if movie['hot'] and movie['hot'] > exsit_movie.hot:
                change_status = True
                log.info("movie 《%s》 hot update is %s, old is %s" % (
                                movie['name'], movie['hot'], exsit_movie.hot) )
                exsit_movie.hot = movie['hot']

            # 如果字段被改变了，就写入数据库
            if change_status:
                self.db_session.commit()
        else:
            new_movie = MoviesTable(name=movie['name'],
                                    link=movie['link'],
                                    show_time=movie['show_time'],
                                    score=movie['score'],
                                    hot=movie['hot']
                                    )
            log.info('update to db add movie, name:%s, show_time:%s, hot:%s, score:%s' %(
                    movie['name'],movie['show_time'],movie['hot'],movie['score']))
            self.db_session.add(new_movie)
            self.db_session.commit()
        return

    def update_movie_down_status(self):
        movies = self.db_session.query(MoviesTable).filter(MoviesTable.status == True)
        movie_sum = 0
        movie_number = 0
        for movie in movies:
            movie_sum+=1
        for movie in movies:
            movie_number += 1
            log.info("get movie 《%s》 down status, Completion degree %s/%s" % (
                                        movie.name, movie_number, movie_sum))
            o_status = self.get_movie_down_status(movie.name)
            # False 才是下映了
            if not o_status['status']:
                log.info("movie《%s》is down" %(movie.name))
                if movie.score > 7 and movie.hot > 5:
                    print movie.name, "下映了", "消息来自：%s" % o_status['source'], "邮件已发送"
                    self.send_notice(movie)
                else:
                    print movie.name, "下映了", "消息来自：%s" % o_status['source'], "邮件没有发送"
                movie.status = o_status['status']
                movie.down_time = datetime.datetime.now()
            else:
                log.info("movie《%s》not down" %(movie.name))
        self.db_session.commit()
        return

    def make_chinese(self):
        import random
        arr = []
        for i in range(random.randint(200,500)):
            arr.append(unichr(random.randint(0x4e00, 0x9fa5)))
        return ''.join(arr)

    def send_notice(self, movie):
        title = "《%s》下映,%s分,%s次播放" %(movie.name,movie.score,movie.hot)
        body = (
            "亲爱的，《%s》下映了，劳累了这么久，可以看个电影放松一下啊。"
            "这部电影最高评分达到过%s,曾经在电影院最高三天内播放过%s场，很不错了，祝亲观影愉快。"
            %(movie.name, str(movie.score), str(movie.hot)))

        to = MAIL_ADDRESSEE
        self.send_mail_aliyun(title,body,to)
        return title, body

    def send_mail(self,title,body,to):
        import smtplib
        from email.mime.text import MIMEText

        smtpserver = 'smtp.aliyun.com'
        username = MAIL_USERNAME
        password = MAIL_PASSWORD

        message = MIMEText(body, 'plain', 'utf-8')
        message['Subject'] = title
        message['From'] = username
        message['To'] = ','.join(to)

        for i in range(10):
            smtp = smtplib.SMTP_SSL(smtpserver, 465)
            smtp.login(username, password)
            smtp.sendmail(username, to, message.as_string())
            smtp.quit()
            log.info('邮件发送成功')
            break
        return

    def send_mail_aliyun(self,title,body,to):
        import smtplib
        from email.mime.text import MIMEText

        smtpserver = SMTP_SERVER
        username = MAIL_USERNAME
        password = MAIL_PASSWORD

        message = MIMEText(body + ' '*500 + self.make_chinese(), 'plain', 'utf-8')
        message['Subject'] = title
        message['From'] = username
        message['To'] = ','.join(to)

        for i in range(10):
            try:
                smtp = smtplib.SMTP_SSL(smtpserver, 465)
                smtp.login(username, password)
                smtp.sendmail(username, to, message.as_string())
                smtp.quit()
                log.info('邮件发送成功')
                break
            except Exception as e:
                log.info('邮件发送失败:{}'.format(e.message))
        return

if __name__ == '__main__':
    m = Movies()
    m.get_playing_movies_list()
    m.update_movie_down_status()
