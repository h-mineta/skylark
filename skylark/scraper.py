# -*- coding: utf-8 -*-

#
# Copyright (c) 2016 h-mineta <h-mineta@0nyx.net>
# This software is released under the MIT License.
#

from pyquery import PyQuery as pq
import aiohttp
import asyncio
import gzip
import http.cookiejar
import logging
import MySQLdb
import os
import re
import sys
import time
import urllib.request, urllib.parse, urllib.error

class SkylarkScraper:
    def __init__(self, args, logger):
        self.args          = args
        self.logger        = logger

        self.html_charset  = "euc-jp"
        self.url_db        = "http://db.netkeiba.com"
        self.url_login     = "https://account.netkeiba.com"

        self.opener        = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
        self.race_url_list = []

        # Proxy有り
        if self.args.http_proxy:
            proxy_handler = urllib.request.ProxyHandler({"http": self.args.http_proxy})
            self.opener.add_handler(proxy_handler)

    # レース結果URLリストを読み込み
    def importRaceUrlList(self):
        if os.path.isfile(self.args.temp + "/" + self.args.race_list_file) == False:
            return

        with open(self.args.temp + "/" + self.args.race_list_file, "r") as file:
            self.race_url_list = sorted([val.strip() for val in file.readlines()])

    # レース結果URLリストを保存
    def exportRaceUrlList(self):
        self.race_url_list = sorted(self.race_url_list)

        with open(self.args.temp + "/" + self.args.race_list_file, "w") as file:
            [file.write(path+"\n") for path in self.race_url_list]

    # レース結果URLを作成
    def makeRaceUrlList(self, period):
        url = self.url_db + "/?pid=race_top"
        pattern_race = re.compile(r"^/race/[0-9]{12}/$")
        pattern_race_list = re.compile(r"^/race/list/[0-9]{8}/$")

        while period > 0:
            try:
                html = self.opener.open(url).read().decode(self.html_charset)

            except Exception as e:
                self.logger.error(ex)
                return

            dom = pq(html)

            # 日別のrace/listを検索
            for doc in dom("div#contents table tr td a[href ^='/race/list/']").items():
                path = doc.attr('href')
                if pattern_race_list.match(path):
                    self.logger.debug("race_list path: %s", path)

                    try:
                        html = self.opener.open(self.url_db + path).read().decode(self.html_charset)

                    except Exception as ex:
                        self.logger.warning(ex)
                        next

                    dom = pq(html)

                    # race/listから各競馬場事のrace結果URL(pathを取り出す
                    for doc in dom("div#contents div#main div.race_list a[href ^='/race/']").items():
                        path = doc.attr('href')
                        if pattern_race.match(path):
                            self.race_url_list.append(path)
                            self.logger.debug("path: %s, name: %s", path, doc.text())

            # 先月分のURLを作成
            path = dom("div#contents div.race_calendar li a").eq(1).attr("href")
            url = self.url_db + path

            period -= 1

            # sleep 200ms
            time.sleep(0.2)

    # netkeibaにログイン
    def login(self):
        if self.args.username and self.args.password:
            post = {
                'pid'        : "login",
                'action'     : "auth",
                'login_id'   : self.args.username,
                'pswd'       : self.args.password
            }
            data = urllib.parse.urlencode(post).encode(self.html_charset)

            html = self.opener.open(self.url_login, data, self.args.http_timeout).read().decode(self.html_charset)
            dom = pq(html)

            for doc in dom("span.error").items():
                # ログイン失敗と思われる
                self.logger.error(doc.text())
                return None

        return self.opener

    # ダウンロードのみ実行
    def download(self):
        self.downloadAndScraping(None)

    # ダウンロード&スクレイピング実行
    def downloadAndScraping(self, db_connector):
        if len(self.race_url_list) > 0:
            loop = asyncio.get_event_loop()

            session = self.login()
            if session == None:
                return False

            futures = self.downloadProcess(session, self.args.download_concurrency, db_connector)
            if futures != None:
                loop.run_until_complete(futures)

        return True

    # race HTMLのダウンロード
    async def downloadProcess(self, session, limit, db_connector):
        sem = asyncio.Semaphore(limit)

        async def worker(session, index, url, filepath, db_connector):
            with await sem:
                self.logger.debug("[%5d] url: %s, start", index, url)

                if os.path.isfile(filepath) == False:
                    try:
                        html = session.open(url, None, self.args.http_timeout).read().decode(self.html_charset)

                    except Exception as ex:
                        self.logger.warning(ex)
                        return

                    with gzip.open(filepath, 'wt', encoding=self.html_charset) as file:
                        file.write(html)

                    self.logger.info("[%5d] url: %s, download finish", index, url)

                    # sleep 1sec
                    await asyncio.sleep(1.0)
                else:
                    self.logger.debug("[%5d] url: %s, downloaded", index, url)

                # scraping
                if db_connector:
                    dummy = None

                self.logger.debug("[%5d] url: %s, done", index, url)

        futures = []
        for index, path in enumerate(self.race_url_list):
            matchese = re.match(r"^/race/([0-9]+)/$", path)
            url = self.url_db + path
            filepath = self.args.temp + "/race." + matchese.group(1) + ".html.gz"

            futures.append(worker(session, index, url, filepath, db_connector))

        if len(futures) > 0:
            return await asyncio.wait(futures)
        else:
            return None
