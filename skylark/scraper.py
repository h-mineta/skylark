# -*- coding: utf-8 -*-

#
# Copyright (c) 2016 h-mineta <h-mineta@0nyx.net>
# This software is released under the MIT License.
#

from . import db
from pyquery import PyQuery as pq
import asyncio
import gzip
import http.cookiejar
import logging
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
                    self.logger.info("race_list path: %s", path)

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

    # ダウンロード実行
    def download(self):
        if len(self.race_url_list) > 0:
            loop = asyncio.get_event_loop()

            session = self.login()
            if session == None:
                return False

            futures = self.downloadSubProcess(session, self.args.download_concurrency)
            if futures != None:
                loop.run_until_complete(futures)

        return True

    # race HTMLのダウンロード
    async def downloadSubProcess(self, session, limit):
        sem = asyncio.Semaphore(limit)

        async def worker(session, index, url, filepath):
            with await sem:
                race_id = int(url.rsplit("/", 2)[1])

                self.logger.debug("[%5d] race_id: %d, url: %s, start", index, race_id, url)
                html = None
                download_flag = False

                if os.path.isfile(filepath) == False:
                    try:
                        html = session.open(url, None, self.args.http_timeout).read().decode(self.html_charset)
                        download_flag = True
                    except Exception as ex:
                        self.logger.warning(ex)
                        return

                    with gzip.open(filepath, 'wt', encoding=self.html_charset) as file:
                        file.write(html)

                    self.logger.info("[%5d] race_id: %d, url: %s, download finish", index, race_id, url)

                else:
                    self.logger.info("[%5d] race_id: %d, url: %s, downloaded", index, race_id, url)

                    with gzip.open(filepath, 'rt', encoding=self.html_charset) as file:
                        html = file.read()

                if html == None:
                    self.logger.warning("[%5d] race_id: %d, url: %s, no data", index, race_id, url)
                else:
                    self.scrapingHtml(race_id, html)

                if download_flag == True:
                    # sleep 1sec
                    await asyncio.sleep(1.0)

                self.logger.debug("[%5d] url: %s, done", index, url)

        futures = []
        for index, path in enumerate(self.race_url_list):
            matchese = re.match(r"^/race/([0-9]+)/$", path)
            url = self.url_db + path
            filepath = self.args.temp + "/race." + matchese.group(1) + ".html.gz"

            futures.append(worker(session, index, url, filepath))

        if len(futures) > 0:
            return await asyncio.wait(futures)
        else:
            return None

    def scrapingHtml(self, race_id, html):
        with db.SkylarkDb(args = self.args, logger = self.logger) as dbi:
            dataset_info   = ()
            dataset_result = []

            dom = pq(html)
            race_head = dom("html body div#page div#main div.race_head")

            # init
            data_race_name = None
            data_distance = None
            data_weather = None
            data_post_time = None
            data_race_number = None
            data_track_surface = None
            data_track_condition = None
            data_track_condition_org = None
            data_track_condition_score = None
            data_run_direction = None
            data_track_surface_org = None

            data_race_number = int(race_head("dl.racedata dt").text().split(" ", 1)[0])

            data_race_name = race_head("dl.racedata dd h1").text()

            # track_surface, distance, weather, track_condition, post_time
            matchese = None
            matchese = re.match(r'^([^\d]+)(\d+)m\s*/\s*天候 : (\w+)\s*/\s*(.+)\s+/\s+発走 : (\d{1,2}:\d{1,2})', race_head("dl.racedata dd p span").text(), re.U)
            if matchese:
                data_track_surface_org = matchese.group(1)
                if re.search(r'^芝', data_track_surface_org):
                    data_track_surface = "芝"
                elif re.search(r'^ダ', data_track_surface_org):
                    data_track_surface = "ダート"
                elif re.search(r'^障', data_track_surface_org):
                    data_track_surface = "障害"

                if re.search(r'^.*左', data_track_surface_org):
                    data_run_direction = "左"
                elif re.search(r'^.*右', data_track_surface_org):
                    data_run_direction = "右"
                elif re.search(r'^.*直線', data_track_surface_org):
                    data_run_direction = "直線"

                if re.search(r'^.*外$', data_track_surface_org):
                    data_run_direction = data_run_direction + " 外"

                data_distance = int(matchese.group(2))

                data_weather = matchese.group(3)

                data_track_condition_org = matchese.group(4)
                matchese_condition = re.match(r'^.*?\s*:\s*(\w+)\s*', data_track_condition_org, re.U)
                if matchese_condition:
                    data_track_condition = matchese_condition.group(1)

                data_post_time = matchese.group(5)

            # date, place_detail, class
            matchese = None
            matchese = re.match(r'^(\d{4})年(\d{1,2})月(\d{1,2})日\s+([^ ]+)\s+(.+)', race_head("div.mainrace_data p").eq(1).text())
            if matchese:
                data_date = matchese.group(1) + "-" + matchese.group(2) + "-" + matchese.group(3)

                data_place_detail = matchese.group(4)

                data_class = matchese.group(5)

            race_head = None

            dataset_info =(
                race_id,
                data_race_name,
                data_distance,
                data_weather,
                data_post_time,
                data_race_number,
                data_run_direction,
                data_track_surface,
                data_track_condition,
                data_track_condition_score,
                data_date,
                data_place_detail,
                data_class
            )

            dbi.insertRaceInfo(dataset_info)

            race_result = dom("html body div#page div#contents_liquid table tr")
            for result_row in race_result[1:]:
                columns = pq(result_row).find("td")

                #着順
                try:
                    order_of_finish = int(columns.eq(0).text())
                except ValueError as ex:
                    order_of_finish = None

                #枠番
                bracket_number = int(columns.eq(1).text())

                #馬番
                horse_number = int(columns.eq(2).text())

                #馬ID
                try:
                    horse_id = int(columns.eq(3).find("a").eq(0).attr("href").rsplit("/", 2)[1])
                except ValueError as ex:
                    horse_id = None

                #馬名
                horse_name = columns.eq(3).find("a").eq(0).text()

                #性別、年齢
                sex = None
                age = 0
                matchese = None
                matchese = re.match(r'^(.)(\d+)$', columns.eq(4).text())
                if matchese:
                    sex = matchese.group(1)
                    age = int(matchese.group(2))

                #斤量
                basis_weight = float(columns.eq(5).text())

                #騎手
                try:
                    jockey_id = int(columns.eq(6).find("a").eq(0).attr("href").rsplit("/", 2)[1])
                except ValueError as ex:
                    jockey_id = None
                jockey_name = columns.eq(6).find("a").eq(0).text()

                #タイム
                finishing_time = None
                matchese = None
                matchese = re.match(r'^(\d+:\d+\.\d+)$', columns.eq(7).text())
                if matchese:
                    finishing_time = '00:'+matchese.group(1)

                #着差
                margin = columns.eq(8).text()

                #タイム指数(有料)
                try:
                    speed_figure = int(columns.eq(9).text())
                except ValueError as ex:
                    speed_figure = None

                #通過
                passing_rank = columns.eq(10).text()

                #上りタイム
                try:
                    last_phase = float(columns.eq(11).text())
                except ValueError as ex:
                    last_phase = None

                #単勝オッズ
                try:
                    odds = float(columns.eq(12).text())
                except ValueError as ex:
                    odds = None

                #人気
                try:
                    popularity = int(columns.eq(13).text())
                except ValueError as ex:
                    popularity = None

                #馬体重
                horse_weight = None
                horse_weight_diff = None
                matchese = None
                matchese = re.match(r'^(\d+)\(\+?(\-?\d+)\)$', columns.eq(14).text())
                if matchese:
                    horse_weight = matchese.group(1)
                    horse_weight_diff = matchese.group(2)

                #備考
                remark = columns.eq(17).text()
                if remark == "":
                    remark = None

                #調教師
                try:
                    trainer_id = int(columns.eq(18).find("a").eq(0).attr("href").rsplit("/", 2)[1])
                except ValueError as ex:
                    trainer_id = None
                trainer_name = columns.eq(18).find("a").eq(0).text()

                #馬主
                try:
                    owner_id = int(columns.eq(19).find("a").eq(0).attr("href").rsplit("/", 2)[1])
                except ValueError as ex:
                    owner_id = None
                owner_name = columns.eq(19).find("a").eq(0).text()

                #賞金
                try:
                    earning_money = float(columns.eq(20).text().replace(",", ""))
                except ValueError as ex:
                    earning_money = 0

                dataset_result.append((
                    race_id,
                    order_of_finish,
                    bracket_number,
                    horse_number,
                    horse_id,
                    #horse_name, # db not insert
                    sex,
                    age,
                    basis_weight,
                    jockey_id,
                    finishing_time,
                    margin,
                    speed_figure,
                    passing_rank,
                    last_phase,
                    odds,
                    popularity,
                    horse_weight,
                    horse_weight_diff,
                    remark,
                    None,
                    trainer_id,
                    #trainer_name, # db not insert
                    owner_id,
                    #owner_name, # db not insert
                    earning_money
                ))

            dbi.insertRaceResult(dataset_result)
