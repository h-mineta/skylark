# -*- coding: utf-8 -*-

#
# Copyright (c) MINETA "m10i" Hiroki <h-mineta@0nyx.net>
# This software is released under the MIT License.
#

from argparse import Namespace
import asyncio
from logging import Logger
import os
import re
import time
import zstandard as zstd

import httpx
from pyquery import PyQuery as pq

from skylark.crud import SkylarkCrud
from skylark.util import SkylarkUtil

class SkylarkScraperDb:
    def __init__(self, db_url: str, args: Namespace, logger: Logger):
        self.db_url = db_url
        self.args = args
        self.logger = logger

        self.url_db : str = "https://db.netkeiba.com"
        self.url_login :str  = "https://account.netkeiba.com"

        self.cookies = httpx.Cookies()
        self.race_url_list = []

        # 無視するレースID
        self.ignore_race_id: list[int] = [
            200808020398,
            200808020399,
            202204010808,
            202209040704
        ]

    def __enter__(self):
        return self

    # レース結果URLリストを読み込み
    def import_race_url_list(self):
        filepath = os.path.join(self.args.temp, self.args.race_list_file)
        print(filepath)
        if os.path.isfile(filepath) == False:
            self.logger.error("file not found: %s", filepath)
            return

        with open(filepath, "r") as file:
            self.race_url_list = sorted([val.strip() for val in file.readlines()])

    # レース結果URLリストを保存
    def export_race_url_list(self):
        self.race_url_list = sorted(self.race_url_list)
        filepath = os.path.join(self.args.temp, self.args.race_list_file)

        with open(filepath, "w") as file:
            [file.write(path+"\n") for path in self.race_url_list]

    def set_race_url_list(self, race_ids):
        [self.race_url_list.append(f"/race/{race_id}/") for race_id in race_ids]
        self.logger.debug(self.race_url_list)

    # レース結果URLを作成
    def make_race_url_list(self, period):
        url = self.url_db + "/?pid=race_top"
        pattern_race = re.compile(r"^/race/[0-9]{12}/$")
        pattern_race_list = re.compile(r"^/race/list/[0-9]{8}/$")

        while period > 0:
            html: str = ""
            try:
                timeout = float(os.environ.get("HTTP_TIMEOUT", 5))
                response = httpx.get(url, timeout=timeout, cookies=self.cookies)
                html = response.text

            except Exception as ex:
                self.logger.error(ex)
                return

            if html == "":
                self.logger.warning("html is empty")
                continue

            dom = pq(html)

            # 日別のrace/listを検索
            for doc in dom("div#contents table tr td a[href ^='/race/list/']").items():
                path = doc.attr('href')
                if isinstance(path, str) and pattern_race_list.match(path):
                    self.logger.info("race_list path: %s", path)

                    for _ in range(1, 3):
                        try:
                            response = httpx.get(self.url_db + path, timeout=timeout, cookies=self.cookies)
                            html = response.text

                        except Exception as ex:
                            self.logger.warning(ex)
                            continue

                        if html == "":
                            self.logger.warning("html is empty")
                            time.sleep(3) # sleep 3sec
                            continue

                        try:
                            dom = pq(html)
                        except Exception as ex:
                            self.logger.error(ex)
                        break

                    # race/listから各競馬場事のrace結果URL(pathを取り出す
                    for doc in dom("div#contents div#main div.race_list a[href ^='/race/']").items():
                        path = doc.attr('href')
                        if isinstance(path, str) and  pattern_race.match(path):
                            self.race_url_list.append(path)
                            self.logger.debug("path: %s, name: %s", path, doc.text())

                    time.sleep(0.2) # sleep 100ms

            # 先月分のURLを作成
            path = dom("div#contents div.race_calendar li a").eq(1).attr("href")
            url = f"{self.url_db}{path}"
            period -= 1

            # sleep 100ms
            time.sleep(0.1)

    # netkeibaにログイン
    def login(self, client: httpx.Client) -> bool|None:
        login_id = os.getenv("NETKEIABA_LOGINID","")
        password = os.getenv("NETKEIABA_PASSWORD","")

        if login_id != "" and password != "":
            post = {
                'pid'        : "login",
                'action'     : "auth",
                'login_id'   : login_id,
                'pswd'       : password
            }

            print(login_id, password)
            timeout = float(os.environ.get("HTTP_TIMEOUT", 5))
            response = client.post(self.url_login, data=post, timeout=timeout, cookies=self.cookies)
            html = response.text
            dom = pq(html)

            for doc in dom("span.error").items():
                # ログイン失敗と思われる
                self.logger.error(doc.text())
                return False

            # ログイン成功
            return True

        return None

    # ダウンロード実行
    def download(self):
        if len(self.race_url_list) == 0:
            return

        with httpx.Client(http2=True) as client:
            result = self.login(client)
            self.logger.info("login: %s", result)

        asyncio.run(
            self.download_concurrently(
                max_concurrent_requests=int(os.environ.get("MAX_CONCURRENT_REQUESTS", 4))
            )
        )

    async def download_concurrently(self, max_concurrent_requests=4):
        pattern = re.compile(r"^/race/([0-9]+)/$")

        async with httpx.AsyncClient(http2=True) as client:
            semaphore = asyncio.Semaphore(max_concurrent_requests)  # 並行数を制御
            tasks = []

            async def limited_download(idx: int, url: str, filepath: str):
                async with semaphore:
                    race_id = int(url.rsplit("/", 2)[1])

                    try:
                        self.ignore_race_id.index(race_id)
                        self.logger.warning("[%5d] race_id: %d, url: %s, reject[ignore_race_id]", idx, race_id, url)
                        return
                    except ValueError as ex:
                        self.logger.debug("[%5d] race_id: %d, url: %s, start", idx, race_id, url)

                    html = None

                    if os.path.isfile(filepath) == False:
                        try:
                            response = await client.get(
                                url,
                                timeout=float(os.environ.get("HTTP_TIMEOUT", 5))
                            )
                            response.raise_for_status()

                            try:
                                # EUC-JPエンコーディングでデコードし、UTF-8に変換
                                html = response.content.decode("euc-jp", errors="replace")
                            except UnicodeDecodeError:
                                # 既にUTF-8または他のエンコーディングの場合
                                html = response.text

                        except Exception as ex:
                            self.logger.warning(ex)
                            return

                        with open(filepath, 'wb') as fp:
                            fp.write(zstd.compress(html.encode("utf-8"), 3))

                        self.logger.info("[%5d] race_id: %d, url: %s, download finish", idx, race_id, url)

                    else:
                        self.logger.info("[%5d] race_id: %d, url: %s, downloaded", idx, race_id, url)

                        with open(filepath, 'rb') as fp:
                            html = zstd.decompress(fp.read()).decode("utf-8")

                    if html == None:
                        self.logger.warning("[%5d] race_id: %d, url: %s, no data", idx, race_id, url)
                    else:
                        self.scraping_html(race_id, html)

                    self.logger.debug("[%5d] url: %s, done", idx, url)

            for idx, url_path in enumerate(self.race_url_list):
                matchese: re.Match|None = pattern.match(url_path)
                if not matchese:
                    continue

                url = self.url_db + url_path
                filepath = os.path.join(self.args.temp, "race." + matchese.group(1) + ".html.zst")

                tasks.append(limited_download(idx, url, filepath))

            results = await asyncio.gather(*tasks)
            return results

    def scraping_html(self, race_id, html):
        db_crud: SkylarkCrud = SkylarkCrud(self.db_url, logger = self.logger)
        try:
            dataset_horse :list   = []
            dataset_jockey :list  = []
            dataset_trainer :list = []
            dataset_owner :list   = []
            dataset_result :list  = []
            dataset_payoff :list  = []

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
            data_place_detail = None
            data_class = None
            data_date = None

            data_race_number_text = str(race_head("dl.racedata dt").text())
            if data_race_number_text:
                data_race_number = int(data_race_number_text.split(" ", 1)[0])
            else:
                data_race_number = None

            data_race_name = race_head("dl.racedata dd h1").text()

            # track_surface, distance, weather, track_condition, post_time
            race_info_text = str(race_head("dl.racedata dd p span").text())
            matchese: re.Match | None = re.match(
                r'^([^\d ]+).*?(\d{4})m\s*/\s*天候 : (\w+)\s*/\s*(.+)\s+/\s+発走 : (\d{1,2}:\d{1,2})',
                race_info_text,
                re.U
            )
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

                if data_run_direction is not None and re.search(r'^.*外$', data_track_surface_org):
                    data_run_direction = data_run_direction + " 外"

                data_distance = int(matchese.group(2))

                data_weather = matchese.group(3)

                data_track_condition_org = matchese.group(4)
                matchese_condition = re.match(r'^.*?\s*:\s*(\w+)\s*', data_track_condition_org, re.U)
                if matchese_condition:
                    data_track_condition = matchese_condition.group(1)

                data_post_time = matchese.group(5)

            # date, place_detail, class
            text_value = str(race_head("div.mainrace_data p").eq(1).text())
            matchese = re.match(r'^(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日\s*([^ ]+)\s+(.+)', text_value)
            if matchese:
                data_date = matchese.group(1) + "-" + matchese.group(2) + "-" + matchese.group(3)
                data_place_detail = matchese.group(4)
                data_class = matchese.group(5)

            race_head = None

            dataset_info: dict = {
                "id":race_id,
                "race_name":data_race_name,
                "distance":data_distance,
                "weather":data_weather,
                "post_time":data_post_time,
                "race_number":data_race_number,
                "run_direction":data_run_direction,
                "track_surface":data_track_surface,
                "track_condition":data_track_condition,
                "track_condition_score":data_track_condition_score,
                "date":data_date,
                "place_detail":data_place_detail,
                "race_grade":SkylarkUtil.convertToClass2Int(data_class),
                "race_class":data_class
            }

            db_crud.upsert_race_info(dataset_info)

            race_result = dom("html body div#page div#contents_liquid table tr")
            for result_row in race_result[1:]:
                columns = pq(result_row).find("td")

                #着順
                order_of_finish = str(columns.eq(0).text())
                try:
                    order_of_finish = int(order_of_finish)
                except ValueError as ex:
                    #self.logger.warning(ex)
                    order_of_finish = None

                #枠番
                bracket_number = str(columns.eq(1).text())
                try:
                    bracket_number = int(bracket_number)
                except ValueError as ex:
                    self.logger.warning(ex)

                #馬番
                horse_number = str(columns.eq(2).text())
                try:
                    horse_number = int(horse_number)
                except ValueError as ex:
                    self.logger.warning(ex)

                #馬ID
                horse_id = str(columns.eq(3).find("a").eq(0).attr("href")).rsplit("/", 2)[1]
                try:
                    horse_id = int(horse_id)
                except ValueError as ex:
                    self.logger.warning(ex)

                #馬名
                horse_name = str(columns.eq(3).find("a").eq(0).text())

                #性別、年齢
                sex = None
                age = 0
                matchese = None
                matchese = re.match(r'^(.)(\d+)$', str(columns.eq(4).text()))
                if matchese:
                    sex = matchese.group(1)
                    age = int(matchese.group(2))

                #斤量
                basis_weight = float(str(columns.eq(5).text()))

                #騎手
                jockey_id = str(columns.eq(6).find("a").eq(0).attr("href")).rsplit("/", 2)[1]
                try:
                    jockey_id = int(jockey_id)
                except ValueError as ex:
                    self.logger.warning(ex)
                jockey_name = columns.eq(6).find("a").eq(0).text()

                #タイム
                finishing_time = None
                matchese = re.match(r'^(\d+:\d+\.\d+)$', str(columns.eq(7).text()))
                if matchese:
                    finishing_time = '00:'+matchese.group(1)

                #着差
                margin = str(columns.eq(8).text())

                #タイム指数(有料)
                try:
                    speed_figure = int(str(columns.eq(9).text()))
                except ValueError as ex:
                    #self.logger.warning(ex)
                    speed_figure =  None

                #通過
                passing_rank = str(columns.eq(10).text())

                #上りタイム
                last_phase = str(columns.eq(11).text())
                try:
                    last_phase = float(last_phase)
                except ValueError as ex:
                    #self.logger.warning(ex)
                    last_phase = None

                #単勝オッズ
                odds = str(columns.eq(12).text())
                try:
                    odds = float(odds)
                except ValueError as ex:
                    #self.logger.warning(ex)
                    odds = None

                #人気
                popularity = str(columns.eq(13).text())
                try:
                    popularity = int(popularity)
                except ValueError as ex:
                    #self.logger.warning(ex)
                    popularity = None

                #馬体重
                horse_weight = None
                horse_weight_diff = None
                matchese = None
                matchese = re.match(r'^(\d+)\(\+?(\-?\d+)\)$', str(columns.eq(14).text()))
                if matchese:
                    horse_weight = matchese.group(1)
                    horse_weight_diff = matchese.group(2)

                #備考
                remark = columns.eq(17).text()
                if remark == "":
                    remark = None

                # 厩舎
                stable = '不明'
                matchese = None
                matchese = re.match(r'\[(.)\]', str(columns.eq(18).text()))
                if matchese:
                    stable = matchese.group(1)

                #調教師
                trainer_id = str(columns.eq(18).find("a").eq(0).attr("href")).rsplit("/", 2)[1]
                try:
                    trainer_id = int(trainer_id)
                except ValueError as ex:
                    self.logger.warning(ex)
                trainer_name = columns.eq(18).find("a").eq(0).text()

                #馬主
                owner_id = str(columns.eq(19).find("a").eq(0).attr("href")).rsplit("/", 2)[1]
                owner_name = columns.eq(19).find("a").eq(0).text()

                #賞金
                earning_money = str(columns.eq(20).text()).replace(",", "")
                try:
                    earning_money = float(earning_money)
                except ValueError as ex:
                    #self.logger.warning(ex)
                    earning_money = 0

                dataset_horse.append({
                    "horse_id":horse_id,
                    "horse_name":horse_name
                })

                dataset_jockey.append({
                    "jockey_id":jockey_id,
                    "jockey_name":jockey_name
                })

                dataset_trainer.append({
                    "trainer_id":trainer_id,
                    "trainer_name":trainer_name
                })

                dataset_owner.append({
                    "owner_id":owner_id,
                    "owner_name":owner_name
                })

                dataset_result.append({
                    "race_id":race_id,
                    "horse_number":horse_number,
                    "order_of_finish":order_of_finish,
                    "bracket_number":bracket_number,
                    "horse_id":horse_id,
                    "sex":sex,
                    "age":age,
                    "basis_weight":basis_weight,
                    "jockey_id":jockey_id,
                    "finishing_time":finishing_time,
                    "margin":margin,
                    "speed_figure":speed_figure,
                    "passing_rank":passing_rank,
                    "last_phase":last_phase,
                    "odds":odds,
                    "popularity":popularity,
                    "horse_weight":horse_weight,
                    "horse_weight_diff":horse_weight_diff,
                    "remark":remark,
                    "stable":stable,
                    "trainer_id":trainer_id,
                    "owner_id":owner_id,
                    "earning_money":earning_money
                })

            race_result = None
            db_crud.upsert_horses(dataset_horse)
            db_crud.upsert_jockeys(dataset_jockey)
            db_crud.upsert_trainers(dataset_trainer)
            db_crud.upsert_owners(dataset_owner)
            db_crud.upsert_race_results(dataset_result)

            pay_block = dom("html body div#page div#contents dl.pay_block tr")
            for pay_result in pay_block:
                columns = pq(pay_result).find("th")
                ticket_type = SkylarkUtil.convertToTicketType2Int(columns.eq(0).text())

                columns = pq(pay_result).find("td")
                horse_numbers_list = str(columns.eq(0).html()).split("<br />")
                payoff_list = str(columns.eq(1).html()).split("<br />")
                popularity_list = str(columns.eq(2).html()).split("<br />")

                idx = 0
                while idx < len(horse_numbers_list):
                    dataset_payoff.append({
                        "race_id":race_id,
                        "ticket_type":ticket_type,
                        "horse_numbers":horse_numbers_list[idx].replace(" ", "").replace("→", "->"),
                        "payoff":int(payoff_list[idx].replace(",", "")),
                        "popularity":int(popularity_list[idx])
                    })
                    idx = idx + 1

            pay_block = None
            db_crud.upsert_payoffs(dataset_payoff)
        except Exception as ex:
            self.logger.error(ex)
