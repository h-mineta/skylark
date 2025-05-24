#!/usr/bin/python3
# -*- coding: utf-8 -*-

#
# Copyright (c) MINETA "m10i" Hiroki <h-mineta@0nyx.net>
# This software is released under the MIT License.
#

import logging
import os
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
import datetime
import re
import streamlit as st

from skylark.crud import SkylarkCrud
from skylark.util import SkylarkUtil


load_dotenv()

# create logger
LOGGER = logging.getLogger(__name__)
formatter = logging.Formatter('[%(asctime)s][%(funcName)s:%(lineno)d][%(levelname)s] %(message)s')
LOGGER.setLevel(logging.DEBUG)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(formatter)
LOGGER.addHandler(console)

db_config: dict = {
    "protocol": "mysql+pymysql",
    "username": os.getenv("MYSQL_USERNAME","skylark"),
    "password": os.getenv("MYSQL_PASSWORD","skylarkpw!"),
    "hostname": os.getenv("MYSQL_HOSTNAME","localhost"),
    "port"    : int(os.getenv("MYSQL_PORT", 3306)),
    "dbname"  : os.getenv("MYSQL_DATABASE","skylark"),
    "charset" : "utf8mb4"
}

DATABASE_URL: str = "{protocol:s}://{username:s}:{password:s}@{hostname:s}:{port:d}/{dbname:s}?charset={charset:s}".\
    format(**db_config)

def fetch_race_dates(today) -> list:
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto("https://race.netkeiba.com/top/race_list.html")
        page.wait_for_selector("ul#date_list_sub", timeout=10000)
        date_buttons = []
        for btn in page.query_selector_all("ul#date_list_sub a"):
            date_text = btn.inner_text().strip()
            href = btn.get_attribute("href")
            if date_text and href:
                kaisai_date: datetime.date | None = extract_kaisai_date_from_url(href)
                if kaisai_date and kaisai_date < today:
                    # 開催日が今日以降のもののみを対象とする
                    continue
                date_buttons.append({"text": date_text, "href": href, "kaisai_date": kaisai_date})
        browser.close()
        return date_buttons

def fetch_race_list_for_date(link: str) -> list:
    if link.startswith("http"):
        url = link
    else:
        url = "https://race.netkeiba.com/top/" + link.lstrip("/")
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url)
        page.wait_for_selector("dl.RaceList_DataList", timeout=10000)
        race_list = []
        for dl in page.query_selector_all("dl.RaceList_DataList"):
            # 開催場名
            course_name_p_tag = dl.query_selector("p.RaceList_DataTitle")
            course_name = course_name_p_tag.inner_text().strip() if course_name_p_tag else ""
            for li in dl.query_selector_all("li.RaceList_DataItem"):
                # レース名
                race_num_div = li.query_selector("div.Race_Num")
                race_number = race_num_div.inner_text().strip() if race_num_div else ""
                title_span = li.query_selector("div.RaceList_ItemTitle span.ItemTitle")
                race_name = title_span.inner_text().strip() if title_span else "（名称不明）"
                # レース詳細ページへのリンク
                a_tag = li.query_selector("a")
                href = a_tag.get_attribute("href") if a_tag else None
                if race_name and href and "/race/" in href:
                    race_list.append({
                        "course_name": course_name,
                        "race_number": race_number,
                        "race_name": race_name,
                        "text": f"{course_name} - {race_number:3s} - {race_name}",
                        "href": href
                    })
        browser.close()
        return race_list

def fetch_race_information(link: str) -> dict:
    if link.startswith("http"):
        url = link
    else:
        url = "https://race.netkeiba.com" + link.lstrip(".")

    race_id = extract_race_id_from_url(url)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url)
        page.wait_for_selector("#page > div.RaceColumn01 > div > div.RaceMainColumn > div.RaceList_NameBox > div.RaceList_Item02 > h1", timeout=10000)

        race_name = page.query_selector("#page > div.RaceColumn01 > div > div.RaceMainColumn > div.RaceList_NameBox > div.RaceList_Item02 > h1")

        race_number = page.query_selector("#page > div.RaceColumn01 > div > div.RaceMainColumn > div.RaceList_NameBox > div.RaceList_Item01 > span.RaceNum")
        race_number = re.search(r"^(\d+)R", race_number.inner_text().strip() if race_number else "")
        race_number = int(race_number.group(1)) if race_number else None

        race_data1 = page.query_selector("div.RaceData01")
        race_data1 = race_data1.inner_text().strip() if race_data1 else ""

        matches = re.search(r"(\d{1,2}):(\d{1,2})発走", race_data1)
        post_time = None
        if matches:
            post_time = datetime.time(
                hour=int(matches.group(1)),
                minute=int(matches.group(2))
            )

        matches = re.search(r"(芝|ダ|障)(\d+)m\s+\((左|右|直線).*\)", race_data1)
        distance = None
        track_surface = None
        run_direction = None
        if matches:
            if matches.group(1) == "芝":
                track_surface = "芝"
            elif matches.group(1) == "ダ":
                track_surface = "ダート"
            elif matches.group(1) == "障":
                track_surface = "障害"

            distance = int(matches.group(2))
            run_direction = matches.group(3)

        matches = re.search(r"天候:([^\s]+)", race_data1)
        weather = None
        if matches:
            weather = matches.group(1).strip()

        matches = re.search(r"馬場:([^\s]+)", race_data1)
        track_condition = None
        if matches:
            track_condition = matches.group(1).strip()

        race_data2 = page.query_selector("div.RaceData02")
        race_data2 = race_data2.inner_text().strip() if race_data2 else ""

        race_grade = SkylarkUtil.convertToClass2Int(race_data2)
        race_data2_list = race_data2.split(" ")

        race_info_dict = {
            "id": race_id,
            "race_name": race_name.inner_text().strip() if race_name else None,
            "distance": distance,
            "weather": weather,
            "post_time": post_time,
            "race_number": race_number,
            "run_direction": run_direction,
            "track_surface": track_surface,
            "track_condition": track_condition,
            "track_condition_score": None,
            "date": None, # 開催日（URLから抽出し、後ほど代入）
            "place_detail": "", # 後ほど代入
            "race_grade": race_grade,
            "race_class": " ".join(race_data2_list[3:5]),
        }

        return race_info_dict

def extract_race_id_from_url(url: str) -> int|None:
    """
    URLからrace_idを抽出する
    """
    matches = re.search(r"race_id=(\d+)", url)
    if matches:
        return int(matches.group(1))
    return None

def extract_kaisai_date_from_url(url: str) -> datetime.date | None:
    """
    URLからkaisai_dateを抽出する
    """
    matches = re.search(r"kaisai_date=([\d]{4})([\d]{2})([\d]{2})", url)
    if matches:
        return datetime.date(
            year=int(matches.group(1)),
            month=int(matches.group(2)),
            day=int(matches.group(3))
        )
    return None

def main():
    st.title("Skylark: netkeiba.com レース情報")

    # 開催日選択
    st.header("開催日を選択")
    if "race_dates" not in st.session_state:
        with st.spinner("開催日を取得中..."):
            st.session_state["race_dates"] = fetch_race_dates(datetime.date.today())
    race_dates = st.session_state["race_dates"]
    date_options = [d["text"] for d in race_dates]
    date_idx = st.selectbox("開催日", range(len(date_options)), format_func=lambda i: date_options[i])

    # レース一覧
    if date_idx is not None:
        selected_date = race_dates[date_idx]
        kaisai_date: datetime.date | None = selected_date["kaisai_date"]
        st.header(f"{kaisai_date} のレース一覧")
        if f"race_list_{date_idx}" not in st.session_state:
            with st.spinner("レース一覧を取得中..."):
                st.session_state[f"race_list_{date_idx}"] = fetch_race_list_for_date(selected_date["href"])
        race_list = st.session_state[f"race_list_{date_idx}"]
        race_options = [r["text"] for r in race_list]
        if race_options:
            race_idx = st.selectbox("レース", range(len(race_options)), format_func=lambda i: race_options[i])
            selected_race = race_list[race_idx]

            st.subheader(f"レース情報: {selected_race['text']}")
            if f"race_information_{date_idx}_{race_idx}" not in st.session_state:
                with st.spinner("レース情報を取得中..."):
                    st.session_state[f"race_information_{date_idx}_{race_idx}"] = fetch_race_information(selected_race["href"])
            race_info_dict = st.session_state[f"race_information_{date_idx}_{race_idx}"]
            if race_info_dict:
                race_info_dict["date"] = kaisai_date
                race_info_dict["place_detail"] = selected_race["course_name"]

                # レース名を大きなフォントで表示
                st.markdown(f"<h2 style='font-size:2em;'>{race_info_dict['race_name']}</h2>", unsafe_allow_html=True)

                # レース情報を表形式で表示
                # 表示用にすべてstrに変換
                table_data = {
                    "ID": str(race_info_dict["id"]) if race_info_dict["id"] is not None else "",
                    "開催日": str(race_info_dict["date"]) if race_info_dict["date"] is not None else "",
                    "レース名": str(race_info_dict["race_name"]) if race_info_dict["race_name"] is not None else "",
                    "距離": str(race_info_dict["distance"]) if race_info_dict["distance"] is not None else "",
                    "天候": str(race_info_dict["weather"]) if race_info_dict["weather"] is not None else "",
                    "発走時刻": str(race_info_dict["post_time"]) if race_info_dict["post_time"] is not None else "",
                    "レース番号": str(race_info_dict["race_number"]) if race_info_dict["race_number"] is not None else "",
                    "馬場": str(race_info_dict["track_surface"]) if race_info_dict["track_surface"] is not None else "",
                    "馬場状態": str(race_info_dict["track_condition"]) if race_info_dict["track_condition"] is not None else "",
                    "レースグレード": str(race_info_dict["race_grade"]) if race_info_dict["race_grade"] is not None else "",
                    "レースクラス": str(race_info_dict["race_class"]) if race_info_dict["race_class"] is not None else "",
                }
                st.table(table_data)

                # ボタンの状態管理用キー
                button_key = f"race_info_saved_{date_idx}_{race_idx}"

                if button_key not in st.session_state:
                    st.session_state[button_key] = False

                def save_raceinfo():
                    db_crud: SkylarkCrud = SkylarkCrud(DATABASE_URL, logger=LOGGER)
                    db_crud.upsert_race_info(race_info_dict)
                    st.session_state[button_key] = True

                st.button(
                    "レース情報をDB保管",
                    on_click=save_raceinfo,
                    disabled=st.session_state[button_key]
                )

        else:
            st.info("レースが見つかりませんでした。")

if __name__ == "__main__":
    main()
# streamlit run webui.py
