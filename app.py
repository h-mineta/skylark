#!/usr/bin/python3 -tt
# -*- coding: utf-8 -*-

#
# Copyright (c) 2016-2020 h-mineta <h-mineta@0nyx.net>
# This software is released under the MIT License.
#
# pip3 install pyquery aiohttp mysqlclient

import argparse
import logging
import os

from dotenv import load_dotenv
from skylark import crud, scraper

load_dotenv()

parser = argparse.ArgumentParser(
    description='This script is a program that netkeiba.com scraping.')

parser.add_argument('--temp',
                    action='store',
                    nargs='?',
                    const=None,
                    default='./temp',
                    type=str,
                    choices=None,
                    help='temp directory(default: ./temp/)',
                    metavar=None)

parser.add_argument('--race-list-file',
                    action='store',
                    nargs='?',
                    const=None,
                    default='race_list.txt',
                    type=str,
                    choices=None,
                    help='race list file(default: race_list.txt)',
                    metavar=None)

parser.add_argument('--period-of-months',
                    action='store',
                    nargs='?',
                    const=None,
                    default=12,
                    type=int,
                    choices=None,
                    help='period of months(default: 12)',
                    metavar=None)

parser.add_argument('-U', '--update-race-list',
                    action='store_true',
                    default=False,
                    help='Update race list(default: False)',)

parser.add_argument('--update-race-data',
                    action='store_true',
                    default=False,
                    help='Update race data(default: False)',)

parser.add_argument('--rebuild',
                    action='store_true',
                    default=False,
                    help='Rebuild mode(default: False)',)

parser.add_argument('--scraping-only',
                    action='store_true',
                    default=False,
                    help='scraping only mode(default: False)',)

parser.add_argument('-V', '--verbose',
                    action='store_true',
                    default=False,
                    help='Verbose mode(default: False)',)

parser.add_argument('--debug',
                    action='store_true',
                    default=False,
                    help='Debug mode(default: False)',)

parser.add_argument('race_id',
                    action='store',
                    nargs='*',
                    const=None,
                    default=None,
                    type=str,
                    choices=None,
                    help='Race ID',
                    metavar=None)

args = parser.parse_args()

# create logger
logger = logging.getLogger(__name__)
formatter = logging.Formatter('[%(asctime)s][%(funcName)s:%(lineno)d][%(levelname)s] %(message)s')
logger.setLevel(logging.DEBUG)

console = logging.StreamHandler()
if args.debug == True:
    console.setLevel(logging.DEBUG)
elif args.verbose == True:
    console.setLevel(logging.INFO)
else:
    console.setLevel(logging.WARNING)
console.setFormatter(formatter)
logger.addHandler(console)

db_config: dict = {
    "protocol": "mysql+pymysql",
    "username": os.getenv("MYSQL_USERNAME","skylark"),
    "password": os.getenv("MYSQL_PASSWORD","skylarkpw!"),
    "hostname": os.getenv("MYSQL_HOSTNAME","localhost"),
    "port"    : os.getenv("MYSQL_PORT", 3306),
    "dbname"  : os.getenv("MYSQL_DATABASE","skylark"),
    "charset" : "utf8mb4"
}

sqlalchemy_db_url: str = "{protocol:s}://{username:s}:{password:s}@{hostname:s}:{port:d}/{dbname:s}?charset={charset:s}".\
    format(**db_config)

if __name__ == "__main__":
    # rebuild時はtempデータを全削除
    if args.rebuild == True:
        if os.path.isdir(args.temp):
            import shutil
            shutil.rmtree(args.temp)

    args.temp = os.path.normcase(args.temp)
    # tempディレクトリ作成
    if os.path.isdir(args.temp) == False:
        os.mkdir(args.temp)

    db_crud = crud.SkylarkCrud(sqlalchemy_db_url, logger=logger)
    try:
        if args.rebuild == True:
            db_crud.drop_tables()

        db_crud.create_tables()

        if args.update_race_list == True or args.update_race_data == True or args.rebuild == True or args.race_id is not None:
            instance = scraper.SkylarkScraper(sqlalchemy_db_url, args = args, logger = logger)
            if args.update_race_list == True:
                logger.info("make race URL list")
                instance.make_race_url_list(period = args.period_of_months)
                instance.export_race_url_list()
            elif args.race_id is not None:
                instance.set_race_url_list(args.race_id)

            instance.import_race_url_list()

            logger.info("Start download race data")
            instance.download()
            logger.info("End download race data")
            instance = None

        if args.scraping_only == True:
            logger.info("* Scraping only mode")
        else:
            count_finish = 0
            count_total = db_crud.get_race_info_count()
            #feature = feature.SkylarkFeature(args = args, logger = logger)
            #for value in dbi.getRaceInfoList():
            #    feature.initialize(race_id = value[0], horse_number = value[1])
            #
            #    count_finish += 1
            #    if (count_finish % 100 == 0 or count_finish == count_total):
            #        logger.info("処理中 ... {0:7.3f} % 完了".format(100.0 * count_finish / count_total))

    except Exception as ex:
        logger.error(f"{ex}")
