#!/usr/bin/env python3.12
# -*- coding: utf-8 -*-

#
# Copyright (c) MINETA "m10i" Hiroki <h-mineta@0nyx.net>
# This software is released under the MIT License.
#

import argparse
import logging
import os
import concurrent.futures
import multiprocessing

from dotenv import load_dotenv
from tqdm import tqdm
from skylark import crud, feature, scraper_db

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

parser.add_argument('--rebuild-all-tables',
                    action='store_true',
                    default=False,
                    help='Rebuild all tables(default: False)',)

parser.add_argument('-Rf', '--rebuild-feature',
                    action='store_true',
                    default=False,
                    help='Rebuild feature table(default: False)',)

parser.add_argument('-U', '--update-race-list',
                    action='store_true',
                    default=False,
                    help='Update race list(default: False)',)

parser.add_argument('-S', '--scraping',
                    action='store_true',
                    default=False,
                    help='scraping mode(default: False)',)

parser.add_argument('-F', '--feature',
                    action='store_true',
                    default=False,
                    help='feature mode(default: False)',)

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
else:
    console.setLevel(logging.INFO)

console.setFormatter(formatter)
logger.addHandler(console)

db_config: dict = {
    "protocol": "mysql+pymysql",
    "username": os.getenv("MYSQL_USERNAME","skylark"),
    "password": os.getenv("MYSQL_PASSWORD","skylarkpw!"),
    "hostname": os.getenv("MYSQL_HOSTNAME","localhost"),
    "port"    : int(os.getenv("MYSQL_PORT", 3306)),
    "dbname"  : os.getenv("MYSQL_DATABASE","skylark"),
    "charset" : "utf8mb4"
}

sqlalchemy_db_url: str = "{protocol:s}://{username:s}:{password:s}@{hostname:s}:{port:d}/{dbname:s}?charset={charset:s}".\
    format(**db_config)

def process_feature(args_tuple):
    sqlalchemy_db_url, args, logger, race_result = args_tuple
    db_crud = crud.SkylarkCrud(sqlalchemy_db_url, logger=logger)
    skylark_feature = feature.SkylarkFeature(args=args, logger=logger)
    skylark_feature.initialize(db_crud, race_id=race_result.race_id, horse_number=race_result.horse_number)

def main(args: argparse.Namespace, logger: logging.Logger, sqlalchemy_db_url: str):
    args.temp = os.path.normcase(args.temp)
    # tempディレクトリ作成
    if os.path.isdir(args.temp) == False:
        os.mkdir(args.temp)

    db_crud = crud.SkylarkCrud(sqlalchemy_db_url, logger=logger)
    try:
        if args.rebuild_all_tables == True:
            db_crud.drop_tables()

        if args.rebuild_feature == True:
            db_crud.drop_table("feature_tbl")

        db_crud.create_tables()

        if args.update_race_list == True:
            instance = scraper_db.SkylarkScraperDb(sqlalchemy_db_url, args = args, logger = logger)
            if args.update_race_list == True:
                logger.info("make race URL list")
                instance.make_race_url_list(period = args.period_of_months)
                instance.export_race_url_list()

        if args.scraping == True:
            instance = scraper_db.SkylarkScraperDb(sqlalchemy_db_url, args = args, logger = logger)
            if len(args.race_id) > 0:
                instance.set_race_url_list(args.race_id)
            else:
                instance.import_race_url_list()

            logger.info("Start download race data")
            instance.download()
            logger.info("End download race data")

        if args.feature == True or args.rebuild_feature == True:
            race_result_list = db_crud.get_race_results()
            if not race_result_list:
                logger.warning("Failed to retrieve race results.")
                return

            logger.info("Start feature")
            max_workers = min(8, multiprocessing.cpu_count())
            with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
                args_iter = ((sqlalchemy_db_url, args, logger, race_result) for race_result in race_result_list)
                list(tqdm(executor.map(process_feature, args_iter), total=len(race_result_list)))
            logger.info("End feature")

    except Exception as ex:
        logger.error(ex,exc_info=True)

if __name__ == "__main__":
    main(args, logger, sqlalchemy_db_url)
