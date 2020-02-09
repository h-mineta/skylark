#!/usr/bin/python3 -tt
# -*- coding: utf-8 -*-

#
# Copyright (c) 2016-2020 h-mineta <h-mineta@0nyx.net>
# This software is released under the MIT License.
#
# pip3 install pyquery aiohttp mysqlclient

from skylark import *
import argparse
import logging
import os

parser = argparse.ArgumentParser(
    description='This script is a program that netkeiba.com scraping.')

parser.add_argument('-u', '--username',
                    action='store',
                    nargs='?',
                    const=None,
                    default=None,
                    type=str,
                    choices=None,
                    help='Netkeiba login username(default: None)',
                    metavar=None)

parser.add_argument('-p', '--password',
                    action='store',
                    nargs='?',
                    const=None,
                    default=None,
                    type=str,
                    choices=None,
                    help='Netkeiba login password(default: None)',
                    metavar=None)

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

parser.add_argument('-V', '--verbose',
                    action='store_true',
                    default=False,
                    help='Verbose mode(default: False)',)

parser.add_argument('--debug',
                    action='store_true',
                    default=False,
                    help='Debug mode(default: False)',)

parser.add_argument('--download-concurrency',
                    action='store',
                    nargs='?',
                    const=None,
                    default=4,
                    type=int,
                    choices=None,
                    help='download concurrency(default: 4)',
                    metavar=None)

parser.add_argument('--http-timeout',
                    action='store',
                    nargs='?',
                    const=None,
                    default=20,
                    type=int,
                    choices=None,
                    help='http timeout(default: 20)',
                    metavar=None)

parser.add_argument('--http-proxy',
                    action='store',
                    nargs='?',
                    const=None,
                    default=None,
                    type=str,
                    choices=None,
                    help='HTTP Proxy(default: None)',
                    metavar=None)

parser.add_argument('--mysql-hostname',
                    action='store',
                    nargs='?',
                    const=None,
                    default='localhost',
                    type=str,
                    choices=None,
                    help='MySQL hostname(default: localhost)',
                    metavar=None)

parser.add_argument('--mysql-port',
                    action='store',
                    nargs='?',
                    const=None,
                    default=3306,
                    type=int,
                    choices=None,
                    help='MySQL port(default: 3306)',
                    metavar=None)

parser.add_argument('--mysql-username',
                    action='store',
                    nargs='?',
                    const=None,
                    default='skylark',
                    type=str,
                    choices=None,
                    help='MySQL username(default: skylark)',
                    metavar=None)

parser.add_argument('--mysql-password',
                    action='store',
                    nargs='?',
                    const=None,
                    default='skylarkpw',
                    type=str,
                    choices=None,
                    help='MySQL password(default: skylarkpw)',
                    metavar=None)

parser.add_argument('--mysql-dbname',
                    action='store',
                    nargs='?',
                    const=None,
                    default='skylark',
                    type=str,
                    choices=None,
                    help='MySQL database name(default: skylark)',
                    metavar=None)

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

    with db.SkylarkDb(args = args, logger = logger) as dbi:
        if args.rebuild == True:
            dbi.dropTables()

        result = dbi.initialize()

        if result == False:
            logger.critical("MySQL connection failed")
            exit(1)

        if args.update_race_list == True or args.update_race_data == True or args.rebuild == True or args.race_id is not None:
            instance = scraper.SkylarkScraper(args = args, logger = logger)
            if args.update_race_list == True:
                logger.info("make race URL list")
                instance.makeRaceUrlList(period = args.period_of_months)
                instance.exportRaceUrlList()
            elif args.race_id is not None:
                instance.setRaceUrlList(args.race_id)
            else:
                instance.importRaceUrlList()

            logger.info("download race data")
            instance.download()
            instance = None

        count_finish = 0
        count_total = dbi.getRaceInfoCount()
        feature = feature.SkylarkFeature(args = args, logger = logger)
        for value in dbi.getRaceInfoList():
            count_finish += 1
            if (count_finish % 100 == 0):
                logger.info("処理中 ... {0:7.3f} % 完了".format(100.0 * count_finish / count_total))

            feature.initialize(race_id = value[0], horse_number = value[1])
