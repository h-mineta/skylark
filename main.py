#!/usr/bin/python3.5 -tt
# -*- coding: utf-8 -*-

#
# Copyright (c) 2016 h-mineta <h-mineta@0nyx.net>
# This software is released under the MIT License.
#

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

parser.add_argument('-U', '--update-race-data',
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
                    default='http://localhost:5432',
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
                    default=5432,
                    type=int,
                    choices=None,
                    help='MySQL port(default: 5432)',
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

args = parser.parse_args()

# create logger
logger = logging.getLogger(__name__)
formatter = logging.Formatter('[%(asctime)s][%(funcName)s:%(lineno)d][%(levelname)s] %(message)s')

if args.debug == True:
    logger.setLevel(logging.DEBUG)
elif args.verbose == True:
    logger.setLevel(logging.INFO)
else:
    logger.setLevel(logging.WARNING)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)

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
        dbi.initialize()

        if args.update_race_data == True or args.rebuild == True:
            scraper = scraper.SkylarkScraper(args = args, logger = logger)

            logger.info("make race URL list.")
            scraper.makeRaceUrlList(period = args.period_of_months)
            scraper.exportRaceUrlList()

            logger.info("download race data.")
            scraper.downloadAndScraping(dbi = dbi)
