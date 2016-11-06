# -*- coding: utf-8 -*-

#
# Copyright (c) 2016 h-mineta <h-mineta@0nyx.net>
# This software is released under the MIT License.
#

import logging
import MySQLdb
from warnings import filterwarnings

class SkylarkDb:
    def __init__(self, args, logger):
        filterwarnings('ignore', category = MySQLdb.Warning)

        self.args    = args
        self.logger  = logger
        self.db_args = {
            "host"    : self.args.mysql_hostname,
            "port"    : self.args.mysql_port,
            "user"    : self.args.mysql_username,
            "passwd"  : self.args.mysql_password,
            "db"      : self.args.mysql_dbname,
            "charset" : "utf8mb4"
        }

        self.connection = MySQLdb.connect(**self.db_args)
        self.connection.autocommit(False)

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        if exception_type:
            self.logger.error(exception_type)
            self.logger.error(exception_value)

        if self.connection:
            try:
                self.connection.close()
            except MySQLdb.Error as ex:
                self.logger.warning(ex)

    # initialize
    def initialize(self):
        try:
            with self.connection.cursor() as cursor:
                sql_create_tbl = '''
                    CREATE TABLE IF NOT EXISTS `race_info_tbl` (
                      `id` bigint(1) UNSIGNED NOT NULL,
                      `race_name` varchar(255) NOT NULL,
                      `distance` int(1) UNSIGNED NOT NULL,
                      `weather` varchar(8) NOT NULL,
                      `post_time` time(0) NOT NULL,
                      `race_number` bigint(1) UNSIGNED NOT NULL,
                      `run_direction` varchar(8) DEFAULT NULL,
                      `track_surface` varchar(8) NOT NULL,
                      `track_condition` varchar(8) NOT NULL,
                      `track_condition_score` int(1) DEFAULT NULL,
                      `date` date NOT NULL,
                      `place_detail` varchar(16) NOT NULL,
                      `race_class` varchar(64) NOT NULL,
                      PRIMARY KEY (`id`),
                      KEY `race_name` (`race_name`),
                      KEY `date` (`date`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='レース情報テーブル' ROW_FORMAT=DYNAMIC;
                '''
                cursor.execute(sql_create_tbl)

                sql_create_tbl = '''
                    CREATE TABLE IF NOT EXISTS `race_result_tbl` (
                      `race_id` bigint(1) UNSIGNED NOT NULL,
                      `order_of_finish` int(1) UNSIGNED DEFAULT NULL,
                      `bracket_number` int(1) UNSIGNED NOT NULL,
                      `horse_number` int(1) UNSIGNED NOT NULL,
                      `horse_id` bigint(1) UNSIGNED NOT NULL,
                      `sex` varchar(8) NOT NULL,
                      `age` int(1) UNSIGNED NOT NULL,
                      `basis_weight` double NOT NULL,
                      `jockey_id` bigint(5) ZEROFILL NOT NULL,
                      `finishing_time` time(2) DEFAULT NULL,
                      `margin` varchar(16) NOT NULL,
                      `speed_figure` int(1) UNSIGNED DEFAULT NULL,
                      `passing_rank` varchar(16) NOT NULL,
                      `last_phase` double UNSIGNED DEFAULT NULL,
                      `odds` double UNSIGNED DEFAULT NULL,
                      `popularity` int(1) UNSIGNED DEFAULT NULL,
                      `horse_weight` int(1) UNSIGNED DEFAULT NULL,
                      `horse_weight_diff` int(1) DEFAULT NULL,
                      `remark` text DEFAULT NULL,
                      `stable` varchar(16) DEFAULT NULL,
                      `trainer_id` bigint(5) UNSIGNED ZEROFILL NOT NULL,
                      `owner_id` bigint(6) UNSIGNED ZEROFILL NOT NULL,
                      `earning_money` double UNSIGNED DEFAULT NULL,
                      PRIMARY KEY (`race_id`,`horse_number`),
                      FOREIGN KEY (race_id) REFERENCES race_info_tbl (id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='レース結果テーブル' ROW_FORMAT=DYNAMIC;
                '''
                cursor.execute(sql_create_tbl)

                sql_create_tbl = '''
                    CREATE TABLE IF NOT EXISTS `payoff_tbl` (
                      `race_id` bigint(1) UNSIGNED NOT NULL,
                      `ticket_type` int(1) UNSIGNED NOT NULL,
                      `horse_number` int(1) UNSIGNED NOT NULL,
                      `payoff` double UNSIGNED NOT NULL,
                      `popularity` int(1) UNSIGNED NOT NULL,
                      PRIMARY KEY (`race_id`,`ticket_type`,`horse_number`) USING BTREE,
                      FOREIGN KEY (race_id) REFERENCES race_info_tbl (id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='支払いテーブル' ROW_FORMAT=DYNAMIC;
                '''
                cursor.execute(sql_create_tbl)

                self.connection.commit()

        except MySQLdb.Error as ex:
            self.connection.rollback()
            self.logger.critical(ex)
            return False

        return True

    def insertRaceInfo(self, dataset):
        sql_insert = '''
            INSERT IGNORE INTO race_info_tbl(
                id,
                race_name,
                distance,
                weather,
                post_time,
                race_number,
                run_direction,
                track_surface,
                track_condition,
                track_condition_score,
                date,
                place_detail,
                race_class
            )
            VALUES(
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s
            );
        '''

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql_insert, dataset)

                self.connection.commit()

        except MySQLdb.Error as ex:
            self.connection.rollback()
            self.logger.error(ex)

    def insertRaceResult(self, dataset_list):
        sql_insert = '''
            INSERT IGNORE INTO race_result_tbl(
                race_id,
                order_of_finish,
                bracket_number,
                horse_number,
                horse_id,
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
                stable,
                trainer_id,
                owner_id,
                earning_money
            )
            VALUES(
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s
            );
        '''

        try:
            with self.connection.cursor() as cursor:
                for dataset in dataset_list:
                    cursor.execute(sql_insert, dataset)

                self.connection.commit()

        except MySQLdb.Error as ex:
            self.connection.rollback()
            self.logger.error(ex)
