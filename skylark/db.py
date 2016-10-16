# -*- coding: utf-8 -*-

#
# Copyright (c) 2016 h-mineta <h-mineta@0nyx.net>
# This software is released under the MIT License.
#

import logging
import MySQLdb

class SkylarkDb:
    def __init__(self, args, logger):
        self.args       = args
        self.logger     = logger

        self.connector = None
        self.cursor = None

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        if exception_type:
            self.logger.error(exception_type)
            self.logger.error(exception_value)

        if self.cursor:
            try:
                self.cursor.close()
            except Exception as ex:
                self.logger.warning(ex)
            finally:
                self.cursor = None

        if self.connector:
            try:
                self.connector.close()
            except Exception as ex:
                self.logger.warning(ex)
            finally:
                self.connector = None

    # Initialize db & table
    def initialize(self):
        if self.connector == None:
            try:
                self.connector = MySQLdb.connect(
                    host    = self.args.mysql_hostname,
                    port    = self.args.mysql_port,
                    user    = self.args.mysql_username,
                    passwd  = self.args.mysql_password,
                    db      = self.args.mysql_dbname,
                    charset = "utf8mb4")

                self.cursor = self.connector.cursor()

                sql_create_tbl = '''
                    CREATE TABLE IF NOT EXISTS `race_info_tbl` (
                      `id` bigint(1) UNSIGNED NOT NULL AUTO_INCREMENT,
                      `race_name` varchar(255) NOT NULL,
                      `surface` varchar(16) NOT NULL,
                      `distance` int(1) UNSIGNED NOT NULL,
                      `weather` varchar(16) NOT NULL,
                      `surface_state` varchar(16) NOT NULL,
                      `race_start` varchar(16) NOT NULL,
                      `race_number` bigint(1) UNSIGNED NOT NULL,
                      `surface_score` int(1) UNSIGNED DEFAULT NULL,
                      `date` date NOT NULL,
                      `place_detail` varchar(16) NOT NULL,
                      `race_class` varchar(16) NOT NULL,
                      PRIMARY KEY (`id`),
                      KEY `race_name` (`race_name`),
                      KEY `date` (`date`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='レース情報テーブル' ROW_FORMAT=DYNAMIC;
                '''
                self.cursor.execute(sql_create_tbl)

                sql_create_tbl = '''
                    CREATE TABLE IF NOT EXISTS `race_result_tbl` (
                      `race_id` bigint(1) UNSIGNED NOT NULL,
                      `order_of_finish` varchar(255) NOT NULL,
                      `frame_number` int(1) NOT NULL,
                      `horse_number` int(1) NOT NULL,
                      `horse_id` varchar(255) NOT NULL,
                      `sex` varchar(255) NOT NULL,
                      `age` int(1) UNSIGNED NOT NULL,
                      `basis_weight` double NOT NULL,
                      `jockey_id` varchar(255) NOT NULL,
                      `finishing_time` varchar(255) NOT NULL,
                      `length` varchar(255) NOT NULL,
                      `speed_figure` int(1) UNSIGNED DEFAULT NULL,
                      `pass` varchar(255) NOT NULL,
                      `last_phase` double UNSIGNED DEFAULT NULL,
                      `odds` double UNSIGNED DEFAULT NULL,
                      `popularity` int(1) UNSIGNED DEFAULT NULL,
                      `horse_weight` varchar(255) NOT NULL,
                      `remark` text,
                      `stable` varchar(255) NOT NULL,
                      `trainer_id` varchar(255) NOT NULL,
                      `owner_id` varchar(255) NOT NULL,
                      `earning_money` double UNSIGNED DEFAULT NULL,
                      PRIMARY KEY (`race_id`,`horse_number`),
                      FOREIGN KEY (race_id) REFERENCES race_info_tbl (id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='レース結果テーブル' ROW_FORMAT=DYNAMIC;
                '''
                self.cursor.execute(sql_create_tbl)

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
                self.cursor.execute(sql_create_tbl)

            except Exception as ex:
                self.logger.error(ex)
                return None

        return self
