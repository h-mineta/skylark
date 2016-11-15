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

        self.ticket_type_list = [
            "単勝",
            "複勝",
            "枠連",
            "馬連",
            "ワイド",
            "馬単",
            "三連複",
            "三連単"
        ]

        self.cursor_iter = None

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
                    `track_condition_score` int(1) UNSIGNED DEFAULT NULL,
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
                    CREATE TABLE IF NOT EXISTS `horse_tbl` (
                    `horse_id` bigint(1) UNSIGNED NOT NULL,
                    `horse_name` varchar(255) NOT NULL,
                    PRIMARY KEY (`horse_id`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='競走馬テーブル' ROW_FORMAT=DYNAMIC;
                '''
                cursor.execute(sql_create_tbl)

                sql_create_tbl = '''
                    CREATE TABLE IF NOT EXISTS `trainer_tbl` (
                    `trainer_id` bigint(1) UNSIGNED NOT NULL,
                    `trainer_name` varchar(255) NOT NULL,
                    PRIMARY KEY (`trainer_id`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='トレーナーテーブル' ROW_FORMAT=DYNAMIC;
                '''
                cursor.execute(sql_create_tbl)

                sql_create_tbl = '''
                    CREATE TABLE IF NOT EXISTS `owner_tbl` (
                    `owner_id` bigint(1) UNSIGNED NOT NULL,
                    `owner_name` varchar(255) NOT NULL,
                    PRIMARY KEY (`owner_id`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='オーナーテーブル' ROW_FORMAT=DYNAMIC;
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
                    `stable` varchar(8) NOT NULL,
                    `trainer_id` bigint(5) UNSIGNED ZEROFILL NOT NULL,
                    `owner_id` bigint(6) UNSIGNED ZEROFILL NOT NULL,
                    `earning_money` double UNSIGNED DEFAULT NULL,
                    PRIMARY KEY (`race_id`,`horse_number`),
                    FOREIGN KEY (race_id) REFERENCES race_info_tbl (id),
                    FOREIGN KEY (horse_id) REFERENCES horse_tbl (horse_id),
                    FOREIGN KEY (trainer_id) REFERENCES trainer_tbl (trainer_id),
                    FOREIGN KEY (owner_id) REFERENCES owner_tbl (owner_id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='レース結果テーブル' ROW_FORMAT=DYNAMIC;
                '''
                cursor.execute(sql_create_tbl)

                sql_create_tbl = '''
                    CREATE TABLE IF NOT EXISTS `payoff_tbl` (
                    `race_id` bigint(1) UNSIGNED NOT NULL,
                    `ticket_type` int(1) UNSIGNED NOT NULL,
                    `horse_numbers` varchar(16) NOT NULL,
                    `payoff` int(1) UNSIGNED NOT NULL,
                    `popularity` int(1) UNSIGNED NOT NULL,
                    PRIMARY KEY (`race_id`,`ticket_type`,`horse_number`) USING BTREE,
                    FOREIGN KEY (race_id) REFERENCES race_info_tbl (id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='支払いテーブル' ROW_FORMAT=DYNAMIC;
                '''
                cursor.execute(sql_create_tbl)

                sql_create_tbl = '''
                    CREATE TABLE IF NOT EXISTS `feature_tbl` (
                    `race_id` bigint(1) UNSIGNED NOT NULL,
                    `horse_number` int(1) UNSIGNED NOT NULL,
                    `grade` int(1) UNSIGNED NOT NULL,
                    `order_of_finish` int(1) UNSIGNED DEFAULT NULL,
                    `horse_id` bigint(1) UNSIGNED NOT NULL,
                    `jockey_id` bigint(5) ZEROFILL NOT NULL,
                    `trainer_id` bigint(5) UNSIGNED ZEROFILL NOT NULL,
                    `sex` varchar(8) NOT NULL,
                    `age` int(1) UNSIGNED NOT NULL,
                    `basis_weight` double UNSIGNED DEFAULT NULL,
                    `weather` varchar(8) NOT NULL,
                    `track_surface` varchar(8) NOT NULL,
                    `track_condition` varchar(8) NOT NULL,
                    `track_condition_score` double UNSIGNED DEFAULT NULL,

                    `sppedrating_last4_avg` double DEFAULT NULL,
                    `winner_last4_avg` double DEFAULT NULL,
                    `horse_weight` int(1) DEFAULT NULL,

                    `disavesr` double DEFAULT NULL,
                    `dis_roc` double DEFAULT NULL,
                    `distance` double DEFAULT NULL,
                    `dsl` double DEFAULT NULL,
                    `enter_times` double DEFAULT NULL,
                    `eps` double DEFAULT NULL,
                    `hweight` double DEFAULT NULL,
                    `jwinper` double DEFAULT NULL,
                    `odds` double DEFAULT NULL,
                    `owinper` double DEFAULT NULL,
                    `pre_sra` double DEFAULT NULL,
                    `twinper` double DEFAULT NULL,
                    `winRun` double DEFAULT NULL,
                    `j_eps` double DEFAULT NULL,
                    `j_avg_win4` double DEFAULT NULL,
                    `pre_oof` double DEFAULT NULL,
                    `pre2_oof` double DEFAULT NULL,
                    `month` double DEFAULT NULL,
                    `riding_strong_jockey` double DEFAULT NULL,
                    `running_style` double DEFAULT NULL,
                    `pre_late_start` double DEFAULT NULL,
                    `pre_last_phase` double DEFAULT NULL,
                    `late_start_per` double DEFAULT NULL,
                    `course` text,
                    `place_code` text,
                    `head_count` double DEFAULT NULL,
                    `preHead_count` double DEFAULT NULL,

                    `surface_changed` double DEFAULT NULL,
                    `grade_changed` double DEFAULT NULL,
                    `pre_margin` double DEFAULT NULL,
                    `female_only` double DEFAULT NULL,

                    PRIMARY KEY (`race_id`,`horse_number`),
                    FOREIGN KEY (race_id) REFERENCES race_info_tbl (id),
                    FOREIGN KEY (horse_id) REFERENCES horse_tbl (horse_id),
                    FOREIGN KEY (trainer_id) REFERENCES trainer_tbl (trainer_id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='予測テーブル' ROW_FORMAT=DYNAMIC;
                '''
                cursor.execute(sql_create_tbl)

                self.connection.commit()

        except MySQLdb.Error as ex:
            self.connection.rollback()
            self.logger.critical(ex)
            return False

        return True

    def destroyTables(self):
        try:
            with self.connection.cursor() as cursor:
                sql_drop_tbl = '''
                    DROP TABLE
                        `race_info_tbl`,
                        `race_result_tbl`,
                        `payoff_tbl`,
                        `horse_tbl`,
                        `owner_tbl`,
                        `trainer_tbl`,
                        `feature_tbl`;
                '''
                cursor.execute(sql_drop_tbl)

                self.connection.commit()

        except MySQLdb.Error as ex:
            self.connection.rollback()
            self.logger.critical(ex)

    def insertHorse(self, dataset_list):
        sql_insert = '''
            INSERT IGNORE INTO horse_tbl(
                horse_id,
                horse_name
            )
            VALUES(
                %s, %s
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

    def insertTrainer(self, dataset_list):
        sql_insert = '''
            INSERT IGNORE INTO trainer_tbl(
                trainer_id,
                trainer_name
            )
            VALUES(
                %s, %s
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

    def insertOwner(self, dataset_list):
        sql_insert = '''
            INSERT IGNORE INTO owner_tbl(
                owner_id,
                owner_name
            )
            VALUES(
                %s, %s
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

    def insertPayoff(self, dataset_list):
        sql_insert = '''
            INSERT IGNORE INTO payoff_tbl(
                race_id,
                ticket_type,
                horse_numbers,
                payoff,
                popularity
            )
            VALUES(
                %s, %s, %s, %s, %s
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

    def getRaceInfoList(self):
        sql_select = '''
            SELECT rr.race_id, rr.horse_number
            FROM race_result_tbl AS rr
            ORDER BY 1 ASC, 2 ASC;
        '''

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql_select)

                for value in iter(cursor):
                    yield value

        except Error as ex:
            self.logger.error(ex)
            raise StopIteration

    def getRaceInfo(self, race_id):
        assert race_id > 0

        sql_select = '''
            SELECT ri.*
            FROM race_info_tbl AS ri
            WHERE id = %s
            LIMIT 1;
        '''

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql_select, [race_id])
                return cursor.fetchone()

        except MySQLdb.Error as ex:
            self.logger.error(ex)

    def getOrderOfFinish(self, race_id, horse_number):
        sql_select = '''
            SELECT rr.order_of_finish
            FROM race_result_tbl AS rr
            WHERE race_id = %s
            AND horse_number = %s
            LIMIT 1;
        '''

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql_select, [race_id, horse_number])
                return cursor.fetchone()

        except MySQLdb.Error as ex:
            self.logger.error(ex)
