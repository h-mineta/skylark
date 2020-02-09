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
                    `race_grade` int(1) UNSIGNED DEFAULT NULL,
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
                    CREATE TABLE IF NOT EXISTS `jockey_tbl` (
                    `jockey_id` bigint(5) UNSIGNED ZEROFILL NOT NULL,
                    `jockey_name` varchar(255) NOT NULL,
                    PRIMARY KEY (`jockey_id`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='騎手テーブル' ROW_FORMAT=DYNAMIC;
                '''
                cursor.execute(sql_create_tbl)

                sql_create_tbl = '''
                    CREATE TABLE IF NOT EXISTS `trainer_tbl` (
                    `trainer_id` bigint(5) UNSIGNED ZEROFILL NOT NULL,
                    `trainer_name` varchar(255) NOT NULL,
                    PRIMARY KEY (`trainer_id`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='トレーナーテーブル' ROW_FORMAT=DYNAMIC;
                '''
                cursor.execute(sql_create_tbl)

                sql_create_tbl = '''
                    CREATE TABLE IF NOT EXISTS `owner_tbl` (
                    `owner_id` bigint(6) UNSIGNED ZEROFILL NOT NULL,
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
                    `jockey_id` bigint(5) UNSIGNED ZEROFILL NOT NULL,
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
                    KEY `race_horse` (`race_id`,`horse_id`),
                    KEY `race_jockey` (`race_id`,`jockey_id`),
                    KEY `race_trainer` (`race_id`,`trainer_id`),
                    KEY `race_owner` (`race_id`,`owner_id`),
                    FOREIGN KEY (`race_id`) REFERENCES race_info_tbl (`id`),
                    FOREIGN KEY (`horse_id`) REFERENCES horse_tbl (`horse_id`),
                    FOREIGN KEY (`jockey_id`) REFERENCES jockey_tbl (`jockey_id`),
                    FOREIGN KEY (`trainer_id`) REFERENCES trainer_tbl (`trainer_id`),
                    FOREIGN KEY (`owner_id`) REFERENCES owner_tbl (`owner_id`)
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
                    PRIMARY KEY (`race_id`,`ticket_type`,`horse_numbers`),
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
                    `sppedrating_avg` double DEFAULT NULL,
                    `winner_avg` double DEFAULT NULL,
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
                    `j_avg_win` double DEFAULT NULL,
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
                    FOREIGN KEY (`race_id`) REFERENCES race_info_tbl (`id`),
                    FOREIGN KEY (`horse_id`) REFERENCES horse_tbl (`horse_id`),
                    FOREIGN KEY (`trainer_id`) REFERENCES trainer_tbl (`trainer_id`),
                    FOREIGN KEY (`jockey_id`) REFERENCES jockey_tbl (`jockey_id`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='予測テーブル' ROW_FORMAT=DYNAMIC;
                '''
                cursor.execute(sql_create_tbl)

                sql_truncate_tbl = '''
                    TRUNCATE TABLE `feature_tbl`;
                '''
                cursor.execute(sql_truncate_tbl)

                self.connection.commit()

        except MySQLdb.Error as ex:
            self.connection.rollback()
            self.logger.critical(ex)
            return False

        return True

    def dropTables(self):
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

    def insertJockey(self, dataset_list):
        sql_insert = '''
            INSERT IGNORE INTO jockey_tbl(
                jockey_id,
                jockey_name
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
                race_grade,
                race_class
            )
            VALUES(
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s
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

    def getRaceInfoCount(self):
        sql_select = '''
            SELECT COUNT(*)
            FROM race_result_tbl AS rr;
        '''

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql_select)

                result = cursor.fetchone()
                return result[0];

        except MySQLdb.Error as ex:
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

        except MySQLdb.Error as ex:
            self.logger.error(ex)
            raise StopIteration

    def getRaceInfo(self, race_id):
        assert race_id > 0

        sql_select = '''
            SELECT ri.*
            FROM race_info_tbl AS ri
            WHERE ri.id = %s
            LIMIT 1;
        '''

        try:
            with self.connection.cursor(MySQLdb.cursors.DictCursor) as cursor:
                cursor.execute(sql_select, [race_id])
                return cursor.fetchone()

        except MySQLdb.Error as ex:
            self.logger.error(ex)

    def getHorseId(self, race_id, horse_number):
        assert race_id > 0 and horse_number > 0

        sql_select = '''
            SELECT rr.horse_id
            FROM race_result_tbl AS rr
            WHERE rr.race_id = %s
            AND rr.horse_number = %s
            LIMIT 1;
        '''

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql_select, [race_id, horse_number])
                result = cursor.fetchone()
                if result is None:
                    return None
                return result[0]

        except MySQLdb.Error as ex:
            self.logger.error(ex)

    def getJockeyId(self, race_id, horse_number):
        assert race_id > 0 and horse_number > 0

        sql_select = '''
            SELECT rr.jockey_id
            FROM race_result_tbl AS rr
            WHERE rr.race_id = %s
            AND rr.horse_number = %s
            LIMIT 1;
        '''

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql_select, [race_id, horse_number])
                result = cursor.fetchone()
                if result is None:
                    return None
                return result[0]

        except MySQLdb.Error as ex:
            self.logger.error(ex)

    def getTrainerId(self, race_id, horse_number):
        assert race_id > 0 and horse_number > 0

        sql_select = '''
            SELECT rr.trainer_id
            FROM race_result_tbl AS rr
            WHERE rr.race_id = %s
            AND rr.horse_number = %s
            LIMIT 1;
        '''

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql_select, [race_id, horse_number])
                result = cursor.fetchone()
                if result is None:
                    return None
                return result[0]

        except MySQLdb.Error as ex:
            self.logger.error(ex)

    def getOwnerId(self, race_id, horse_number):
        assert race_id > 0 and horse_number > 0

        sql_select = '''
            SELECT rr.owner_id
            FROM race_result_tbl AS rr
            WHERE rr.race_id = %s
            AND rr.horse_number = %s
            LIMIT 1;
        '''

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql_select, [race_id, horse_number])
                result = cursor.fetchone()
                if result is None:
                    return None
                return result[0]

        except MySQLdb.Error as ex:
            self.logger.error(ex)

    def getOrderOfFinish(self, race_id, horse_number):
        assert race_id > 0 and horse_number > 0

        sql_select = '''
            SELECT rr.order_of_finish
            FROM race_result_tbl AS rr
            WHERE rr.race_id = %s
            AND rr.horse_number = %s
            LIMIT 1;
        '''

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql_select, [race_id, horse_number])
                result = cursor.fetchone()
                if result is None:
                    return None
                return result[0]

        except MySQLdb.Error as ex:
            self.logger.error(ex)

    def getSpeedFigureLast(self, horse_id, date):
        assert horse_id > 0

        sql_select = '''
            SELECT rr.speed_figure
            FROM race_result_tbl AS rr
            JOIN race_info_tbl AS ri
            ON rr.race_id = ri.id
            WHERE rr.horse_id = %s
            AND ri.date < %s
            AND rr.speed_figure IS NOT NULL
            ORDER BY ri.date DESC
            LIMIT 1;
        '''

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql_select, [horse_id, date])
                result = cursor.fetchone()
                if result is None:
                    return None
                return result[0]

        except MySQLdb.Error as ex:
            self.logger.error(ex)

    def getSpeedFigureAvg(self, horse_id, date, limit):
        assert horse_id > 0 and limit > 0

        sql_select = '''
            SELECT AVG(sub.speed_figure)
            FROM (
                SELECT rr.speed_figure
                FROM race_result_tbl AS rr
                JOIN race_info_tbl AS ri
                ON rr.race_id = ri.id
                WHERE rr.horse_id = %s
                AND ri.date < %s
                AND rr.speed_figure IS NOT NULL
                ORDER BY ri.date DESC
                LIMIT {0:d}
            ) AS sub;
        '''.format(limit)

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql_select, [horse_id, date])
                result = cursor.fetchone()
                if result is None:
                    return None
                return result[0]

        except MySQLdb.Error as ex:
            self.logger.error(ex)

    def getWinnerAvg(self, horse_id, date, limit):
        assert horse_id > 0 and limit > 0

        sql_select = '''
            SELECT AVG(sub.order_of_finish)
            FROM (
                SELECT rr.order_of_finish
                FROM race_result_tbl AS rr
                JOIN race_info_tbl AS ri
                ON rr.race_id = ri.id
                WHERE rr.horse_id = %s
                AND rr.order_of_finish BETWEEN 1 and 3
                AND ri.date < %s
                AND rr.speed_figure IS NOT NULL
                ORDER BY ri.date DESC
                LIMIT {0:d}
            ) AS sub;
        '''.format(limit)

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql_select, [horse_id, date])
                result = cursor.fetchone()
                if result is None:
                    return None
                return result[0]

        except MySQLdb.Error as ex:
            self.logger.error(ex)

    def getWinnerAvg_Free(self, horse_id, date, limit):
        assert horse_id > 0 and limit > 0

        sql_select = '''
            SELECT AVG(sub.order_of_finish)
            FROM (
                SELECT rr.order_of_finish
                FROM race_result_tbl AS rr
                JOIN race_info_tbl AS ri
                ON rr.race_id = ri.id
                WHERE rr.horse_id = %s
                AND rr.order_of_finish BETWEEN 1 and 3
                AND ri.date < %s
                ORDER BY ri.date DESC
                LIMIT {0:d}
            ) AS sub;
        '''.format(limit)

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql_select, [horse_id, date])
                result = cursor.fetchone()
                if result is None:
                    return None
                return result[0]

        except MySQLdb.Error as ex:
            self.logger.error(ex)

    def getDisavesr(self, horse_id, date, distance, limit):
        assert horse_id > 0 and distance > 0 and limit > 0

        sql_select = '''
            SELECT AVG(sub.speed_figure)
            FROM (
                SELECT rr.speed_figure
                FROM race_result_tbl AS rr
                JOIN race_info_tbl AS ri
                ON rr.race_id = ri.id
                WHERE rr.horse_id = %s
                AND ri.date < %s
                AND ri.distance = %s
                AND rr.speed_figure IS NOT NULL
                ORDER BY ri.date DESC
                LIMIT {0:d}
            ) AS sub;
        '''.format(limit)

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql_select, [horse_id, date, distance])
                result = cursor.fetchone()
                if result is None:
                    return None
                return result[0]

        except MySQLdb.Error as ex:
            self.logger.error(ex)

    def getDistanceAvg(self, horse_id, date, limit):
        assert horse_id > 0 and limit > 0

        sql_select = '''
            SELECT AVG(sub.distance)
            FROM (
                SELECT ri.distance
                FROM race_result_tbl AS rr
                JOIN race_info_tbl AS ri
                ON rr.race_id = ri.id
                WHERE rr.horse_id = %s
                AND ri.date < %s
                ORDER BY ri.date DESC
                LIMIT {0:d}
            ) AS sub;
        '''.format(limit)

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql_select, [horse_id, date])
                result = cursor.fetchone()
                if result is None:
                    return None
                return result[0]

        except MySQLdb.Error as ex:
            self.logger.error(ex)

    def getEarningsPerShare(self, horse_id, date, limit):
        assert horse_id > 0 and limit > 0

        sql_select = '''
            SELECT AVG(sub.earning_money)
            FROM (
                SELECT rr.earning_money
                FROM race_result_tbl AS rr
                JOIN race_info_tbl AS ri
                ON rr.race_id = ri.id
                WHERE rr.horse_id = %s
                AND ri.date < %s
                ORDER BY ri.date DESC
                LIMIT {0:d}
            ) AS sub;
        '''.format(limit)

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql_select, [horse_id, date])
                result = cursor.fetchone()
                if result is None:
                    return None
                return result[0]

        except MySQLdb.Error as ex:
            self.logger.error(ex)
