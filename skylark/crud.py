# -*- coding: utf-8 -*-

#
# Copyright (c) MINETA "m10i" Hiroki <h-mineta@0nyx.net>
# This software is released under the MIT License.
#

from logging import Logger
from sqlalchemy import create_engine, desc, func
from sqlalchemy.orm import sessionmaker

from skylark.models import Base, Feature, Horse, Jockey, Trainer, Owner, RaceInfo, RaceResult, Payoff

class SkylarkCrud:
    def __init__(self, db_url: str, logger: Logger):
        # Create the engine and session
        self.engine = create_engine(
            db_url,
            pool_size=1,
            max_overflow=0,
            pool_recycle=3600,
            pool_pre_ping=True
        )
        self.session = sessionmaker(bind=self.engine)
        # Set the logger
        self.logger: Logger = logger

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            self.engine.dispose()
        except Exception as ex:
            self.logger.error(f"{ex}")

    def create_tables(self):
        Base.metadata.create_all(self.engine)

    def create_table(self, table_name: str):
        try:
            table = Base.metadata.tables.get(table_name)
            if table is not None:
                table.create(self.engine)
            else:
                self.logger.warning(f"Table {table_name} does not exist.")
        except Exception as ex:
            self.logger.error(f"{ex}")

    def drop_tables(self):
        Base.metadata.drop_all(self.engine)

    def drop_table(self, table_name: str):
        try:
            table = Base.metadata.tables.get(table_name)
            if table is not None:
                table.drop(self.engine)
            else:
                self.logger.warning(f"Table {table_name} does not exist.")
        except Exception as ex:
            self.logger.error(f"{ex}")

    def get_horse(self, horse_id) -> Horse|None:
        with self.session() as session:
            try:
                horse = session.query(Horse).filter_by(id=horse_id).first()
                return horse
            except Exception as ex:
                self.logger.error(f"{ex}")
        return None

    def get_horse_id(self, race_id, horse_number) -> int|None:
        assert race_id > 0 and horse_number > 0

        with self.session() as session:
            try:
                horse = session.query(RaceResult.horse_id).filter_by(race_id=race_id, horse_number=horse_number).first()
                return horse[0] if horse else None
            except Exception as ex:
                self.logger.error(ex)
        return None

    def upsert_horses(self, dataset_list: list):
        with self.session() as session:
            try:
                for dataset in dataset_list:
                    horse = Horse(**dataset)
                    session.merge(horse)
                session.commit()
            except Exception as ex:
                session.rollback()
                raise ex

    def get_jockey(self, jockey_id) -> Jockey|None:
        with self.session() as session:
            try:
                jockey = session.query(Jockey).filter_by(id=jockey_id).first()
                return jockey
            except Exception as ex:
                self.logger.error(ex)
        return None

    def get_jockey_id(self, race_id, horse_number) -> int|None:
        assert race_id > 0 and horse_number > 0

        with self.session() as session:
            try:
                jockey = session.query(RaceResult.jockey_id).filter_by(race_id=race_id, horse_number=horse_number).first()
                return jockey[0] if jockey else None
            except Exception as ex:
                self.logger.error(ex)
        return None

    def upsert_jockeys(self, dataset_list: list) -> None:
        with self.session() as session:
            try:
                for dataset in dataset_list:
                    jockey = Jockey(**dataset)
                    session.merge(jockey)
                session.commit()
            except Exception as ex:
                session.rollback()
                raise ex

    def get_trainer(self, trainer_id) -> Trainer|None:
        with self.session() as session:
            try:
                trainer = session.query(Trainer).filter_by(trainer_id=trainer_id).first()
                return trainer
            except Exception as ex:
                self.logger.error(ex)
        return None

    def get_trainer_id(self, race_id, horse_number) -> int|None:
        assert race_id > 0 and horse_number > 0

        with self.session() as session:
            try:
                result = session.query(RaceResult.trainer_id).filter_by(race_id=race_id, horse_number=horse_number).first()
                return result[0] if result else None
            except Exception as ex:
                self.logger.error(ex)
        return None

    def upsert_trainers(self, dataset_list: list) -> None:
        with self.session() as session:
            try:
                for dataset in dataset_list:
                    trainer = Trainer(**dataset)
                    session.merge(trainer)
                session.commit()
            except Exception as ex:
                session.rollback()
                raise ex

    def get_owner(self, owner_id) -> Owner|None:
        with self.session() as session:
            try:
                owner = session.query(Owner).filter_by(owner_id=owner_id).first()
                return owner
            except Exception as ex:
                self.logger.error(ex)
        return None

    def get_owner_id(self, race_id, horse_number) -> str|None:
        assert race_id > 0 and horse_number > 0

        with self.session() as session:
            try:
                owner = session.query(RaceResult.owner_id).filter_by(race_id=race_id, horse_number=horse_number).first()
                return owner[0] if owner else None
            except Exception as ex:
                self.logger.error(ex)
        return None

    def upsert_owners(self, dataset_list: list) -> None:
        with self.session() as session:
            try:
                for dataset in dataset_list:
                    owner = Owner(**dataset)
                    session.merge(owner)
                session.commit()
            except Exception as ex:
                session.rollback()
                raise ex

    def get_race_info(self, race_id) -> RaceInfo|None:
        with self.session() as session:
            try:
                race_info = session.query(RaceInfo).filter_by(id=race_id).first()
                return race_info
            except Exception as ex:
                self.logger.error(ex)
        return None

    def upsert_race_info(self, dataset: dict) -> None:
        with self.session() as session:
            try:
                race_info = RaceInfo(**dataset)
                session.merge(race_info)
                session.commit()
            except Exception as ex:
                session.rollback()
                raise ex

    def get_race_results(self) -> list[RaceResult] | None:
        with self.session() as session:
            try:
                return session.query(RaceResult).order_by(RaceResult.race_id, RaceResult.horse_number).all()
            except Exception as ex:
                self.logger.error(ex)
        return None

    def get_race_result(self, race_id: int, horse_number: int) -> RaceResult|None:
        with self.session() as session:
            try:
                return session.query(RaceResult).filter_by(race_id=race_id, horse_number=horse_number).first()
            except Exception as ex:
                self.logger.error(ex)
        return None

    def upsert_race_results(self, dataset_list: list):
        with self.session() as session:
            try:
                for dataset in dataset_list:
                    race_result = RaceResult(**dataset)
                    session.merge(race_result)
                session.commit()
            except Exception as ex:
                session.rollback()
                raise ex

    def get_order_of_finish(self, race_id: int, horse_number: int) -> float|None:
        assert race_id > 0 and horse_number > 0

        with self.session() as session:
            try:
                result = (
                    session.query(RaceResult.order_of_finish)
                    .filter(
                        RaceResult.race_id == race_id,
                        RaceResult.horse_number == horse_number
                    )
                    .limit(1)
                    .scalar()
                )
                return result
            except Exception as ex:
                print(ex)
        return None

    def upsert_payoffs(self, dataset_list: list) -> None:
        with self.session() as session:
            try:
                for dataset in dataset_list:
                    payoff = Payoff(**dataset)
                    session.merge(payoff)
                session.commit()
            except Exception as ex:
                session.rollback()
                raise ex

    def insert_features(self, dataset_list: list) -> None:
        with self.session() as session:
            try:
                for dataset in dataset_list:
                    feature = Feature(**dataset)
                    session.merge(feature)
                session.commit()
            except Exception as ex:
                session.rollback()
                raise ex

    def get_speed_figure_last(self, horse_id: int, date) -> float|None:
        assert horse_id > 0

        with self.session() as session:
            try:
                result = (
                    session.query(RaceResult.speed_figure)
                    .join(RaceInfo, RaceResult.race_id == RaceInfo.id)
                    .filter(
                        RaceResult.horse_id == horse_id,
                        RaceInfo.date < date,
                        RaceResult.speed_figure.isnot(None)
                    )
                    .order_by(desc(RaceInfo.date))
                    .limit(1)
                    .scalar()
                )
                return result
            except Exception as ex:
                print(ex)
        return None

    def get_speed_figure_avg(self, horse_id: int, date, limit: int) -> float|None:
        assert horse_id > 0 and limit > 0

        with self.session() as session:
            try:
                subquery = (
                    session.query(RaceResult.speed_figure)
                    .join(RaceInfo, RaceResult.race_id == RaceInfo.id)
                    .filter(
                        RaceResult.horse_id == horse_id,
                        RaceInfo.date < date,
                        RaceResult.speed_figure.isnot(None)
                    )
                    .order_by(desc(RaceInfo.date))
                    .limit(limit)
                    .subquery()
                )

                result = session.query(func.avg(subquery.c.speed_figure)).scalar()
                return result
            except Exception as ex:
                self.logger.error(ex)
        return None

    def get_winner_avg(self, horse_id: int, date, limit: int) -> float|None:
        assert horse_id > 0 and limit > 0

        with self.session() as session:
            try:
                subquery = (
                    session.query(RaceResult.order_of_finish)
                    .join(RaceInfo, RaceResult.race_id == RaceInfo.id)
                    .filter(
                        RaceResult.horse_id == horse_id,
                        RaceResult.order_of_finish.between(1, 3),
                        RaceInfo.date < date,
                        RaceResult.speed_figure.isnot(None)
                    )
                    .order_by(desc(RaceInfo.date))
                    .limit(limit)
                    .subquery()
                )

                result = session.query(func.avg(subquery.c.order_of_finish)).scalar()
                return result
            except Exception as ex:
                self.logger.error(ex)
        return None

    def get_disavesr(self, horse_id: int, date, distance: int, limit: int) -> float|None:
        """
        特定の馬の過去のレース距離データを基に、speed_figure の平均を取得します。
        """
        assert horse_id > 0 and distance > 0 and limit > 0

        with self.session() as session:
            try:
                subquery = (
                    session.query(RaceResult.speed_figure)
                    .join(RaceInfo, RaceResult.race_id == RaceInfo.id)
                    .filter(
                        RaceResult.horse_id == horse_id,
                        RaceInfo.date < date,
                        RaceInfo.distance == distance,
                        RaceResult.speed_figure.isnot(None)
                    )
                    .order_by(desc(RaceInfo.date))
                    .limit(limit)
                    .subquery()
                )

                result = session.query(func.avg(subquery.c.speed_figure)).scalar()
                return result
            except Exception as ex:
                self.logger.error(ex)
        return None

    def get_distance_avg(self, horse_id: int, date, limit: int) -> float|None:
        """
        特定の馬の過去のレース距離の平均を取得します。
        """
        assert horse_id > 0 and limit > 0

        with self.session() as session:
            try:
                subquery = (
                    session.query(RaceInfo.distance)
                    .join(RaceResult, RaceInfo.id == RaceResult.race_id)
                    .filter(
                        RaceResult.horse_id == horse_id,
                        RaceInfo.date < date
                    )
                    .order_by(desc(RaceInfo.date))
                    .limit(limit)
                    .subquery()
                )

                result = session.query(func.avg(subquery.c.distance)).scalar()
                return result
            except Exception as ex:
                self.logger.error(ex)
        return None

    def get_earnings_per_share(self, horse_id: int, date, limit: int) -> float|None:
        """
        特定の馬の過去のレースでの賞金の平均を取得します。
        """
        assert horse_id > 0 and limit > 0

        with self.session() as session:
            try:
                subquery = (
                    session.query(RaceResult.earning_money)
                    .join(RaceInfo, RaceResult.race_id == RaceInfo.id)
                    .filter(
                        RaceResult.horse_id == horse_id,
                        RaceInfo.date < date
                    )
                    .order_by(desc(RaceInfo.date))
                    .limit(limit)
                    .subquery()
                )

                result = session.query(func.avg(subquery.c.earning_money)).scalar()
                return result
            except Exception as ex:
                self.logger.error(ex)
        return None
