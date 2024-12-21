# -*- coding: utf-8 -*-

#
# Copyright (c) 2024 MINETA "m10i" Hiroki <h-mineta@0nyx.net>
# This software is released under the MIT License.
#

from logging import Logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from skylark.models import Base, Feature, Horse, Jockey, Trainer, Owner, RaceInfo, RaceResult, Payoff

class SkylarkCrud:
    def __init__(self, db_url: str, logger: Logger):
        # Create the engine and session
        self.engine = create_engine(db_url)
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

    def drop_tables(self):
        Base.metadata.drop_all(self.engine)

    def insert_horses(self, dataset_list: list):
        with self.session() as session:
            try:
                for dataset in dataset_list:
                    horse = Horse(**dataset)
                    session.merge(horse)
                session.commit()
            except Exception as ex:
                session.rollback()
                raise ex

    def get_horse(self, horse_id) -> Horse|None:
        with self.session() as session:
            try:
                horse = session.query(Horse).filter_by(id=horse_id).first()
                return horse
            except Exception as ex:
                self.logger.error(f"{ex}")
        return None

    def get_horse_id(self, race_id, horse_number) -> int|None:
        with self.session() as session:
            try:
                horse_id = session.query(RaceResult.horse_id).filter_by(race_id=race_id, horse_number=horse_number).first()
                return horse_id
            except Exception as ex:
                self.logger.error(f"{ex}")
        return None

    def insert_jockeys(self, dataset_list: list):
        with self.session() as session:
            try:
                for dataset in dataset_list:
                    jockey = Jockey(**dataset)
                    session.merge(jockey)
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
                self.logger.error(f"{ex}")
        return None

    def get_jockey_id(self, race_id, horse_number) -> int|None:
        with self.session() as session:
            try:
                jockey_id = session.query(RaceResult.jockey_id).filter_by(race_id=race_id, horse_number=horse_number).first()
                return jockey_id
            except Exception as ex:
                self.logger.error(f"{ex}")
        return None

    def insert_trainers(self, dataset_list: list):
        with self.session() as session:
            try:
                for dataset in dataset_list:
                    trainer = Trainer(**dataset)
                    session.merge(trainer)
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
                self.logger.error(f"{ex}")
        return None

    def get_trainer_id(self, race_id, horse_number) -> int|None:
        with self.session() as session:
            try:
                trainer_id = session.query(RaceResult.trainer_id).filter_by(race_id=race_id, horse_number=horse_number).first()
                return trainer_id
            except Exception as ex:
                self.logger.error(f"{ex}")
        return None

    def insert_owners(self, dataset_list: list):
        with self.session() as session:
            try:
                for dataset in dataset_list:
                    owner = Owner(**dataset)
                    session.merge(owner)
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
                self.logger.error(f"{ex}")
        return None

    def get_owner_id(self, race_id, horse_number) -> str|None:
        with self.session() as session:
            try:
                owner_id = session.query(RaceResult.owner_id).filter_by(race_id=race_id, horse_number=horse_number).first()
                return owner_id
            except Exception as ex:
                self.logger.error(f"{ex}")
        return None

    def insert_race_info(self, dataset: dict):
        with self.session() as session:
            try:
                race_info = RaceInfo(**dataset)
                session.merge(race_info)
                session.commit()
            except Exception as ex:
                session.rollback()
                raise ex

    def get_race_info(self, race_id):
        with self.session() as session:
            try:
                race_info = session.query(RaceInfo).filter_by(id=race_id).first()
                return race_info
            except Exception as ex:
                self.logger.error(f"{ex}")
        return None

    def get_race_info_count(self):
        with self.session() as session:
            try:
                count = session.query(RaceInfo).count()
                return count
            except Exception as ex:
                self.logger.error(f"{ex}")
        return None

    def get_race_info_list(self):
        with self.session() as session:
            try:
                race_info_list = session.query(RaceInfo).order_by(RaceInfo.id).all()
                return race_info_list
            except Exception as ex:
                self.logger.error(f"{ex}")
        return None

    def insert_race_results(self, dataset_list: list):
        with self.session() as session:
            try:
                for dataset in dataset_list:
                    race_info = RaceResult(**dataset)
                    session.merge(race_info)
                session.commit()
            except Exception as ex:
                session.rollback()
                raise ex

    def insert_payoffs(self, dataset_list: list):
        with self.session() as session:
            try:
                for dataset in dataset_list:
                    payoff = Payoff(**dataset)
                    session.merge(payoff)
                session.commit()
            except Exception as ex:
                session.rollback()
                raise ex

    def insert_features(self, dataset_list: list):
        with self.session() as session:
            try:
                for dataset in dataset_list:
                    feature = Feature(**dataset)
                    session.merge(feature)
                session.commit()
            except Exception as ex:
                session.rollback()
                raise ex

    def update_horse(self, horse_id, name):
        with self.session() as session:
            try:
                horse = session.query(Horse).filter_by(id=horse_id).first()
                if horse:
                    horse.name = name
                    session.commit()
            except Exception as ex:
                session.rollback()
                self.logger.error(f"{ex}")
