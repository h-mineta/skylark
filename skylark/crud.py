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

    def insert_race_infos(self, dataset_list: list):
        with self.session() as session:
            try:
                for dataset in dataset_list:
                    race_info = RaceInfo(**dataset)
                    session.merge(race_info)
                session.commit()
            except Exception as ex:
                session.rollback()
                raise ex

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

    def delete_horse(self, horse_id):
        with self.session() as session:
            try:
                horse = session.query(Horse).filter_by(id=horse_id).first()
                if horse:
                    session.delete(horse)
                    session.commit()
            except Exception as ex:
                session.rollback()
                self.logger.error(f"{ex}")

    def get_all_horses(self):
        with self.session() as session:
            try:
                horses = session.query(Horse).all()
                return horses
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

    def get_race_info(self, race_id):
        with self.session() as session:
            try:
                race_info = session.query(RaceInfo).filter_by(id=race_id).first()
                return race_info
            except Exception as ex:
                self.logger.error(f"{ex}")
        return None
