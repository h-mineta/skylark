# -*- coding: utf-8 -*-

#
# Copyright (c) MINETA "m10i" Hiroki <h-mineta@0nyx.net>
# This software is released under the MIT License.
#

from sqlalchemy import (
    Column, Integer, BigInteger, String, Float, Text, Time, Date,
    ForeignKey, Index
)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

# ORM モデル定義
class RaceInfo(Base):
    __tablename__ = 'race_info_tbl'
    id = Column(BigInteger, primary_key=True, autoincrement=False)
    race_name = Column(String(256), nullable=False)
    distance = Column(Integer, nullable=False)
    weather = Column(String(8), nullable=False)
    post_time = Column(Time, nullable=False)
    race_number = Column(BigInteger, nullable=False)
    run_direction = Column(String(8))
    track_surface = Column(String(8), nullable=False)
    track_condition = Column(String(8), nullable=False)
    track_condition_score = Column(Integer)
    date = Column(Date, nullable=False)
    place_detail = Column(String(16), nullable=False)
    race_grade = Column(Integer)
    race_class = Column(String(64), nullable=False)

    # インデックス
    __table_args__ = (
        Index('idx_race_name', 'race_name'),
        Index('idx_date', 'date'),
    )

class Horse(Base):
    __tablename__ = 'horse_tbl'
    horse_id = Column(BigInteger, primary_key=True, autoincrement=False)
    horse_name = Column(String(256), nullable=False)

class Jockey(Base):
    __tablename__ = 'jockey_tbl'
    jockey_id = Column(BigInteger, primary_key=True, autoincrement=False)
    jockey_name = Column(String(256), nullable=False)

class Trainer(Base):
    __tablename__ = 'trainer_tbl'
    trainer_id = Column(BigInteger, primary_key=True, autoincrement=False)
    trainer_name = Column(String(256), nullable=False)

class Owner(Base):
    __tablename__ = 'owner_tbl'
    owner_id = Column(String(32), primary_key=True)
    owner_name = Column(String(256), nullable=False)

class RaceResult(Base):
    __tablename__ = 'race_result_tbl'
    race_id = Column(BigInteger, ForeignKey('race_info_tbl.id'), primary_key=True)
    horse_number = Column(Integer, primary_key=True)
    order_of_finish = Column(Integer)
    bracket_number = Column(Integer, nullable=False)
    horse_id = Column(BigInteger, ForeignKey('horse_tbl.horse_id'), nullable=False)
    sex = Column(String(8), nullable=False)
    age = Column(Integer, nullable=False)
    basis_weight = Column(Float, nullable=False)
    jockey_id = Column(BigInteger, ForeignKey('jockey_tbl.jockey_id'), nullable=False)
    finishing_time = Column(Time)
    margin = Column(String(16), nullable=False)
    speed_figure = Column(Integer)
    passing_rank = Column(String(16), nullable=False)
    last_phase = Column(Float)
    odds = Column(Float)
    popularity = Column(Integer)
    horse_weight = Column(Integer)
    horse_weight_diff = Column(Integer)
    remark = Column(Text)
    stable = Column(String(8), nullable=False)
    trainer_id = Column(BigInteger, ForeignKey('trainer_tbl.trainer_id'), nullable=False)
    owner_id = Column(String(32), ForeignKey('owner_tbl.owner_id'), nullable=False)
    earning_money = Column(Float)

    # インデックス
    __table_args__ = (
        Index('idx_race_horse', 'race_id', 'horse_id'),
        Index('idx_race_jockey', 'race_id', 'jockey_id'),
        Index('idx_race_trainer', 'race_id', 'trainer_id'),
        Index('idx_race_owner', 'race_id', 'owner_id'),
    )

class Payoff(Base):
    __tablename__ = 'payoff_tbl'
    race_id = Column(BigInteger, ForeignKey('race_info_tbl.id'), primary_key=True)
    ticket_type = Column(Integer, primary_key=True)
    horse_numbers = Column(String(16), primary_key=True)
    payoff = Column(Integer, nullable=False)
    popularity = Column(Integer, nullable=False)

class Feature(Base):
    __tablename__ = 'feature_tbl'
    race_id = Column(BigInteger, ForeignKey('race_info_tbl.id'), primary_key=True)
    horse_number = Column(Integer, primary_key=True)
    grade = Column(Integer, nullable=False)
    order_of_finish = Column(Integer)
    horse_id = Column(BigInteger, ForeignKey('horse_tbl.horse_id'), nullable=False)
    jockey_id = Column(BigInteger, ForeignKey('jockey_tbl.jockey_id'), nullable=False)
    trainer_id = Column(BigInteger, ForeignKey('trainer_tbl.trainer_id'), nullable=False)
    sex = Column(String(8), nullable=False)
    age = Column(Integer, nullable=False)
    basis_weight = Column(Float)
    weather = Column(String(8), nullable=False)
    track_surface = Column(String(8), nullable=False)
    track_condition = Column(String(8), nullable=False)
    track_condition_score = Column(Float)

    # インデックス
    __table_args__ = (
        Index('idx_feature_race_horse', 'race_id', 'horse_number'),
    )
