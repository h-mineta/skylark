# -*- coding: utf-8 -*-

#
# Copyright (c) 2024 MINETA "m10i" Hiroki <h-mineta@0nyx.net>
# This software is released under the MIT License.
#

from skylark.crud import SkylarkCrud


class SkylarkFeature():
    def __init__(self, args, logger):
        self.args         = args
        self.logger       = logger

    def __enter__(self):
        return self

    def initialize(self, db_crud: SkylarkCrud, race_id, horse_number):
        assert race_id > 0 and horse_number > 0

        race_info = db_crud.get_race_info(race_id)
        if (race_info is None):
            return

        order_of_finish = db_crud.get_order_of_finish(race_id, horse_number)

        horse_id = db_crud.get_horse_id(race_id, horse_number)

        jockey_id = db_crud.get_jockey_id(race_id, horse_number)

        trainer_id = db_crud.get_trainer_id(race_id, horse_number)

        owner_id = db_crud.get_owner_id(race_id, horse_number)

        speed_figure_last = db_crud.get_speed_figure_last(horse_id, race_info.date)

        speed_figure_avg = db_crud.get_speed_figure_avg(horse_id, race_info.date, 5)

        winner_avg = db_crud.get_winner_avg(horse_id, race_info.date, 5)

        disavesr = db_crud.get_disavesr(horse_id, race_info.date, race_info.distance, 100)

        distance_avg = db_crud.get_distance_avg(horse_id, race_info.date, 100)

        #if distance_avg is not None:
        #    print((race_info.distance - distance_avg) / distance_avg)

        earnings_per_share = db_crud.get_earnings_per_share(horse_id, race_info.date, 100)
