# -*- coding: utf-8 -*-

#
# Copyright (c) MINETA "m10i" Hiroki <h-mineta@0nyx.net>
# This software is released under the MIT License.
#

import json
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
        if race_info is None:
            return

        race_result = db_crud.get_race_result(race_id, horse_number)
        if race_result is None:
            return

        order_of_finish = db_crud.get_order_of_finish(race_id, horse_number)

        horse_id = db_crud.get_horse_id(race_id, horse_number)

        if horse_id is None:
            return

        jockey_id = db_crud.get_jockey_id(race_id, horse_number)

        trainer_id = db_crud.get_trainer_id(race_id, horse_number)

        #owner_id = db_crud.get_owner_id(race_id, horse_number)

        # 安全に取得
        date = getattr(race_info, "date", None)
        distance = getattr(race_info, "distance", None)
        if not isinstance(distance, int):
            distance = None

        speed_figure_last = db_crud.get_speed_figure_last(horse_id, date)

        speed_figure_avg = db_crud.get_speed_figure_avg(horse_id, date, 5)

        winner_avg = db_crud.get_winner_avg(horse_id, date, 5)

        disavesr = None
        if distance is not None:
            disavesr = db_crud.get_disavesr(horse_id, date, distance, 100)

        distance_avg = db_crud.get_distance_avg(horse_id, date, 100)

        #if distance_avg is not None:
        #    print((race_info.distance - distance_avg) / distance_avg)

        earnings_per_share = db_crud.get_earnings_per_share(horse_id, date, 100)

        calculation_result = {
            "sppeed_figure_last": speed_figure_last,
            "speed_figure_avg": speed_figure_avg,
            "winner_avg": winner_avg,
            "disavesr": disavesr.as_integer_ratio() if disavesr is not None else None,
            "distance_avg": distance_avg.as_integer_ratio() if distance_avg is not None else None,
            "earnings_per_share": earnings_per_share
        }

        db_crud.insert_features([
            {
                "horse_id": horse_id,
                "race_id": race_id,
                "jockey_id": jockey_id,
                "trainer_id": trainer_id,
                "calculation_result_json": json.dumps(calculation_result, ensure_ascii=False, sort_keys=True),
            }
        ])
