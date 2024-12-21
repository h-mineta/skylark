# -*- coding: utf-8 -*-

#
# Copyright (c) 2016 h-mineta <h-mineta@0nyx.net>
# This software is released under the MIT License.
#

from . import crud

class SkylarkFeature():
    def __init__(self, args, logger):
        self.args         = args
        self.logger       = logger

    def __enter__(self):
        return self

    def initialize(self, race_id, horse_number):
        assert race_id > 0 and horse_number > 0

        with crud.SkylarkCrud(args = self.args, logger = self.logger) as dbi:
            race_info = dbi.getRaceInfo(race_id)
            if (race_info is None):
                return

            order_of_finish = dbi.getOrderOfFinish(race_id, horse_number)

            horse_id = dbi.getHorseId(race_id, horse_number)

            jockey_id = dbi.getJockeyId(race_id, horse_number)

            trainer_id = dbi.getTrainerId(race_id, horse_number)

            owner_id = dbi.getOwnerId(race_id, horse_number)

            speed_figure_last = dbi.getSpeedFigureLast(horse_id, race_info['date'])

            speed_figure_avg = dbi.getSpeedFigureAvg(horse_id, race_info['date'], 5)

            #winner_avg = dbi.getWinnerAvg(horse_id, race_info['date'], 5)
            winner_avg = dbi.getWinnerAvg_Free(horse_id, race_info['date'], 5)

            disavesr = dbi.getDisavesr(horse_id, race_info['date'], race_info['distance'], 100)

            distance_avg = dbi.getDistanceAvg(horse_id, race_info['date'], 100)
            #if distance_avg is not None:
            #    print((race_info['distance'] - distance_avg) / distance_avg)

            earnings_per_share = dbi.getEarningsPerShare(horse_id, race_info['date'], 100)
