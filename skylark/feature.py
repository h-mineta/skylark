# -*- coding: utf-8 -*-

#
# Copyright (c) 2016 h-mineta <h-mineta@0nyx.net>
# This software is released under the MIT License.
#

from . import db
import logging

class SkylarkFeature():
    def __init__(self, args, logger, race_id, horse_number):
        assert race_id > 0 and horse_number > 0

        self.args         = args
        self.logger       = logger
        self.race_id      = race_id
        self.horse_number = horse_number

        with db.SkylarkDb(args = args, logger = logger) as dbi:
            print(dbi.getRaceInfo(race_id))
