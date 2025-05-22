# -*- coding: utf-8 -*-

#
# Copyright (c) MINETA "m10i" Hiroki <h-mineta@0nyx.net>
# This software is released under the MIT License.
#

class SkylarkUtil:
    ticket_type_list = (
        "単勝",
        "複勝",
        "枠連",
        "馬連",
        "ワイド",
        "馬単",
        "三連複",
        "三連単"
    )

    class_list = (
        "オープン",
        "1600万下",
        "1000万下",
        "500万下",
        "未勝利",
        "新馬"
    )

    @staticmethod
    def convertToTicketType2Int(strings):
        return SkylarkUtil.ticket_type_list.index(strings)

    @staticmethod
    def convertToClass2Int(strings):
        count = 0
        for value in SkylarkUtil.class_list:
            if strings.find(value) > 0:
                return count
            count += 1

        return None
