# The MIT License (MIT)
# Copyright (c) 2014-2017 University of Bristol
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
# DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
# OR OTHER DEALINGS IN THE SOFTWARE.
"""
Module containing some SPHERE specific helpers:
    - useful constants
    - predefined tools (with their parameters)
"""
from datetime import timedelta
from hyperstream.itertools2 import online_average, count as online_count
from scipy import integrate
import numpy as np
import sys

second = timedelta(seconds=1)
minute = timedelta(minutes=1)
hour = timedelta(hours=1)

environmental_aggregators = {
    'humidity': online_average,
    'dust': online_average,
    'noise': online_average,
    'temp': online_average,
    'pir': max,
    'coldwater': integrate.quad,
    'hotwater': integrate.quad,
    'electricity_tv': max,
    'electricity_total': max,
}

motion_sensors = {
    "bedroom 1": "motion-S1_B1",
    "study": "motion-S1_B0",
    "bedroom 2": "motion-S1_B2",
    "bathroom": "motion-S1_BR",
    "hallway": "motion-S1_H",
    "kitchen": "motion-S1_K",
    "lounge": "motion-S1_L",
    "stairs": "motion-S1_S",
    "toilet": "motion-S1_WC"
}

annotator_ids = ["2", "3", "5"]

# Mapping from access point to location, and wearable uid to wearable name
mappings = {
    "aid": {
        # New access points and sensor locations
        "b827eb26dbd6": "hallway",
        "b827eb643ba7": "lounge",
        "b827eb0654a9": "study",
        "b827eb48a755": "kitchen",
        "b827eb645a8e": "kitchen",
        "b827eb62fe45": "hallway",
        "b827eb036271": "lounge",
        "b827ebfd0967": "bedroom 1",
        "b827ebcb1413": "bedroom 1",
        "b827eb3f1106": "bedroom 2",
        # Old access points
        "b827ebea7bc4": "lounge",
        "b827eb94cbd1": "kitchen",
        "b827eb524fec": "study"
    },
    'uid': {
        "0aa05543a5c2": "A"
    },
    'annotator': dict((x, x) for x in annotator_ids)
}


def diff(x):
    x = list(x)
    if not x:
        return None
    return np.diff(x) if len(x) > 1 else None

eps = sys.float_info.epsilon


class PredefinedTools(object):
    def __init__(self, channel_manager):

        # ENVIRONMENTAL
        self.environmental = channel_manager.get_tool(
            name="sphere",
            parameters=dict(modality="environmental"))

        self.environmental_relative_apply = channel_manager.get_tool(
            name="relative_apply2",
            parameters=dict(func=lambda kk, vv: environmental_aggregators[kk](vv))
        )

        self.environmental_humidity = channel_manager.get_tool(
            name="component",
            parameters=dict(key="humidity")
        )

        self.environmental_motion = channel_manager.get_tool(
            name="sphere",
            parameters=dict(modality="environmental", elements={"motion"})
        )

        # WEARABLE
        self.wearable = channel_manager.get_tool(
            name="sphere",
            parameters=dict(modality="wearable"))

        self.wearable_rss = channel_manager.get_tool(
            name="sphere",
            parameters=dict(modality="wearable", elements={"rss"}))

        self.wearable_rss_values = channel_manager.get_tool(
            name="component",
            parameters=dict(key="wearable-rss"),
        )

        # ANNOTATIONS
        self.annotations_location = channel_manager.get_tool(
            name="sphere",
            parameters=dict(modality="annotations", annotators=annotator_ids, elements={"Location"},
                            filters={"trigger": 1})
        )

        self.annotations_label = channel_manager.get_tool(
            name="component",
            parameters=dict(key="label")
        )

        # CLOCKS
        self.clock_10s = channel_manager.get_tool(
            name="clock",
            parameters=dict(stride=10 * second)
        )

        self.clock_5m = channel_manager.get_tool(
            name="clock",
            parameters=dict(stride=5 * minute)
        )

        # RELATIVE WINDOWS and APPLY
        self.relative_window_minus10_0 = channel_manager.get_tool(
            name="relative_window",
            parameters=dict(relative_start=-10 - eps, relative_end=0)
        )

        self.relative_apply_diff = channel_manager.get_tool(
            name="relative_apply",
            parameters=dict(func=diff)
        )

        self.relative_apply_mean = channel_manager.get_tool(
            name="relative_apply",
            parameters=dict(func=online_average)
        )

        # SPLITTERS
        self.split_annotator = channel_manager.get_tool(
            name="splitter",
            parameters=dict(element="annotator", mapping=mappings["annotator"])
        )

        self.split_aid = channel_manager.get_tool(
            name="splitter",
            parameters=dict(element="aid", mapping=mappings["aid"])
        )

        self.split_uid = channel_manager.get_tool(
            name="splitter",
            parameters=dict(element="uid", mapping=mappings["uid"])
        )
