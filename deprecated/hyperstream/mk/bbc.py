
"""
The MIT License (MIT)
Copyright (c) 2014-2016 University of Bristol

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
OR OTHER DEALINGS IN THE SOFTWARE.
"""

from hyperstream import * 
from hyperstream_sphere import * 

train_times = time_interval('2016-04-19','2016-04-22')
bbc_times = time_interval('2016-04-29 20:00','2016-04-30 12:00') # times need checking

# get house id:
sphere_house = get_house_ids(houses='sphere')

# get room ids:
all_rooms = get_room_ids(house=sphere_house)
kitchen = get_room_ids(house=sphere_house,role='kitchen')
bathroom = get_room_ids(house=sphere_house,role='bathroom')
env_rooms = get_room_ids(house=sphere_house,has_sensors=[humid,dust,noise,temp,pir])
rssi_rooms = get_room_ids(house=sphere_house,has_sensors=[rssi])

# define sensor streams:
s = {}
for room in env_rooms:
  for sensor in [humid,dust,noise,temp,pir]:
    s[room,sensor] = def_stream(sensor_stream,houses=sphere_house,rooms=room,types=sensor)
for room in [kitchen,bathroom]:
  for hotcold in [hot,cold]:
    s[room,water,hotcold] = def_stream(sensor_stream,houses=sphere_house,rooms=room,types='water',subtypes=hotcold)
for room in rssi_rooms:
  s[room,rssi] = def_stream(sensor_stream,houses=sphere_house,rooms=room,types='rssi')
s[wearable] = def_stream(sensor_stream,houses=sphere_house,types='wearable')

# pre-process the sensor streams:
t = def_temporary_stream(time_stream,stride=delta(seconds=10))
p = {}
for room in env_rooms:
  for sensor in [humid,dust,noise,temp]:
    p[room,sensor] = def_stream(pool,data=use_stream(s[room,sensor],start=-5,end=5),times=use_stream(t),operator='mean')
  p[room,pir] = def_stream(pool,data=use_stream(s[room,pir],start=-5,end=5),times=use_stream(t),operator='max')
for room in [kitchen,bathroom]:
  for hotcold in [hot,cold]:
    p[room,water,hotcold] = def_stream(pool,data=use_stream(s[room,water,hotcold],start=-5,end=5),times=use_stream(t),operator='integrate_per_sec',default=0)
for room in rssi_rooms:
  p[room,rssi] = def_stream(pool,data=use_stream(s[room,rssi],start=-5,end=5),times=use_stream(t),operator='mean',default=-120)
p[wearable] = def_stream(apply_van_hees,data=use_stream(s[wearable]),times=use_stream(t))

# define sensor stream pre-processing workflow:
#TO DO

#pred_loc = apply_loc_model(model=loc_model,rssi=rssi_10s[RSSI_sensors])

#TO BE CONTINUED

#TO DO:
#	- think about whether to try to reuse results between two streams that are both created with the same 'pool' calls except for a different but overlapping 'times' stream

