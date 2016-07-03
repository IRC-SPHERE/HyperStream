
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

# useful definitions
humid = 'humidity'
dust = 'dust'
noise = 'noise'
temp = 'temperature'
pir = 'pir'
water = 'water'
rssi = 'rssi'
wearable = 'wearable'
hot = 'hot'
cold = 'cold'

def get_house_ids(houses):
  return(0)
#     - function returns the identifiers of the houses specified by the 'houses' parameter

def get_room_ids(house,role,has_sensors):
  return(0)
#     - function returns for the specified house the identifiers of the rooms which match the parameters
#     - 'role' specifies the role of the room, e.g. 'master bedroom' or 'kitchen'
#     - 'has_sensors' specifies which sensors the room must have

def sensor_stream(houses,rooms,types,subtypes):
  return(0)
#     - performs a query to MongoDB to create a combined stream of all sensors which match the parameters
#     - 'types' refers to the type of sensors, e.g. 'water', 'electricity', 'pir'
#     - 'subtypes' refers to the subtype of sensors, e.g. type='water' can have subtypes 'hot' and 'cold'

