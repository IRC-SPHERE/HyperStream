
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
from hyperstream_bases import *
from hyperstream_modifiers import *
from hyperstream_intervals import *

import logging
import sys
sys.path.append("../..")
sys.path.append("..")
#from sphere_connector_package.sphere_connector.utils import get_value_safe
#from sphere_connector_package import sphere_connector as sc
from sphere_connector_package.sphere_connector.config import BasicConfig, ExperimentConfig
from sphere_connector_package.sphere_connector.experiment import Experiment
from client import Client
from mongoengine import connect
try:
    from pymongo.errors import ServerSelectionTimeoutError
except ImportError:
    ServerSelectionTimeoutError = None

def double_connect(basic_config):
    """
    Connects to mongo twice - once using standard pymongo connection, and another using mongoengine. Currently can't
    be peformed in one step, see here: https://github.com/MongoEngine/mongoengine/issues/704
    :param basic_config: Basic configuration
    :return:
    """
    client = Client(basic_config.mongo)
    d = dict((k, v) for k, v in basic_config.mongo.items() if k not in ['modalities', 'summaries'])
    if 'authentication_database' in d:
        d['authentication_source'] = d['authentication_database']
        del d['authentication_database']
    session = connect(**d)
    return [client, session]

def try_connect(basic_config):
    if ServerSelectionTimeoutError:
        try:
            return double_connect(basic_config)
        except ServerSelectionTimeoutError as e:
            logging.warn(e.message)
            sys.exit()
    else:
        return double_connect(basic_config)


class SphereBase(StreamBase):
  '''SPHERE MongoDB storing the raw sensor data'''
  def __init__(self,base_id,up_to_timestamp=MIN_DATE):
    state = BaseState(base_id)
    super(SphereBase,self).__init__(can_calc=False,can_create=False,state=state)
    self.modalities = ('video','environmental')
    for stream_id in self.modalities:
      self.state.set_name2id(stream_id,stream_id)
      self.state.set_id2calc(stream_id,TimeIntervals([(MIN_DATE,up_to_timestamp)]))
    self.up_to_timestamp = MIN_DATE
    if up_to_timestamp>MIN_DATE:
      self.update(up_to_timestamp)
    self.basic_config = BasicConfig(True, False)
    [self.sphere_client, self.session] = try_connect(self.basic_config)
  def repr_stream(self,stream_id):
    return('read-only SPHERE MongoDB stream')
  def __setitem__(self,key,value):
    raise Exception('SphereBase is read-only, cannot create new streams')
  def update(self,up_to_timestamp):
    '''Call this function to report to the system that the SPHERE MongoDB is fully populated until up_to_timestamp'''
    for stream_id in self.modalities:
      self.state.set_id2calc(stream_id,TimeIntervals([(MIN_DATE,up_to_timestamp)]))
    self.up_to_timestamp = up_to_timestamp
  def get_results(self,stream_ref,args,kwargs):
    stream_id = stream_ref.stream_id
    start = stream_ref.start
    abs_start = start
    if type(start)==delta:
      try:
        abs_start = kwargs['start'] + start
      except KeyError:
	raise Exception('The stream reference to a SphereBase stream has a relative start time, need an absolute start time')
    end = stream_ref.end
    abs_end = end
    if type(end)==delta:
      try:
        abs_end = kwargs['end'] + end
      except KeyError:
	raise Exception('The stream reference to a SphereBase stream has a relative end time, need an absolute end time')
    if abs_end>self.up_to_timestamp:
      raise Exception('The stream is not available after '+str(self.up_to_timestamp)+' and cannot be obtained')
    experiment_config = ExperimentConfig(experiment_start=abs_start,experiment_end=abs_end,experiment_id=42)
    experiment = Experiment(self.sphere_client,experiment_config,self.basic_config)
    if stream_id not in self.modalities:
      raise Exception('Unknown stream_id: '+str(stream_id))
    if stream_id=='video':
      data = experiment.video.get_data(elements={"2Dbb"})
    elif stream_id=='environmental':
      data = experiment.environmental.get_data()
    else:
      assert(False)
    def reformat(doc):
      timestamp = doc['datetime']
      del doc['datetime']
      return( (timestamp,doc) )
    # assume that the data are already sorted by time
    result = stream_ref.modifier( (reformat(doc) for doc in data) ) # make a generator out from result and then apply the modifier
    return(result)
  def get_default_ref(self):
    return({'start':MIN_DATE,'end':self.up_to_timestamp,'modifier':Identity()})


class AttrDict(dict):
  def __getattr__(self,key):
    return(self.__getitem__(key))
  def __setattr__(self,key,value):
    self.__setitem__(key,value)




# experiment_config = ExperimentConfig(experiment_start=date(2016,4,28,20),experiment_end=date(2016,4,29,13),experiment_id=1234)
# experiment = Experiment(sphere_client, experiment_config, basic_config)
# data = experiment.environmental.get_data()
# data = experiment.video.get_data(elements={"2Dbb"})
# data = experiment.wearable.get_data()
# data = experiment.annotations.get_data()
# data = experiment.wearable.get_data(elements={"rss"})



### # useful definitions
### humid = 'humidity'
### dust = 'dust'
### noise = 'noise'
### temp = 'temperature'
### pir = 'pir'
### water = 'water'
### rssi = 'rssi'
### wearable = 'wearable'
### hot = 'hot'
### cold = 'cold'
### 
### def get_house_ids(houses):
###   return(0)
### #     - function returns the identifiers of the houses specified by the 'houses' parameter
### 
### def get_room_ids(house,role,has_sensors):
###   return(0)
### #     - function returns for the specified house the identifiers of the rooms which match the parameters
### #     - 'role' specifies the role of the room, e.g. 'master bedroom' or 'kitchen'
### #     - 'has_sensors' specifies which sensors the room must have
### 
### def sensor_stream(houses,rooms,types,subtypes):
###   return(0)
### #     - performs a query to MongoDB to create a combined stream of all sensors which match the parameters
### #     - 'types' refers to the type of sensors, e.g. 'water', 'electricity', 'pir'
### #     - 'subtypes' refers to the subtype of sensors, e.g. type='water' can have subtypes 'hot' and 'cold'

