
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

##########################################################
### FUNCTIONS TO DEFINE STREAMS AND THEIR DEPENDENCIES ###
##########################################################

from datetime import datetime as date
from datetime import timedelta as delta

epsilon = delta(microseconds=1)

#print(TimeIntervals([(date(2010,1,1),date(2015,3,1))])-TimeIntervals([(date(2011,4,1),date(2013,3,1))]))
#(2010-01-01 00:00:00,2011-04-01 00:00:00]U(2013-03-01 00:00:00,2015-03-01 00:00:00]
class TimeIntervals: # example object: (t1,t2]U(t3,t4]U...
  def __init__(self,value=[]):
    self.value=value
  def __str__(self):
    return("U".join(["("+str(a)+","+str(b)+"]" for (a,b) in self.value]))
  def split(self,points):
    if len(points)==0:
      return
    p = points[-1]
    for i in range(len(self.value)):
      if (self.value[i][0]<p) and (self.value[i][1]>p):
        self.value = self.value[:i]+[(self.value[i][0],p),(p,self.value[i][1])]+self.value[(i+1):]
    self.split(points[:-1])
  def compress(self):
    if len(self.value)==0:
      return
    v = self.value[:1]
    for i in range(1,len(self.value)):
      if self.value[i][0]==v[-1][1]:
        v[-1] = (v[-1][0],self.value[i][1])
      else:
        v.append(self.value[i])
    self.value = v
  def __add__(self,other):
    self_points = [point for interval in self.value for point in interval]
    other_points = [point for interval in other.value for point in interval]
    self.split(other_points)
    other.split(self_points)
    v = list(set(self.value).union(set(other.value)))
    v.sort()
    new = TimeIntervals(v)
    self.compress()
    other.compress()
    new.compress()
    return(new)
  def __sub__(self,other):
    self_points = [point for interval in self.value for point in interval]
    other_points = [point for interval in other.value for point in interval]
    self.split(other_points)
    other.split(self_points)
    v = list(set(self.value).difference(set(other.value)))
    v.sort()
    new = TimeIntervals(v)
    self.compress()
    other.compress()
    new.compress()
    return(new)

class StreamTool: 
  # inputs a mixed list of parameters and StreamRef and lists of StreamRef
  # outputs a stream
  def __init__(self):
    pass
  def run(self,start,end,**kwargs):
    raise NotImplementedError

class DocumentTool:
  # inputs a mixed list of parameters and StreamRef and lists of StreamRef
  # outputs a single document at a time, the times are specified by a timing stream
  def __init__(self):
    pass
  def run_single(self,timestamp,**kwargs):
    raise NotImplementedError
  def run(self,start,end,timer=None,**kwargs):
    if timer.__class__!=StreamRef:
      raise RuntimeError
    documents = []
    for time_doc in timer.get_documents(start,end):
      timestamp = time_doc.timestamp
      kwargs2 = {}
      for kw in kwargs:
	arg = kwargs[kw]
	if arg.__class__==StreamRef:
	  kwargs2[kw] = arg.get_data(timestamp-epsilon,timestamp)
	elif arg.__class__==list:
	  arg_list = []
	  for arg_i in arg:
	    if arg_i.__class__==StreamRef:
	      arg_list.append(arg_i.get_data(timestamp-epsilon,timestamp))
	    else:
	      arg_list.append(arg_i)
	  kwargs2[kw] = arg_list
	else:
	  kwargs2[kw] = arg
      documents.extend(self.run_single(timestamp,kwargs))
    return(documents)
    

class Clock(StreamTool):
#  def run(self,start,end,**kwargs):
  def run(self,start,end,first=date(1970,1,1),stride=delta(seconds=1)):
#    first = kwargs['first']
#    stride = kwargs['stride']
    n_strides = int( (start-first).total_seconds() // stride.total_seconds() )
    t = first + n_strides*stride
    documents = []
    while t<=end:
      if t>start:
	documents.append(Document(t,t))
      t = t+stride
    return(documents)
#    time_stream(stride=delta(seconds=1),first=date(1970,1,1)):

class Constant(Tool):
  def run(self,start,end,times=None,value=None):
    documents = []
    for tdoc in times:
      documents.append(Document(tdoc.timestamp,value))
    return(documents)

class Document:
  def __init__(self,timestamp,data):
    self.timestamp = timestamp
    self.data = data

class Stream:
  def __init__(self,tool,temporary,**kwargs):
    self.tool = tool
    self.temporary = temporary
    self.kwargs = kwargs
    self.times = TimeIntervals()
    self.documents = []
  def calc(self,start,end):
    times = TimeIntervals([(start,end)])-self.times
    for (s,e) in times.value:
      kwargs = {}
      for kw in self.kwargs:
	arg = self.kwargs[kw]
	if arg.__class__==StreamRef:
	  kwargs[kw] = arg.get_data(s,e)
	elif arg.__class__==list:
	  arg_list = []
	  for arg_i in arg:
	    if arg_i.__class__==StreamRef:
	      arg_list.append(arg_i.get_data(s,e))
	    else:
	      arg_list.append(arg_i)
	  kwargs[kw] = arg_list
	else:
	  kwargs[kw] = arg
      print('Running the tool '+self.tool.__class__.__name__+' from '+str(s)+' to '+str(e))
      documents = self.tool.run(s,e,**kwargs)
      self.documents.extend(documents)
    self.times = self.times+TimeIntervals([(start,end)])
  def get_documents(self,start,end):
    self.calc(start,end)
    documents = [doc for doc in self.documents if (doc.timestamp>start) and (doc.timestamp<=end)]
    documents.sort(key=lambda doc:doc.timestamp)
    return(documents)
  def get_data(self,start,end):
    return([doc.data for doc in self.get_documents(start,end)])
    
    
class StreamRef:
  def __init__(self,stream,start=date(1970,1,1),end=delta(0),sfilter=None,tail=None,use_only_data=False):
    self.stream = stream
    self.start = start
    self.end = end
    self.tail = tail
    self.sfilter = sfilter
    self.use_only_data = use_only_data
  def get_data(self,start,end):
    s = self.start
    if s.__class__==delta:
      s = start+s
    e = self.end
    if e.__class__==delta:
      e = end+e
    if self.use_only_data:
      data = self.stream.get_data(s,e)      
    else:
      data = self.stream.get_documents(s,e)      
    # sfilter to be implemented here
    if self.tail==None:
      return(data)
    else:
      return(data[-self.tail:])
    
class StreamBase:
  def __init__(self):
    pass

#s = Stream(Clock(),False,stride=delta(minutes=2),first=date(1970,1,1))
#s = Stream(Clock(),False,stride=delta(minutes=2))
s = Stream(Clock(),False)
d = s.get_data(date(2000,3,1,12),date(2000,3,1,13))
d2 = s.get_data(date(2000,3,1,12,29),date(2000,3,1,13,15))
s = Stream(Clock(),False,stride=delta(minutes=2))
d = s.get_data(date(2000,3,1,12),date(2000,3,1,13))
d2 = s.get_data(date(2000,3,1,12,29),date(2000,3,1,13,15))
s2 = Stream(Constant(),False,times=StreamRef(s,-epsilon,delta(0)),value=42)
d3 = s2.get_data(date(2000,3,1,12,29),date(2000,3,1,13,15))
d4 = s2.get_documents(date(2000,3,1,12,29),date(2000,3,1,13,15))

def def_stream(stream_func,**kwargs):
  return(0)
#     - this defines a new stream, which can be calculated with the 'stream_func'
#     - the remaining parameters must be named and will be passed on to 'stream_func' during the calculation
#     - if some existing stream has an identical definition, then an integer representing the identifier of that stream is returned
#     - otherwise, a new stream is created and the identifier to that stream is returned
#     - this function does not calculate the stream, it only defines the stream

def def_temporary_stream(stream_func,**kwargs):
  return(0)
#     - the same as def_stream, except that the data in the defined stream are never stored in the MongoDB
#     - instead, the data are always recalculated (or kept in memory if this possibility is implemented)

def use_stream(stream_id,start=date(1970,1,1),end=delta(0),tail=None,sfilter=None):
  return(0)
#     - this function should be called when defining new streams that depend on the existing stream with the identifier 'stream_id'
#     - e.g., def_stream(stream_func,input_stream=use_stream(stream,...))
#     - it returns a data structure defining which stream and which documents of that stream are going to be used
#     - by default, the whole stream up to the current moment is going to be used
#     - i.e., the whole stream up to now must be calculated in order to calculate the new stream
#     - documents outside of the range from 'start' to 'end' are not going to be used
#     - 'start' and 'end' can be defined either with 'date' (datetime.datetime) or 'delta' (datetime.timedelta)
#    - if defined by 'delta' then times are taken relative to the time in the new stream that uses this stream
#     - e.g., start=delta(seconds=-5) and end=delta(seconds=5) mean that 5 seconds from the past and 5 seconds from the future will be used
#     - i.e., the new stream can be calculated only with a delay of 5 seconds
#     - if 'sfilter' is not 'None' then it must define a stream filter allowing to use a subselection of stream (format to be determined, e.g. MongoDB query format)
#     - if 'tail' is not 'None' then it must be a non-negative integer specifying the number of last documents used in the time interval from 'start' to 'end' after applying 'sfilter'

#################################################
### FUNCTIONS TO CREATE AND CALCULATE STREAMS ###
#################################################

# How to call these things?
# worker (Taverna old)
# service (Taverna new)
# component (Kepler general)
# actor (Kepler specific)
# director (Kepler specific)
# tool (Galaxy)
# processor (?)

def time_stream(stride=delta(seconds=1),first=date(1970,1,1)):
  return(0)
#     - returns a stream with documents at regular intervals
#     - each document contains information about the current time (datetime.datetime)
#     - the first document is at the datetime 'first'
#     - every next one is after 'stride' timedelta
#     - 'stride' must be positive, defined with 'delta' (i.e. datetime.timedelta)
#     - allowed parameters to 'delta' are 'days','weeks','hours','minutes','seconds','milliseconds','microseconds'
#     - the problems of leap seconds (23:59:60, see https://en.wikipedia.org/wiki/Leap_second) and daylight saving time are not dealt with, i.e. treated the same way as the datetime module does it

def merge(stream_list):
  return(0)
#     - returns a stream that is merged together from the streams in 'stream_list'
#     - the ordering is first by timestamp and second by the order within the 'stream_list'

def pool(data,times,operator,default):
  return(0)
#     - returns a stream with documents at exactly the same times as the 'times' stream
#     - each document is calculated by pooling together the documents extracted from the 'data' stream according to the use_stream parameters
#     - 'data' and 'times' must be streams represented as records returned by the use_stream function
#     - pooling is done by calling the function corresponding to the value of 'operator'
#     - allowed values are 'min', 'max', 'sum', 'mean', 'median','integrate_per_sec',...

