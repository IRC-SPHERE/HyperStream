
x = {}
for i in range(3):
  x[i] = lambda:i
for j in range(3):
  print(x[j]())
for i in range(3):
  print(x[i]())
print(x[0]())
print(x[1]())
print(x[2]())

x = {}
for i in range(3):
  def f(i=i):
    return(i)
  x[i] = f
#  x[i] = lambda:i
for j in range(3):
  print(x[j]())
for i in range(3):
  print(x[i]())
print(x[0]())
print(x[1]())
print(x[2]())





class TempTools(object):
#    tool(start,end,writer,*args,**kwargs)
  def clock(self,start,end,writer,first=MIN_DATE,stride=sec):
    if start<first:
      start = first
    n_strides = int( (start-first).total_seconds() // stride.total_seconds() )
    t = first + n_strides*stride
    while t<=end:
      if t>start:
	yield (t,t)
      t = t+stride

class TempToolStreamBase(StreamBase):
  def __init__(self,base_id):
    state = BaseState(base_id)
    s = super(TempToolStreamBase,self)
#    super(TempToolStreamBase,self).__init__()
    super(TempToolStreamBase,self).__init__(can_calc=False,can_create=False,state=state)
    self.tools = TempTools()
    for t in dir(self.tools):
      if t[0]!='_':
	state.set_name2id(t,t)
	state.set_id2calc(t,TimeIntervals([(MIN_DATE,MAX_DATE)]))
  def get_stream_reader(self,stream_id):
    def reader(start,end):
      yield (MIN_DATE,getattr(self.tools,stream_id))
    return(reader)

#TT = TempToolStreamBase(1)

class HyperSystemBase:
  def __init__(self):
    self._load_streambases()
  def _load_streambases(self):
    self.streambases = do_load_streambases()


class HyperSystem:
  def __init__(self):
    self.last_script_time = None
  def _new_scripts(self):
    scr = find_new_scripts(self.last_script_time) ### TODO
    for s in scr:
      yield s
    return
  def _load_and_run(self,script):
    module = do_import(script) ### TODO
    module.run(hyper_system=self) ### TODO
  def run(self):
    while True:
      for script in self._new_scripts():
	try:
	  self.check_that_time_order_is_ok() ### TODO perhaps newer file copied later
	  self._load_and_run(script)
	except Exception as e:
	  print(e)







    print(dirpath,filenames)
    # todo implement subfolders x.y instead of x_y
    # implement checks to forbid use of x if x.y is used

        f.extend(filenames)
	    break
#  x = __import__('codebase.tools.test.t2016_07_09_17_18_10_859_v1')
  x = __import__('codebase.tools.test.x3')
  x = x.tools.__dict__['test'].__dict__['x3'].__dict__['Test']
#  x = x.tools.__dict__['test'].__dict__['t2016_07_09_17_18_10_859_v1'].__dict__['Test']
#  x = x.tools.__dict__['test'].__dict__['t2016_07_09_17_18_10_859_v1'].__dict__['test_v1_func2']
  globals()['test_v1'] = x
#  globals()['test_v1'] = __import__('codebase.tools.test.t2016_07_09_17_18_10_859_v1')
  globals()['test_v2'] = __import__('codebase.tools.test2.t2016_07_09_17_21_27_811_v2')
#  globals()['test_v1'] = __import__('codebase.tools.test.2016_07_09_17_03_27_v1')

def f():
  return(3,5)

adict = { 'x' : 'I am x', 'y' : ' I am y' }
locals().update(adict)



from json import JSONEncoder
class MyEncoder(JSONEncoder):
  def default(self,o):
    return o.__dict__

class HyperStreamBase:
  """An abstract class from which all stream databases derive

  Must have its own stream definition stream
  """
  def __init__(self):
    pass
  def create_stream(self,

class HyperCodeStreamBase:
  def __init__(self,path):
    self.path = path
  def __setitem__(self,key,value):
    print('setting '+str(key)+' to '+str(value))
  def __getitem__(self,key):
    print('getting '+str(key))

C = HyperCodeStreamBase('path')

class HyperMemoryStreamBase:
  def __init__(self):
    self.path = path
  def __setitem__(self,key,value):
    print('setting '+str(key)+' to '+str(value))
  def __getitem__(self,key):
    print('getting '+str(key))




class Tool: 
  # inputs a mixed list of parameters and StreamRef and lists of StreamRef
  # outputs a stream
  def __init__(self):
    pass
  def calc(self,start,end,**kwargs):
    raise NotImplementedError

class TimedTool(Tool):
  # inputs a mixed list of parameters and StreamRef and lists of StreamRef
  # outputs a single document at a time, the times are specified by a timing stream
  def __init__(self):
    pass
  def calc_document(self,timestamp,**kwargs):
    raise NotImplementedError
  def calc(self,start,end,timer=None,**kwargs):
    if timer.__class__!=StreamRef:
      raise RuntimeError
    documents = []
    for time_doc in timer.get_documents(start,end):
      timestamp = time_doc.timestamp
      kwargs2 = {}
      for kw in kwargs:
	arg = kwargs[kw]
	if arg.__class__==StreamRef:
	  kwargs2[kw] = arg.get_documents(timestamp-epsilon,timestamp)
	elif arg.__class__==list:
	  arg_list = []
	  for arg_i in arg:
	    if arg_i.__class__==StreamRef:
	      arg_list.append(arg_i.get_documents(timestamp-epsilon,timestamp))
	    else:
	      arg_list.append(arg_i)
	  kwargs2[kw] = arg_list
	else:
	  kwargs2[kw] = arg
      documents.append(Document(timestamp,self.calc_document(timestamp,**kwargs2)))
    return(documents)
    

class Clock(Tool):
#  def calc(self,start,end,**kwargs):
  def calc(self,start,end,first=date(1970,1,1),stride=delta(seconds=1)):
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
  def calc(self,start,end,times=None,value=None):
    documents = []
    for tdoc in times.get_documents(start,end):
      documents.append(Document(tdoc.timestamp,value))
    return(documents)

class Constant2(TimedTool):
  def calc_document(self,timestamp,value=None):
    return(value)

class Pool(TimedTool):
  def calc_document(self,timestamp,data=None,document_func=None,data_func=None):
    print(timestamp)
    print(data)
    if document_func!=None:
      assert data_func==None
      return(document_func(data))
    if data_func!=None:
      assert document_func==None
      return(data_func([doc.data for doc in data]))
    assert False
#     - returns a stream with documents at exactly the same times as the 'times' stream
#     - each document is calculated by pooling together the documents extracted from the 'data' stream according to the use_stream parameters
#     - 'data' and 'times' must be streams represented as records returned by the use_stream function
#     - pooling is done by calling the function corresponding to the value of 'operator'
#     - allowed values are 'min', 'max', 'sum', 'mean', 'median','integrate_per_sec',...

class Merge(Tool):
  def calc(self,start,end,streams=[]):
    documents = []
    for s in streams:
      pass
    return(documents)

class Document:
  def __init__(self,timestamp,data):
    self.timestamp = timestamp
    self.data = data
  def __str__(self):
    return('TIME: '+str(self.timestamp)+' DATA: '+str(self.data))
  def __repr__(self):
    return(str(self))

class Stream:
  def __init__(self,tool,stored,**kwargs):
    """Defines a new stream, no calculations made at this moment.

    Positional arguments:
    tool -- the tool that is to be used in calculating this stream
    stored -- whether or not the stream is stored in the database: if false then the stream is only calculated on-the-fly and not stored to the MongoDB

    Keword arguments:
    All arguments must be either StreamRef, list of StreamRef, or "standard" datatypes (allowed set of types is to be determined based on what we want to allow to be stored in MongoDB)
    These parameters will be passed on to the tool during calculations
    """
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
	kwargs[kw] = arg # temporary
#	if arg.__class__==StreamRef:
#	  kwargs[kw] = arg.get_data(s,e)
#	elif arg.__class__==list:
#	  arg_list = []
#	  for arg_i in arg:
#	    if arg_i.__class__==StreamRef:
#	      arg_list.append(arg_i.get_data(s,e))
#	    else:
#	      arg_list.append(arg_i)
#	  kwargs[kw] = arg_list
#	else:
#	  kwargs[kw] = arg
      print('Running the tool '+self.tool.__class__.__name__+' from '+str(s)+' to '+str(e))
      documents = self.tool.calc(s,e,**kwargs)
      self.documents.extend(documents)
    self.times = self.times+TimeIntervals([(start,end)])
    return
  def get_documents(self,start,end):
    self.calc(start,end)
    documents = [doc for doc in self.documents if (doc.timestamp>start) and (doc.timestamp<=end)]
    documents.sort(key=lambda doc:doc.timestamp)
    return(documents)
  def get_data(self,start,end):
    return([doc.data for doc in self.get_documents(start,end)])
    
    
class StreamRef:
  def __init__(self,stream,start=date(1970,1,1),end=delta(0),sfilter=None,tail=None):
    self.stream = stream
    self.start = start
    self.end = end
    self.tail = tail
    self.sfilter = sfilter
  def __get_doc_and_data(self,start,end,only_data=False):
    s = self.start
    if s.__class__==delta:
      s = start+s
    e = self.end
    if e.__class__==delta:
      e = end+e
    if only_data:
      data = self.stream.get_data(s,e)      
    else:
      data = self.stream.get_documents(s,e)      
    # sfilter to be implemented here
    if self.tail==None:
      return(data)
    else:
      return(data[-self.tail:])
  def get_documents(self,start,end):
    return(self.__get_doc_and_data(start,end,only_data=False))
  def get_data(self,start,end):
    return(self.__get_doc_and_data(start,end,only_data=True))
    
class StreamBase:
  def __init__(self):
    streams = {}
  def def_stream(tool,stored,**kwargs):
    tool,stored,

#stream -> hash ???

# workflow as a generalisation of a Stream definition

# problem of one-item streams: overhead in creation and extraction
# everything that we save to mongo should be a stream (?)
# the system should allow other types of input/output than mongo, such as files
# 
# calling a workflow vs calling a tool
# workflow is just a python function, defining the involved streams?
# 
# declaring equivalences of tool versions
# 
# where do we keep python code?
# how do we know which streams come from which code?
# how do we trace calculation progress?
# how to ensure that calculation is performed in maximally big batches?
# make sure it is possible to create sensor streams from a time interval with emptiness outside of this interval - needs to be done with filters
# do we need more complex filters, such as filters that use some other streams?
# is filter just a temporary stream?
# define tool streams, return a runnable object so that one can call tool(...) to define a new stream from this tool

# python code calling recorded?

# separate stream definition code and stream calculation code?

# use cases:
# want to use a model trained by someone else
# a feature is defined on the sphere house, want to deploy it to other houses

# python code is run to define all required streams, returns identifiers that can be used in visualisations

# how to create flexible workflows, that can deal with varying subsets of sensors?

# THINK OF HOW INFER.NET WOULD BE CALLED

# git push as a deployment mechanism

# ids as strings

# make as many ids as possible human-readable

# ToDo:
# - streams in different locations: stream in a folder, sqlite, mongo
# - tool equivalence relationships (code versions and parameters which only change locations, improve speed, etc)
# - isolate the scheduler
# - 
# 1. calc-commands loaded from a particular file
# 2. tools loaded from the code folder
#
# should the stream's identifier encode its location? e.g. f:pool:v1:...
#
# how to ensure that the file streams don't get modified, and files and folder are only added (deleted only if nothing depends on them)
#
# how are the trained models learned, stored and used?
#
# how is it possible to trace the calculation process back from a particular stream?
# store a pointer to the defining python file?
#
# live system: 
# if any changes in the file system
#   then rerun stream definition files which output the stream ids to be followed
# acquire the set of followed stream identifiers 
# update those particular streams
#
# a milestone stream which says when to retrain some model
#
# code stream in a folder:
# - Pool/v1.py

# ----------
# suppose wearables in the house are not documented separately but need to be discovered from the stream
# this violates the separation of stream definitions and calculations
# ----------
#
# png-s of visualisation also built with hyperstreams
#
# callbacks - allows the tool to work more directly with the database (if it's a separate process/thread)
# processes waiting behind each other, if not enough calculated
#
# StreamBase - one in memory

# FileStreamBase - must support moving folders around
# there must be a mapping table from stream_ids to folders

# stream has an id and a pointer to code and a name within that code (or multiple names within that code? - perhaps should be avoided?) or multiple pointers of this kind

# narrow streams within a wide stream

# model training: 
# result is a singleton stream

# python 

###     - if some existing stream has an identical definition, then an integer representing the identifier of that stream is returned
###     - otherwise, a new stream is created and the identifier to that stream is returned

#s = Stream(Clock(),False,stride=delta(minutes=2),first=date(1970,1,1))
#s = Stream(Clock(),False,stride=delta(minutes=2))
s = Stream(Clock(),False)
d = s.get_data(date(2000,3,1,12),date(2000,3,1,13))
d2 = s.get_data(date(2000,3,1,12,29),date(2000,3,1,13,15))



s = Stream(Clock(),False,stride=delta(minutes=2))
d = s.get_documents(date(2000,3,1,12),date(2000,3,1,13))
d = s.get_documents(date(2000,3,1,12,30),date(2000,3,1,13,30))
d = s.get_documents(date(2000,3,1,15,30),date(2000,3,1,16,00))



s2 = Stream(Constant2(),False,timer=StreamRef(s,delta(0)),value=42)
d2 = s2.get_documents(date(2000,3,1,12,29),date(2000,3,1,13,15))
d2 = s2.get_documents(date(2000,3,1,15,55),date(2000,3,1,16,15))
d2 = s.get_documents(date(2000,3,1,15,55),date(2000,3,1,16,15))

s3 = Stream(Clock(),False,stride=delta(minutes=5))


s4 = Stream(Pool(),False,timer=StreamRef(s3,delta(0)),data=StreamRef(s2,-delta(minutes=10),delta(0)),data_func=sum)
d4 = s4.get_documents(date(2001,1,1,10,1),date(2001,1,2,8,15))



s2 = Stream(Constant(),False,times=StreamRef(s,delta(0)),value=42)


d3 = s2.get_data(date(2000,3,1,12,29),date(2000,3,1,13,15))
d4 = s2.get_documents(date(2000,3,1,12,29),date(2000,3,1,13,15))
s3 = Stream(Constant2(),False,timer=StreamRef(s,delta(0)),value=42)
d5 = s3.get_documents(date(2000,3,1,12,29),date(2000,3,1,13,15))
d6 = s3.get_data(date(2000,3,1,12,29),date(2000,3,1,13,15))
s4 = Stream(Clock(),False,stride=delta(minutes=5))
s5 = Stream(Pool(),False,timer=StreamRef(s4,delta(0)),data=StreamRef(s3,-delta(minutes=5)),data_func=sum)
d7 = s5.get_data(date(2000,3,1,12,20),date(2000,3,1,13,10))
d8 = s5.get_documents(date(2000,3,1,12,20),date(2000,3,1,13,10))

#  def calc_document(self,timestamp,data=None,document_func=None,data_func=None):








### 
### def def_stream(stream_func,**kwargs):
###   return(0)
### def use_stream(stream_id,start=date(1970,1,1),end=delta(0),tail=None,sfilter=None):
###   return(0)
### #     - this function should be called when defining new streams that depend on the existing stream with the identifier 'stream_id'
### #     - e.g., def_stream(stream_func,input_stream=use_stream(stream,...))
### #     - it returns a data structure defining which stream and which documents of that stream are going to be used
### #     - by default, the whole stream up to the current moment is going to be used
### #     - i.e., the whole stream up to now must be calculated in order to calculate the new stream
### #     - documents outside of the range from 'start' to 'end' are not going to be used
### #     - 'start' and 'end' can be defined either with 'date' (datetime.datetime) or 'delta' (datetime.timedelta)
### #    - if defined by 'delta' then times are taken relative to the time in the new stream that uses this stream
### #     - e.g., start=delta(seconds=-5) and end=delta(seconds=5) mean that 5 seconds from the past and 5 seconds from the future will be used
### #     - i.e., the new stream can be calculated only with a delay of 5 seconds
### #     - if 'sfilter' is not 'None' then it must define a stream filter allowing to use a subselection of stream (format to be determined, e.g. MongoDB query format)
### #     - if 'tail' is not 'None' then it must be a non-negative integer specifying the number of last documents used in the time interval from 'start' to 'end' after applying 'sfilter'
### 
### #################################################
### ### FUNCTIONS TO CREATE AND CALCULATE STREAMS ###
### #################################################
### 
### # How to call these things?
### # worker (Taverna old)
### # service (Taverna new)
### # component (Kepler general)
### # actor (Kepler specific)
### # director (Kepler specific)
### # tool (Galaxy)
### # processor (?)
### 
### def time_stream(stride=delta(seconds=1),first=date(1970,1,1)):
###   return(0)
### #     - returns a stream with documents at regular intervals
### #     - each document contains information about the current time (datetime.datetime)
### #     - the first document is at the datetime 'first'
### #     - every next one is after 'stride' timedelta
### #     - 'stride' must be positive, defined with 'delta' (i.e. datetime.timedelta)
### #     - allowed parameters to 'delta' are 'days','weeks','hours','minutes','seconds','milliseconds','microseconds'
### #     - the problems of leap seconds (23:59:60, see https://en.wikipedia.org/wiki/Leap_second) and daylight saving time are not dealt with, i.e. treated the same way as the datetime module does it
### 
### def merge(stream_list):
###   return(0)
### #     - returns a stream that is merged together from the streams in 'stream_list'
### #     - the ordering is first by timestamp and second by the order within the 'stream_list'
### 
### def pool(data,times,operator,default):
###   return(0)
### #     - returns a stream with documents at exactly the same times as the 'times' stream
### #     - each document is calculated by pooling together the documents extracted from the 'data' stream according to the use_stream parameters
### #     - 'data' and 'times' must be streams represented as records returned by the use_stream function
### #     - pooling is done by calling the function corresponding to the value of 'operator'
### #     - allowed values are 'min', 'max', 'sum', 'mean', 'median','integrate_per_sec',...
### 
