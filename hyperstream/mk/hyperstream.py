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

from os import walk
import re
from datetime import datetime as date
from datetime import timedelta as delta

# are we working in local time or GMT time?

day = delta(days=1)
hour = delta(hours=1)
minute = delta(minutes=1)
sec = delta(seconds=1)
msec = delta(milliseconds=1)
usec = delta(microseconds=1)
epsilon = usec

MIN_DATE = date(1,1,1)
MAX_DATE = date(9999,12,31,23,59,59,999999)

# e.g. -5*sec

# hyper-system
# has its own (read-write-calc) hyper-streambases
# can use hyper-streambases (read-only or read-calc-only) from other hyper-systems
# has its own hyper-definition-stream (raw calc stream definitions) - a special stream
# has its own hyper-tool-stream (python tool definitions, must not have side-effects)
# has its own hyper-workflow-stream (python workflow definitions, must not have side-effects)
# has its own hyper-script-stream (python scripts with commands, the system executes these as they appear)
# script-stream could be called control-stream
# workflow-stream is purely for better code management, these commands could be directly launched from script-stream
# maybe workflow-stream should be merged with script-stream - they are just defs in the script-scream?

# requirement of repeatability:
# running all the scripts of the hyper-script-stream must always give the same result, regardless of time of run:
# - the same set of streams created (identifiers can differ, but names must be the same)
# - in any time-range where these streams are calculated, the documents must be identical (if stochastic, then these must be identical generative models)
# note that if a script is run more than once in the script stream then the effects can vary between these two runs
# mathematically, results must be independent of clock time
# 
# stronger requirement of replicability
# - requires that all tools were non-stochastic

# 3 different timestamps/intervals:
# - STREAM-TIME: current time interval in the stream for which a document is being calculated (defines which parts of input streams are used)
# - SCRIPT-TIME: the time when the current running script was entered into the hyper-script-stream (defines which versions of code and other similar streams are being used)
# - CLOCK-TIME: the actual current clock (used to insert documents into the system log files)

# types of streams:
# - EXTERNAL STREAM (all hyper-systems have read-only access)
# - DERIVED STREAM (one system has read-write access)
# - SYSTEM STREAM (user has read-only access, system has read-write access) PROBABLY WILL NOT BE USED, instead log files, which are not streams per se
# all streams in each streambase must have the same type
# i.e., stream type is determined by the streambase that it belongs to

# script commands:
# run a workflow
# calculate a stream in some time interval
# run garbage collection
# invalidate some stream after some time-point (require re-calculation of all dependencies)
# - think more about that
# - if some input stream gets deleted then all derived streams must become stored to ensure repeatability
# - if this stream has been used in another hyper-system, then that system needs to get invalidate-commands in its script-stream as well
# - perhaps introduce an invalidation stream into each hyper-system, then this can be checked every now and then

# how to deal with equivalences of two versions of a tool

# during defining a stream all parameters of fixed time ranges can be already replaced with actual data, e.g. the tool reference
# where are the parameters stored? json! (or bson)
# when are stream definitions identical? 
# - if the respective json is identical
# - if the jsons are equivalent (a checker)
# equivalences must be documented together with the tool
# - where and how?
# - must be hashed without the things that don't matter, so that can be found later
# - equivalences are a matter of efficiency, no effect on the content

# streambase optimisation procedure: 
# replace static streamrefs within streamdefs by actual data, if possible
# then rehash them within the streambase

# how to use streams defined by earlier scripts?
# - by name (but names can be overwritten, might introduce bugs - do we require script id as well?)
# - don't allow overwriting of names?
# - introduce global and local names?
# any workflow is run in its definition time, i.e. any global names used are resolved according to that time
# if up-to-date meanings required, then workflow must have parameters (or code copied rather than called)
# that is, any code is run in with stream names resolved in definition-time and stream data resolved in call-time
# if a workflow calls some tool and that tool gets updated, then the workflow starts calling the new tool
# but if a stream name gets overwritten, then the workflow still refers to the old name
# thus, all stream names are global but can be used locally without worrying about these getting overwritten later
# i.e. a stream directly referred to from a workflow (e.g. F['streamname']) will always remain the same stream
#
# tool default params must be resolved before checking whether the stream is already defined
#
# localise workflow used in the phantom removal workflow - how to update the localisation part?
# workflow used as a tool
# but it is hard to trace the dependencies of the workflow - it can depend on many streams, linked through both parameters and global names
# still, tool updates and workflow updates should be dealt with in a similar or same manner?
# when a workflow uses another workflow, then the name of the workflow in use will always refer to the same workflow stream (definition-time), but the actual workflow called in that stream will be decided call-time
# or maybe the workflow itself can choose the way it calls the other workflow. the default choice is call-time, but alternatives are definition-time and delta(0)
#
# tool versions 1.0 and 1.0.0 and 1.0.1 could give same output, 1.1 can give different output

#from os import listdir
#from os.path import isfile, join
#  onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]

#print(TimeIntervals([(date(2010,1,1),date(2015,3,1))])-TimeIntervals([(date(2011,4,1),date(2013,3,1))]))
#(2010-01-01 00:00:00,2011-04-01 00:00:00]U(2013-03-01 00:00:00,2015-03-01 00:00:00]
class TimeIntervals: # example object: (t1,t2]U(t3,t4]U...
  def __init__(self,value=[]):
    self.value=value
  def __str__(self):
    return("U".join(["("+str(a)+","+str(b)+"]" for (a,b) in self.value]))
  def __repr__(self):
    return(str(self))
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

#def import_tools():
#  tool_path = 'codebase/tools'
#  modules = {}
#  for (dirpath, dirnames, filenames) in walk(tool_path):
#    module_path = dirpath.replace('/','.')
#    module_files = [module_path+'.'+fn[:-3] for fn in sorted(filenames) if re.search('^((\d\d\d\d)_(\d\d)_(\d\d)_(\d\d)_(\d\d)_(\d\d)_(\d\d\d)_.*).py$',fn)]
#    for module_file in module_files:
#      mf_vec = module_file.split('.')
#      module_name = '_'.join(mf_vec[2:-1])
#      module_version = '_'.join(mf_vec[2:-1])+mf_vec[-1][23:]
#      module = __import__(module_file)
#      for mf in mf_vec[1:]:
#	module = module.__dict__[mf]
#      modules[module_name] = module
#      modules[module_version] = modules[module_name]
#  return(modules)

#locals().update(import_tools())

class Job(object):
  '''Helps to manage callbacks of calculation jobs:
  Example where the callback of j1 gets called once its two subjobs j2 and j3 are done: 
  def success():
    print('j1 is now done')
  def failure():
    print('j1 failed')
  j1 = Job(2,success,failure)
  j2 = Job(2,j1.subjob_done,j1.subjob_failed)
  j3 = Job(2,j1.subjob_done,j1.subjob_failed)
  j2.subjob_done()
  j2.subjob_done()
  j3.subjob_done()
#  j3.subjob_failed()
  j3.subjob_done()
  '''
  def __init__(self,subjobs_total,done_callback,fail_callback):
    self.subjobs_total = subjobs_total
    self.done_callback = done_callback
    self.fail_callback = fail_callback
    self.subjobs_done = 0
  def add_subjob(self):
    self.subjobs_total = self.subjobs_total + 1
  def subjob_failed(self):
    self.fail_callback()
  def subjob_done(self):
    self.subjobs_done = self.subjobs_done + 1
    if self.subjobs_done==self.subjobs_total:
      self.done_callback()

class SFilter(object):
  pass

class HyperEngine(object):
  def __init__(self):
    self.jobs = {}
    self.max_job_id = 0
    self.bases = {}
  def add_base(self,base):
    self.bases[base.state.base_id] = base
  def find_stream_refs(self,x):
    if x.__class__ in (list,tuple):
      for x_i in x:
        for y in self.find_stream_refs(x_i):
	  yield y
    elif x.__class__==dict:
      for x_i in x.values():
        for y in self.find_stream_refs(x_i):
	  yield y
    elif x.__class__==StreamRef:
      yield x
  def get_base_from_id(self,base_id):
    return(self.bases[base_id])
  def replace_streamrefs_by_readers(self,x,start,end):
    if x.__class__==list:
      return([self.replace_streamrefs_by_readers(x_i,start,end) for x_i in x])
    elif x.__class__==tuple:
      return(tuple([self.replace_streamrefs_by_readers(x_i,start,end) for x_i in x]))
    elif x.__class__==dict:
      y = {}
      for k in x:
	y[k] = self.replace_streamrefs_by_readers(x[k],start,end)
      return(y)
    elif x.__class__==StreamRef:
      base_id = x.base_id
      base = self.get_base_from_id(base_id)
      stream_id = x.stream_id
      abs_start = x.start
      if type(abs_start)==delta:
        abs_start = start + abs_start
      abs_end = x.end
      if type(abs_end)==delta:
        abs_end = end + abs_end
      sfilter = x.sfilter
      stream_reader = base.get_stream_reader(stream_id)
      def local_reader():
        for doc in stream_reader(abs_start,abs_end):
          yield doc
      return(local_reader)
    else:  
      return(x)
  def job_inputs_ready(self,job_id,finished_callback,failed_callback):
    job = self.jobs[job_id]
    base_id = job['base_id']
    stream_id = job['stream_id']
    start = job['start']
    end = job['end']
    stream_def = job['stream_def']
    base = self.get_base_from_id(base_id)
    writer = base.get_stream_writer(stream_id)
    tool = stream_def.tool
    if tool.__class__==StreamRef:
      reader = self.replace_streamrefs_by_readers(tool,start,end)
      tool_list = list(reader())
      tool = tool_list[-1][1]
    args = self.replace_streamrefs_by_readers(stream_def.args,start,end)
    kwargs = self.replace_streamrefs_by_readers(stream_def.kwargs,start,end)
    tool(start,end,writer,*args,**kwargs)
    base.state.set_id2calc(stream_id,base.state.get_id2calc(stream_id)+TimeIntervals([(start,end)]))
    del self.jobs[job_id]
    finished_callback()
  def add_calc2(self,stream_ref,finished_callback=lambda:None,failed_callback=lambda:None):
    self.add_calc(stream_ref.base_id,stream_ref.stream_id,stream_ref.start,stream_ref.end,finished_callback,failed_callback)
  def add_calc(self,base_id,stream_id,start,end,finished_callback=lambda:None,failed_callback=lambda:None):
    base = self.get_base_from_id(base_id)
    done_calc_times = base.state.get_id2calc(stream_id)
    need_to_calc_times = TimeIntervals([(start,end)]) - done_calc_times
    if str(need_to_calc_times)!='':
      if base.can_calc==False:
	failed_callback()
      stream_def = base.state.get_id2def(stream_id)
      self.max_job_id = self.max_job_id + 1
      job_id = self.max_job_id
      self.jobs[job_id] = {'base_id':base_id,'stream_id':stream_id,'start':start,'end':end,'stream_def':stream_def}
      n_refs = 0
      stream_ref_list = list(self.find_stream_refs([stream_def.tool,stream_def.args,stream_def.kwargs]))
      job = Job(1+len(stream_ref_list),lambda:self.job_inputs_ready(job_id,finished_callback,failed_callback),failed_callback)
      self.stream_ref_list = stream_ref_list ### TODO remove
      self.job = job ### TODO remove
      for stream_ref in stream_ref_list:
	n_refs = n_refs + 1
	abs_start = stream_ref.start
	if type(abs_start)==delta:
	  abs_start = start + abs_start
	abs_end = stream_ref.end
	if type(abs_end)==delta:
	  abs_end = end + abs_end
	self.add_calc(stream_ref.base_id,stream_ref.stream_id,abs_start,abs_end,job.subjob_done,job.subjob_failed)
      job.subjob_done()
    else:
      finished_callback()
  ### TODO: if cannot calculate for the whole duration, then should be possible to find out the subintervals where calculation is possible

class StreamRef(object):
  def __init__(self,base_id,stream_id,start,end,sfilter,tool_extractor):
    self.base_id = base_id
    self.stream_id = stream_id
    self.start = start
    self.end = end
    self.sfilter = sfilter
    self.tool_extractor = tool_extractor
  def __str__(self): ### TODO possibly need repr as well? or even repr instead of str?
    return(str((self.base_id,self.stream_id,self.start,self.end,self.sfilter))) ### TODO perhaps sfilter needs to go to str separately? or move all to repr?
  def __eq__(self,other):
    return(str(self)==str(other))
  def __hash__(self):
    return(hash(str(self)))
  def __call__(self,*args,**kwargs):
    tool = self.tool_extractor(self)
    (args,kwargs) = tool.process_params(*args,**kwargs)
    return(StreamDef(tool,*args,**kwargs))

class BaseState(object): ### TODO needs to stored permanently as well
  def __init__(self,base_id):
    self.base_id = base_id
    self.name2id = {}
    self.def2id = {}
    self.id2def = {}
    self.id2calc = {}
  def get_name2id(self,name):
    return(self.name2id[name])
  def set_name2id(self,name,stream_id):
    self.name2id[name] = stream_id 
  def get_def2id(self,stream_def):
    return(self.def2id[stream_def])
  def set_def2id(self,stream_def,stream_id):
    self.def2id[stream_def] = stream_id
  def get_id2def(self,stream_id):
    return(self.id2def[stream_id])
  def set_id2def(self,stream_id,stream_def):
    self.id2def[stream_id] = stream_def
  def get_id2calc(self,stream_id):
    return(self.id2calc[stream_id])
  def set_id2calc(self,stream_id,calc_interval):
    self.id2calc[stream_id] = calc_interval

class StreamBase(object):
  def __init__(self,can_calc=False,can_create=False,state=None,calc_agent=None):
    self.can_calc = can_calc
    self.can_create = can_create
    self.state = state
    self.calc_agent = calc_agent
  def tool_extractor(self,streamref):
    '''Must be overridden by deriving classes which implement a base of tool streams'''
    raise NotImplementedError
  def create_stream(self,streamdef):
    '''Must be overridden by deriving classes, must create the stream according to streamdef and return its unique identifier stream_id'''
    raise NotImplementedError
  def get_stream_reader(self,stream_id):
    '''Must be overridden by deriving classes, must return a function(start,end) which yields one by one all the documents from time interval (start,end] from the stream stream_id
       Example:
       if stream_id==1:
         def f(start,end):
            doc1 = (date(2016,1,1),'data 1')
            doc2 = (date(2017,1,1),'data 2')
            if start<doc1[0]<=end:
              yield doc1
            if start<doc2[0]<=end:
              yield doc2
         return(f)
       else:
         raise Exception('No stream with id '+str(stream_id))
    '''
    raise NotImplementedError
  def get_stream_writer(self,stream_id):
    '''Must be overridden by deriving classes, must return a function(document_collection) which writes all the given documents of the form (timestamp,data) from document_collection to the stream stream_id
       Example:
       if stream_id==1:
	 def f(document_collection):
	   for (timestamp,data) in document_collection:
	     database[timestamp] = data
         return(f)
       else:
         raise Exception('No stream with id '+str(stream_id))
    '''
    raise NotImplementedError
  def get_default_ref(self):
    '''Could be overridden by deriving classes, should return the default values for start,end,sfilter when referring to a stream in this streambase'''
    return({'start':MIN_DATE,'end':delta(0),'sfilter':None})
  def parse_setkey(self,key):
    # ( stream_id_part [,stream_id_part]* )
    if type(key)==tuple:
      if len(key)==0:
	raise Exception('Empty stream identifier')
      classes = tuple([k.__class__ for k in key])
      if SFilter in classes:
	raise Exception('Stream identifier cannot include a SFilter')
      if (delta in classes) or (date in classes):
	raise Exception('Stream identifier cannot include date or delta')
      return('.'.join([str(k) for k in key]))
    else:
      return(str(key))
  def parse_getkey(self,key):
    # ( stream_id_part [,stream_id_part]* [,start | ,start,end] [,sfilter] )
    refdict = self.get_default_ref()
    if type(key)==tuple:
      if (len(key)>=2) and key[-1].__class__==SFilter:
	refdict['sfilter'] = key[-1]
	key = key[:-1]
      if (len(key)>=3) and (key[-2].__class__ in (delta,date)) and (key[-1].__class__ in (delta,date)):
	refdict['start'] = key[-2]
	refdict['end'] = key[-1]
	key = key[:-2]
      elif (len(key)>=2) and (key[-1].__class__ in (delta,date)):
	refdict['start'] = key[-1]
	key = key[:-1]
      refdict['stream_id'] = self.parse_setkey(key)
    else:
      refdict['stream_id'] = self.parse_setkey(key)
    return(refdict)
  def __getitem__(self,key):
    key = self.parse_getkey(key)
    key['base_id'] = self.state.base_id
    key['stream_id'] = self.state.get_name2id(key['stream_id'])
    key['tool_extractor'] = self.tool_extractor
    return(StreamRef(**key))
  def __setitem__(self,key,value):
    key = self.parse_setkey(key)
    try:
      stream_id = self.state.get_def2id(value)
    except KeyError:
      stream_id = self.create_stream(value)
      self.state.set_id2calc(stream_id,TimeIntervals())
      self.state.set_def2id(value,stream_id)
      self.state.set_id2def(stream_id,value)
    self.state.set_name2id(key,stream_id)
    return

#class FileStreamBase:
#  def __init__(self,path,write=False):
#    self._write = write
#  def __getitem__(self,key):
#    if type(key)==tuple:
#      
#  def __setitem__(self,key):
#    if self._write==False:
#      raise Exception("This FileStreamBase is read-only")
#  def has_key(self,key):
#    pass


# need to check if the same stream already exists in the same streambase or not
class StreamDef(object): # tool with params
  def __init__(self,tool,*args,**kwargs):
    self.tool = tool
    self.args = args
    self.kwargs = kwargs
  def normalise(self):
    return(self.tool.normalise_streamdef(self))
  def __eq__(self,other):
    return(self.normalise()==other.normalise())
  def __hash__(self):
    return(hash(self.normalise()))

class Tool(object): 
  # inputs a mixed list of parameters and StreamRef and lists of StreamRef
  # outputs a stream
  def __init__(self):
    pass
  def process_params(self,*args,**kwargs):
    return(args,kwargs)
  def normalise_streamdef(self,streamdef):
    nt = self.normalise_tool()
    na = self.normalise_args(streamdef.args)
    nk = self.normalise_kwargs(streamdef.kwargs)
    keys = tuple(sorted(nk.keys()))
    values = tuple([nk[k] for k in keys])
    return((nt,tuple(na),keys,values))
  def normalise_tool(self):
    return(self.__class__.__module__) # alternatively, could return e.g. 'codebase.tools.test.2016_07_10_10_15_54_932_v1'
  def normalise_args(self,args):
    return(args)
  def normalise_kwargs(self,kwargs):
    return(kwargs)
  def calc_range(self,start,end,*args,**kwargs):
    raise NotImplementedError

### class Test(Tool):
### ###  def normalise_tool(self):
### ###    return(self.__class__.__module__) # alternatively, could return e.g. 'codebase.tools.test.2016_07_10_10_15_54_932_v1'
### ###  def normalise_args(self,args):
### ###    return(args)
### ###  def normalise_kwargs(self,kwargs):
### ###    return(kwargs)
###   def process_params(self,first=date(1970,1,1),stride=delta(seconds=1),optim=42):
###     return([],{'first':first,'stride':stride,'optim':optim})
###   def calc_range(self,start,end,first,stride,optim):
###     if start<first:
###       start = first
###     n_strides = int( (start-first).total_seconds() // stride.total_seconds() )
###     t = first + n_strides*stride
###     while t<=end:
###       if t>start:
### 	yield (t,t)
###       t = t+stride
###     return
### #    time_stream(stride=delta(seconds=1),first=date(1970,1,1)):

class ToolStreamBase(StreamBase):
  def __init__(self,tool_path='codebase/tools'):
    state = BaseState(tool_path)
    super(ToolStreamBase,self).__init__(can_calc=False,can_create=False,state=state)
    self.tool_path = 'codebase/tools'
    modules = {}
    for (dirpath, dirnames, filenames) in walk(tool_path):
      module_path = dirpath.replace('/','.')
      module_files = [module_path+'.'+fn[:-3] for fn in sorted(filenames) if re.search('^((\d\d\d\d)_(\d\d)_(\d\d)_(\d\d)_(\d\d)_(\d\d)_(\d\d\d)_.*).py$',fn)]
      for module_file in module_files:
        mf_vec = module_file.split('.')
        module_name = '_'.join(mf_vec[2:-1])
        module_version = '_'.join(mf_vec[2:-1])+mf_vec[-1][23:]
	def tool_importer(module_file=module_file,mf_vec=mf_vec): 
	  # this function is to be called without parameters
	  # parameters are here just to enforce early binding 
	  # http://stackoverflow.com/questions/3431676/creating-functions-in-a-loop
	  print('importing '+module_file)
          module = __import__(module_file)
          for mf in mf_vec[1:]:
            module = module.__dict__[mf]
	  return(module)
        modules[module_name] = tool_importer
        modules[module_version] = modules[module_name]
    self.tool_importers = modules
    for t in self.tool_importers.keys():
      state.set_name2id(t,t)
      state.set_id2calc(t,TimeIntervals([(MIN_DATE,MAX_DATE)]))
  def tool_extractor(self,streamref):
    '''Must be overridden by deriving classes which implement a base of tool streams'''
    module = self.tool_importers[streamref.stream_id]()
    classname = streamref.stream_id
    classname = classname[0].upper()+classname[1:]
    toolclass = getattr(module,classname)
    tool = toolclass()
    return(tool)
  def get_tool_dict(self):
    tools = {}
    for tool in self.tool_importers.keys():
      tools[tool] = self[tool]
    return(tools)
#  def create_stream(self,streamdef):
#    pass
#  def get_stream_reader(self,stream_id):
#    def reader(start,end):
#      yield (MIN_DATE,getattr(self.tools,stream_id))
#    return(reader)
#  def get_stream_writer(self,stream_id):
#    pass

class MemoryStreamBase(StreamBase):
  def __init__(self,base_id):
    state = BaseState(base_id)
    super(MemoryStreamBase,self).__init__(can_calc=True,can_create=True,state=state)
    self.streams = {}
    self.max_stream_id = 0
  def create_stream(self,streamdef):
    '''Must be overridden by deriving classes, must create the stream according to streamdef and return its unique identifier stream_id'''
    self.max_stream_id = self.max_stream_id + 1
    stream_id = self.max_stream_id
    self.streams[stream_id] = []
    return(stream_id)
  def get_stream_reader(self,stream_id):
    def reader(start,end):
      result = []
      for (timestamp,data) in self.streams[stream_id]:
	if start<timestamp<=end:
	  result.append((timestamp,data))
      result.sort(key=lambda x:x[0])
      for doc in result:
	yield doc
    return(reader)
  def get_stream_writer(self,stream_id):
    def writer(document_collection):
      self.streams[stream_id].extend(document_collection)
    return(writer)
  def get_default_ref(self):
    return({'start':delta(0),'end':delta(0),'sfilter':None})

