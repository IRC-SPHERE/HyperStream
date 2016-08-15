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
from hyperstream_modifiers import *
from hyperstream_intervals import *


# from os import listdir
# from os.path import isfile, join
#  onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]

# def import_tools():
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

# locals().update(import_tools())

### class Job(object):
###   '''Helps to manage callbacks of calculation jobs:
###   Example where the callback of j1 gets called once its two subjobs j2 and j3 are done: 
###   def success():
###     print('j1 is now done')
###   def failure():
###     print('j1 failed')
###   j1 = Job(2,success,failure)
###   j2 = Job(2,j1.subjob_done,j1.subjob_failed)
###   j3 = Job(2,j1.subjob_done,j1.subjob_failed)
###   j2.subjob_done()
###   j2.subjob_done()
###   j3.subjob_done()
### #  j3.subjob_failed()
###   j3.subjob_done()
###   '''
###   def __init__(self,subjobs_total,done_callback,fail_callback):
###     self.subjobs_total = subjobs_total
###     self.done_callback = done_callback
###     self.fail_callback = fail_callback
###     self.subjobs_done = 0
###   def add_subjob(self):
###     self.subjobs_total = self.subjobs_total + 1
###   def subjob_failed(self):
###     self.fail_callback()
###   def subjob_done(self):
###     self.subjobs_done = self.subjobs_done + 1
###     if self.subjobs_done==self.subjobs_total:
###       self.done_callback()
### 
### class HyperEngine(object):
###   def __init__(self):
###     self.jobs = {}
###     self.max_job_id = 0
###     self.bases = {}
###   def add_base(self,base):
###     self.bases[base.state.base_id] = base
###   def find_stream_refs(self,x):
###     if x.__class__ in (list,tuple):
###       for x_i in x:
###         for y in self.find_stream_refs(x_i):
### 	  yield y
###     elif x.__class__==dict:
###       for x_i in x.values():
###         for y in self.find_stream_refs(x_i):
### 	  yield y
###     elif x.__class__==StreamRef:
###       yield x
###   def get_base_from_id(self,base_id):
###     return(self.bases[base_id])
###   def replace_streamrefs_by_readers(self,x,start,end):
###     if x.__class__==list:
###       return([self.replace_streamrefs_by_readers(x_i,start,end) for x_i in x])
###     elif x.__class__==tuple:
###       return(tuple([self.replace_streamrefs_by_readers(x_i,start,end) for x_i in x]))
###     elif x.__class__==dict:
###       y = {}
###       for k in x:
### 	y[k] = self.replace_streamrefs_by_readers(x[k],start,end)
###       return(y)
###     elif x.__class__==StreamRef:
###       base_id = x.base_id
###       base = self.get_base_from_id(base_id)
###       stream_id = x.stream_id
###       abs_start = x.start
###       if type(abs_start)==delta:
###         abs_start = start + abs_start
###       abs_end = x.end
###       if type(abs_end)==delta:
###         abs_end = end + abs_end
###       modifier = x.modifier
###       stream_reader = base.get_stream_reader(stream_id)
###       def local_reader():
###         for doc in stream_reader(abs_start,abs_end):
###           yield doc
###       return(local_reader)
###     else:  
###       return(x)
###   def job_inputs_ready(self,job_id,finished_callback,failed_callback):
###     job = self.jobs[job_id]
###     base_id = job['base_id']
###     stream_id = job['stream_id']
###     start = job['start']
###     end = job['end']
###     stream_def = job['stream_def']
###     base = self.get_base_from_id(base_id)
###     writer = base.get_stream_writer(stream_id)
###     tool = stream_def.tool
###     if tool.__class__==StreamRef:
###       reader = self.replace_streamrefs_by_readers(tool,start,end)
###       tool_list = list(reader())
###       tool = tool_list[-1][1]
###     args = self.replace_streamrefs_by_readers(stream_def.args,start,end)
###     kwargs = self.replace_streamrefs_by_readers(stream_def.kwargs,start,end)
###     tool(stream_def,start,end,writer,*args,**kwargs)
###     base.state.set_id2calc(stream_id,base.state.get_id2calc(stream_id)+TimeIntervals([(start,end)]))
###     del self.jobs[job_id]
###     finished_callback()
###   def add_calc2(self,stream_ref,finished_callback=lambda:None,failed_callback=lambda:None):
###     self.add_calc(stream_ref.base_id,stream_ref.stream_id,stream_ref.start,stream_ref.end,finished_callback,failed_callback)
###   def add_calc(self,base_id,stream_id,start,end,finished_callback=lambda:None,failed_callback=lambda:None):
###     base = self.get_base_from_id(base_id)
###     done_calc_times = base.state.get_id2calc(stream_id)
###     need_to_calc_times = TimeIntervals([(start,end)]) - done_calc_times
###     if str(need_to_calc_times)!='':
###       if base.can_calc==False:
### 	failed_callback()
###       stream_def = base.state.get_id2def(stream_id)
###       self.max_job_id = self.max_job_id + 1
###       job_id = self.max_job_id
###       self.jobs[job_id] = {'base_id':base_id,'stream_id':stream_id,'start':start,'end':end,'stream_def':stream_def}
###       n_refs = 0
###       stream_ref_list = list(self.find_stream_refs([stream_def.tool,stream_def.args,stream_def.kwargs]))
###       job = Job(1+len(stream_ref_list),lambda:self.job_inputs_ready(job_id,finished_callback,failed_callback),failed_callback)
###       self.stream_ref_list = stream_ref_list ### TODO remove
###       self.job = job ### TODO remove
###       for stream_ref in stream_ref_list:
### 	n_refs = n_refs + 1
### 	abs_start = stream_ref.start
### 	if type(abs_start)==delta:
### 	  abs_start = start + abs_start
### 	abs_end = stream_ref.end
### 	if type(abs_end)==delta:
### 	  abs_end = end + abs_end
### 	self.add_calc(stream_ref.base_id,stream_ref.stream_id,abs_start,abs_end,job.subjob_done,job.subjob_failed)
###       job.subjob_done()
###     else:
###       finished_callback()
###   ### TODO: if cannot calculate for the whole duration, then should be possible to find out the subintervals where calculation is possible
### 
class StreamRef(object):
    def __init__(self, base_id, stream_id, start, end, modifier, get_results_func):
        self.base_id = base_id
        self.stream_id = stream_id
        self.start = start
        self.end = end
        self.modifier = modifier
        self.get_results_func = get_results_func
    
    def __repr__(self):  ### TODO possibly need repr as well? or even repr instead of str?
        s = "StreamRef\n      BASE_ID  : " + repr(self.base_id)
        s = s + "\n      STREAM_ID: " + repr(self.stream_id)
        s = s + "\n      START    : " + repr(self.start)
        s = s + "\n      END      : " + repr(self.end)
        s = s + "\n      MODIFIER : " + repr(self.modifier)
        s = s + "\n    "
        return (s)
    
    def __eq__(self, other):
        return (str(self) == str(other))
    
    def __hash__(self):
        return (hash(str(self)))
    
    def __call__(self, *args, **kwargs):
        return (self.get_results_func(self, args, kwargs))


# tool = self.data_extractor(self)
#    (args,kwargs) = tool.process_params(*args,**kwargs)
#    return(StreamDef(tool,*args,**kwargs))

class BaseState(object):  ### TODO needs to be stored permanently as well
    def __init__(self, base_id):
        self.base_id = base_id
        self.name2id = {}
        self.def2id = {}
        self.id2def = {}
        self.id2calc = {}
    
    def get_name2id(self, name):
        return (self.name2id[name])
    
    def set_name2id(self, name, stream_id):
        self.name2id[name] = stream_id
    
    def get_def2id(self, stream_def):
        return (self.def2id[stream_def])
    
    def set_def2id(self, stream_def, stream_id):
        self.def2id[stream_def] = stream_id
    
    def get_id2def(self, stream_id):
        return (self.id2def[stream_id])
    
    def set_id2def(self, stream_id, stream_def):
        self.id2def[stream_id] = stream_def
    
    def get_id2calc(self, stream_id):
        return (self.id2calc[stream_id])
    
    def set_id2calc(self, stream_id, calc_interval):
        self.id2calc[stream_id] = calc_interval


class StreamBase(object):
    def __init__(self, can_calc=False, can_create=False, state=None, calc_agent=None):
        self.can_calc = can_calc
        self.can_create = can_create
        self.state = state
        self.calc_agent = calc_agent
    
    def get_results(self, stream_ref, args, kwargs):  # TODO: force_calc=False):
        '''Must be overridden by deriving classes.
        1. Calculates/receives the documents in the stream interval determined by the stream_ref
        2. Applies the modifiers within stream_ref
        3. Applies streambase custom modifiers as determined by args and kwargs
        4. Returns success or failure and the results (for some streambases the values of args and kwargs can override the return process, e.g. introduce callbacks)
        '''
        raise NotImplementedError
    
    def create_stream(self, stream_def):
        '''Must be overridden by deriving classes, must create the stream according to stream_def and return its unique identifier stream_id'''
        raise NotImplementedError
    
    def get_stream_writer(self, stream_id):
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
        '''Could be overridden by deriving classes, should return the default values for start,end,modifier when referring to a stream in this streambase'''
        return ({'start': MIN_DATE, 'end': delta(0), 'modifier': Identity()})
    
    def __repr__(self):
        s = super(StreamBase, self).__repr__() + ' with ID: ' + str(self.state.base_id)
        s = s + ' and containing ' + str(len(self.state.id2calc)) + " streams:"
        for stream_id in self.state.id2calc:
            s = s + '\nSTREAM ID: ' + str(stream_id)
            s = s + "\n  NAMES: "
            names = []
            for name in self.state.name2id:
                if self.state.name2id[name] == stream_id:
                    names.append(name)
            s = s + ', '.join(names)
            s = s + "\n  CALCULATED RANGES: " + repr(self.state.id2calc[stream_id])
            s = s + "\n  STREAM DEFINITION: "
            s = s + self.repr_stream(stream_id)
        return (s)
    
    def repr_stream(self, stream_id):
        '''Must be over-ridden to provide details about the stream'''
        raise NotImplementedError
    
    def parse_setkey(self, key):
        # ( stream_id_part [,stream_id_part]* )
        if type(key) == tuple:
            if len(key) == 0:
                raise Exception('Empty stream identifier')
            classes = [k.__class__ for k in key]
            if True in [issubclass(cls, Modifier) for cls in classes]:
                raise Exception('Stream identifier cannot include a Modifier')
            if (delta in classes) or (date in classes):
                raise Exception('Stream identifier cannot include date or delta')
            return ('.'.join([str(k) for k in key]))
        else:
            return (str(key))
    
    def parse_getkey(self, key):
        # ( stream_id_part [,stream_id_part]* [,start | ,start,end] [,modifier] )
        refdict = self.get_default_ref()
        if type(key) == tuple:
            if (len(key) >= 2) and issubclass(key[-1].__class__, Modifier):
                refdict['modifier'] = key[-1]
                key = key[:-1]
            if (len(key) >= 3) and (key[-2].__class__ in (delta, date)) and (key[-1].__class__ in (delta, date)):
                refdict['start'] = key[-2]
                refdict['end'] = key[-1]
                key = key[:-2]
            elif (len(key) >= 2) and (key[-1].__class__ in (delta, date)):
                refdict['start'] = key[-1]
                key = key[:-1]
            refdict['stream_id'] = self.parse_setkey(key)
        else:
            refdict['stream_id'] = self.parse_setkey(key)
        return (refdict)
    
    def __getitem__(self, key):
        key = self.parse_getkey(key)
        key['base_id'] = self.state.base_id
        key['stream_id'] = self.state.get_name2id(key['stream_id'])
        key['get_results_func'] = self.get_results
        return (StreamRef(**key))
    
    def __setitem__(self, key, value):
        key = self.parse_setkey(key)
        try:
            stream_id = self.state.get_def2id(value)
        except KeyError:
            stream_id = self.create_stream(value)
            self.state.set_id2calc(stream_id, TimeIntervals())
            self.state.set_def2id(value, stream_id)
            self.state.set_id2def(stream_id, value)
        self.state.set_name2id(key, stream_id)
        return


# need to check if the same stream already exists in the same streambase or not
class StreamDef(object):  # tool with params
    def __init__(self, tool, *args, **kwargs):
        self.tool = tool
        self.args = args
        self.kwargs = kwargs
    
    def __repr__(self):
        s = "StreamDef:"
        s = s + "\n    TOOL  : " + repr(self.tool)
        s = s + "\n    ARGS  : " + repr(self.args)
        s = s + "\n    KWARGS: " + repr(self.kwargs)
        return (s)
    
    def normalise(self):
        return (self.tool.normalise_stream_def(self))
    
    def __eq__(self, other):
        return (self.normalise() == other.normalise())
    
    def __hash__(self):
        return (hash(self.normalise()))


class Tool(object):
    # inputs a mixed list of parameters and StreamRef and lists of StreamRef
    # outputs a stream
    def __init__(self):
        pass
    
    def process_params(self, *args, **kwargs):
        return (args, kwargs)
    
    def normalise_stream_def(self, stream_def):
        nt = self.normalise_tool()
        na = self.normalise_args(stream_def.args)
        nk = self.normalise_kwargs(stream_def.kwargs)
        keys = tuple(sorted(nk.keys()))
        values = tuple([nk[k] for k in keys])
        return ((nt, tuple(na), keys, values))
    
    def normalise_tool(self):
        return (
        self.__class__.__module__)  # alternatively, could return e.g. 'codebase.tools.test.2016_07_10_10_15_54_932_v1'
    
    def normalise_args(self, args):
        return (args)
    
    def normalise_kwargs(self, kwargs):
        return (kwargs)
    
    def __call__(self, stream_def, start, end, writer, *args, **kwargs):
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
    
    #	def tool_importer(module_file=module_file,mf_vec=mf_vec):
    # this function is to be called without parameters
    # parameters are here just to enforce early binding
    # http://stackoverflow.com/questions/3431676/creating-functions-in-a-loop

###  def get_stream_reader(self,stream_id):
###    '''Must be overridden by deriving classes, must return a function(start,end) which yields one by one all the documents from time interval (start,end] from the stream stream_id
###       Example:
###       if stream_id==1:
###         def f(start,end):
###            doc1 = (date(2016,1,1),'data 1')
###            doc2 = (date(2017,1,1),'data 2')
###            if start<doc1[0]<=end:
###              yield doc1
###            if start<doc2[0]<=end:
###              yield doc2
###         return(f)
###       else:
###         raise Exception('No stream with id '+str(stream_id))
###    '''
###    raise NotImplementedError
