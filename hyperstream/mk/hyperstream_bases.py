
from hyperstream import *
from hyperstream_modifiers import *
from hyperstream_intervals import *
#import types
#    if type(x)==types.GeneratorType:

class ReadOnlyMemoryBase(StreamBase):
  '''An abstract streambase with a read-only set of memory-based streams.
  By default it is constructed empty with the last update at MIN_DATE.
  New streams and documents within streams are created with the update(up_to_timestamp) method, which ensures that the streambase is up to date until up_to_timestamp.
  No documents nor streams are ever deleted.
  Any deriving class must override update_streams(up_to_timestamp) which must update self.streams to be calculated until up_to_timestamp exactly.
  The data structure self.streams is a dict of streams indexed by stream_id, each stream is a list of tuples (timestamp,data), in no specific order.
  Names and identifiers are the same in this streambase.
  '''
  def __init__(self,base_id,up_to_timestamp=MIN_DATE):
    state = BaseState(base_id)
    super(ReadOnlyMemoryBase,self).__init__(can_calc=False,can_create=False,state=state)
    self.streams = {}
    self.up_to_timestamp = MIN_DATE
    if up_to_timestamp>MIN_DATE:
      self.update(up_to_timestamp)
  def repr_stream(self,stream_id):
    return('externally defined, memory-based, read-only stream')
  def update_streams(self,up_to_timestamp):
    '''Deriving classes must override this function'''
    raise NotImplementedError
  def update(self,up_to_timestamp):
    '''Call this function to ensure that the streambase is up to date at the time of timestamp.
    I.e., all the streams that have been created before or at that timestamp are calculated exactly until up_to_timestamp.
    '''
    self.update_streams(up_to_timestamp)
    self.update_state(up_to_timestamp)
  def update_state(self,up_to_timestamp):
    for stream_id in self.streams.keys():
      self.state.set_name2id(stream_id,stream_id)
      self.state.set_id2calc(stream_id,TimeIntervals([(MIN_DATE,up_to_timestamp)]))
    self.up_to_timestamp = up_to_timestamp
  def get_results(self,stream_ref,args,kwargs):
    start = stream_ref.start
    end = stream_ref.end
    if (type(start)==delta) or (type(end)==delta):
      raise Exception('Cannot calculate a relative stream_ref')
    if end>self.up_to_timestamp:
      raise Exception('The stream is not available after '+str(self.up_to_timestamp)+' and cannot be calculated')
    result = []
    for (timestamp,data) in self.streams[stream_ref.stream_id]:
      if start<timestamp<=end:
        result.append((timestamp,data))
    result.sort(key=lambda x:x[0])
    result = stream_ref.modifier( (x for x in result) ) # make a generator out from result and then apply the modifier
    return(result)
#    def reader(start,end):
#      if end>self.up_to_timestamp:
#	raise Exception # TODO: make the error more precise
#      result = []
#      for (timestamp,data) in self.streams[stream_id]:
#	if start<timestamp<=end:
#	  result.append((timestamp,data))
#      result.sort(key=lambda x:x[0])
#      for doc in result:
#	yield doc
#    return(reader)


class FileBase(ReadOnlyMemoryBase):
  '''An abstract streambase where the streams are recursive sub-folders under a given path and documents correspond to all those files which have a timestamp as their prefix in the format yyyy_mm_dd_hh_mm_ss_mmm_*.
  All the derived classes must override the function data_loader(short_path,file_long_name) which determines how the data are loaded into the document of the stream.
  The files of the described format must never be deleted.
  The call update(up_to_timestamp) must not be called unless it is guaranteed that later no files with earlier timestamps are added.
  '''
  def __init__(self,base_id,path,up_to_timestamp=MIN_DATE):
    self.path = path 
    super(FileBase,self).__init__(base_id,up_to_timestamp)
  def repr_stream(self,stream_id):
    s = 'externally defined by the file system, read-only stream'
    s = s + ', currently holding ' + str(len(self.streams[stream_id])) + ' files'
    return(s)
  def file_filter(self,sorted_file_names):
    for file_long_name in sorted_file_names:
      pattern = '^(\d\d\d\d)_(\d\d)_(\d\d)_(\d\d)_(\d\d)_(\d\d)_(\d\d\d)_(.*)$'
      matches = re.match(pattern,file_long_name)
      if matches:
        file_timestamp = date(*([int(x) for x in matches.groups()[:-1]]+[UTC]))
	file_short_name = matches.groups()[-1]
        yield (file_timestamp,file_short_name,file_long_name)
  def update_streams(self,up_to_timestamp):
    path = self.path
    for (long_path, dir_names, file_names) in walk(path):
      short_path = long_path[len(path)+1:]
      stream_id = short_path
      stream = []
      self.streams[stream_id] = stream
      for (file_timestamp,file_short_name,file_long_name) in self.file_filter(sorted(file_names)):
        if file_timestamp<=up_to_timestamp:
          file_timestamp_str = file_long_name[:23]
          stream.append((file_timestamp,self.data_loader(short_path,file_long_name)))
  def data_loader(self,short_path,file_long_name):
    raise NotImplementedError
  def get_default_ref(self):
    return({'start':MIN_DATE,'end':self.up_to_timestamp,'modifier':Identity()})

class FileNameBase(FileBase):
  '''A filebase where the end of the file name after the timestamp is used as data by the data_loader'''
  def data_loader(self,short_path,file_long_name):
    return(file_long_name[24:])

class ModuleBase(FileBase):
  '''A streambase of module streams, the documents in the streams contain functions that can be called to import the respective module'''
  def update_state(self,up_to_timestamp):
    super(ModuleBase,self).update_state(up_to_timestamp)
    versions = {}
    self.versions = versions
    for stream_id in self.streams.keys():
      for (timestamp,(version,module_importer)) in self.streams[stream_id]:
	name = stream_id.replace("/","_").replace(".","_")
	name_version = name+"_"+version.replace("/","_").replace(".","_")
	versions[name_version] = self[stream_id,MIN_DATE,timestamp]
	versions[name] = versions[name_version]
  def file_filter(self,sorted_file_names):
    for (file_timestamp,file_short_name,file_long_name) in super(ModuleBase,self).file_filter(sorted_file_names):
      if file_short_name[-3:]=='.py':
        yield (file_timestamp,file_short_name,file_long_name)
  def data_loader(self,short_path,file_long_name):
    version = file_long_name[24:-3]
    module_file = '/'.join([self.path,short_path,file_long_name])
    module_file_components = module_file[:-3].split('/')
    def module_importer():
      print('importing '+module_file)
      module = __import__(module_file[:-3].replace('/','.'))
      for component in module_file_components[1:]:
	module = module.__dict__[component]
      return(module)
    return(version,module_importer)
  def get_default_ref(self):
    return({'start':MIN_DATE,'end':self.up_to_timestamp,'modifier':Last()+IData()})

class ToolBase(ModuleBase):
  def get_results(self,stream_ref,args,kwargs):
    (version,module_importer) = super(ToolBase,self).get_results(stream_ref,args,kwargs)
    module = module_importer()
    class_name = stream_ref.stream_id
    class_name = class_name[0].upper() + class_name[1:]
    tool_class = getattr(module,class_name)
    tool = tool_class()
    (args,kwargs) = tool.process_params(*args,**kwargs)
    return(StreamDef(tool,*args,**kwargs))
    
class MemoryBase(StreamBase):
  def __init__(self,base_id):
    state = BaseState(base_id)
    super(MemoryBase,self).__init__(can_calc=True,can_create=True,state=state)
    self.streams = {}
    self.max_stream_id = 0
  def repr_stream(self,stream_id):
    s = repr(self.state.id2def[stream_id])
    return(s)
  def create_stream(self,stream_def):
    '''Must be overridden by deriving classes, must create the stream according to stream_def and return its unique identifier stream_id'''
    self.max_stream_id = self.max_stream_id + 1
    stream_id = self.max_stream_id
    self.streams[stream_id] = []
    return(stream_id)
  def get_params(self,x,start,end):
    if x.__class__ in (list,tuple):
      res = []
      for x_i in x:
	res.append(self.get_params(x_i,start,end))
      if x.__class__==list:
        return(res)
      else:
	return(tuple(res))
    elif x.__class__==dict:
      res = {}
      for x_i in x:
	res[x_i] = self.get_params(x[x_i],start,end)
      return(res)
    elif x.__class__==StreamRef:
      return(x(start=start,end=end))
    else:
      return(x)
  def get_results(self,stream_ref,args,kwargs):
    stream_id = stream_ref.stream_id
    start = stream_ref.start
    abs_start = start
    if type(start)==delta:
      try:
        abs_start = kwargs['start'] + start
      except KeyError:
	raise Exception('The stream reference to be calculated has a relative start time, need an absolute start time')
    end = stream_ref.end
    abs_end = end
    if type(end)==delta:
      try:
        abs_end = kwargs['end'] + end
      except KeyError:
	raise Exception('The stream reference to be calculated has a relative end time, need an absolute end time')
    done_calc_times = self.state.get_id2calc(stream_id)
    need_to_calc_times = TimeIntervals([(abs_start,abs_end)]) - done_calc_times
    if str(need_to_calc_times)!='':
      stream_def = self.state.get_id2def(stream_id)
      writer = self.get_stream_writer(stream_id)
      tool = stream_def.tool
      for (start2,end2) in need_to_calc_times.value:
        args2 = self.get_params(stream_def.args,start2,end2)
        kwargs2 = self.get_params(stream_def.kwargs,start2,end2)
        tool(stream_def,start2,end2,writer,*args2,**kwargs2)
	self.state.set_id2calc(stream_id,self.state.get_id2calc(stream_id)+TimeIntervals([(start2,end2)]))
      done_calc_times = self.state.get_id2calc(stream_id)
      need_to_calc_times = TimeIntervals([(abs_start,abs_end)]) - done_calc_times
      print(done_calc_times)
      print(need_to_calc_times)
      assert str(need_to_calc_times)==''
    result = []
    for (timestamp,data) in self.streams[stream_ref.stream_id]:
      if abs_start<timestamp<=abs_end:
        result.append((timestamp,data))
    result.sort(key=lambda x:x[0])
    result = stream_ref.modifier( (x for x in result) ) # make a generator out from result and then apply the modifier
    return(result)
  def get_stream_writer(self,stream_id):
    def writer(document_collection):
      self.streams[stream_id].extend(document_collection)
    return(writer)
  def get_default_ref(self):
    return({'start':delta(0),'end':delta(0),'modifier':Identity()})


#    def reader(start,end):
#      result = []
#      for (timestamp,data) in self.streams[stream_id]:
#	if start<timestamp<=end:
#	  result.append((timestamp,data))
#      result.sort(key=lambda x:x[0])
#      for doc in result:
#	yield doc
#    return(reader)

