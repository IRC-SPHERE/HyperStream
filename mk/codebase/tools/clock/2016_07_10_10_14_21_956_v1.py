from datetime import datetime as date
from datetime import timedelta as delta

# need to check if the same stream already exists in the same streambase or not
class StreamDef: # tool with params
  def __init__(self,tool,*args,**kwargs):
    self.tool = tool
    self.args = args
    self.kwargs = kwargs
  def __hash__(self):
    return(self.tool.streamdef_hash(self))
  # todo: check equality as well
class Tool: 
  # inputs a mixed list of parameters and StreamRef and lists of StreamRef
  # outputs a stream
  def __init__(self,**kwargs):
    pass
  def process_params(self,*args,**kwargs):
    return(args,kwargs)
  def streamdef_hash(self,streamdef):
    keys = tuple(sorted(streamdef.kwargs.keys()))
    values = tuple([streamdef.kwargs[k] for k in keys])
    return(hash((streamdef.tool,tuple(streamdef.args),keys,values)))
  def calc_range(self,start,end,*args,**kwargs):
    raise NotImplementedError

class Test(Tool):
  def __hash__(self):
    return(__name__)
  def process_params(self,first=date(1970,1,1),stride=delta(seconds=1),optim=42):
    return([],{'first':first,'stride':stride,'optim':optim})
  def calc_range(self,start,end,first,stride,optim):
    if start<first:
      start = first
    n_strides = int( (start-first).total_seconds() // stride.total_seconds() )
    t = first + n_strides*stride
    while t<=end:
      if t>start:
	yield (t,t)
      t = t+stride
    return
#    time_stream(stride=delta(seconds=1),first=date(1970,1,1)):

