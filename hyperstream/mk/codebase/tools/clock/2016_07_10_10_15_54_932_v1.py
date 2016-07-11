
from hyperstream import Tool,TimeIntervals,date,delta

class Clock(Tool):
  def __str__(self):
    return(__name__)
  def __hash__(self):
    return(hash(__name__))
  def process_params(self,first=date(1970,1,1),stride=delta(seconds=1),optim=42):
    return([],{'first':first,'stride':stride,'optim':optim})
  def normalise_kwargs(self,kwargs):
    kwargs2 = {}
    for k in kwargs:
      if not k in ('optim'):
	kwargs2[k] = kwargs[k]
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

