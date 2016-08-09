
from hyperstream import Tool,date,delta

class Prod(Tool):
  def __str__(self):
    return(__name__)
  def __hash__(self):
    return(hash(__name__))
  def process_params(self,stream1,stream2):
    print('Defining a Prod stream')
    return([],{'stream1':stream1,'stream2':stream2})
  def __call__(self,stream_def,start,end,writer,stream1,stream2):
    print('Prod running from '+str(start)+' to '+str(end))
    for (t,data1) in stream1:
      (_,data2) = next(stream2)
      res = data1*data2
      writer([(t,data1*data2)])


