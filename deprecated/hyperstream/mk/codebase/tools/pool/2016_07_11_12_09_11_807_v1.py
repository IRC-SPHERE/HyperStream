
from hyperstream import Tool,date,delta

class Pool(Tool):
  def __str__(self):
    return(__name__)
  def __hash__(self):
    return(hash(__name__))
  def process_params(self,timer,data,func):
    print('Defining a Pool stream')
    return([],{'timer':timer,'data':data,'func':func})
  def __call__(self,stream_def,start,end,writer,timer,data,func):
    print('Pool running from '+str(start)+' to '+str(end))
    for (t,_) in timer():
      writer([(t,'pool')])
      # TODO




