
class Tool: 
  # inputs a mixed list of parameters and StreamRef and lists of StreamRef
  # outputs a stream
  def __init__(self):
    pass
  def calc(self,start,end,**kwargs):
    raise NotImplementedError


class Test(Tool):
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

def test_v1_func2():
  return 'test_v1'


