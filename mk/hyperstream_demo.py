
import hyperstream
reload(hyperstream)
from hyperstream import *

import pprint
pp = pprint.PrettyPrinter()

H = HyperEngine()
T = ToolStreamBase('codebase/tools')
M = MemoryStreamBase(42)
H.add_base(T)
H.add_base(M)

locals().update(T.get_tool_dict())


#streamref = StreamRef(T.state.base_id,'clock',MIN_DATE,MAX_DATE,None,T.tool_extractor)

M['every20s'] = T['clock'](stride=delta(seconds=20))
M['every20sx'] = T['clock'](stride=delta(seconds=20),optim=17)
M['every30s'] = T['clock'](stride=delta(seconds=30))
M['every30sx'] = clock(stride=30*sec)
# M['every30s1'] = clock_v1(stride=30*sec)
# M['every30s2'] = clock_v2(stride=30*sec)
# M['every30s3'] = clock_v3(stride=30*sec)



def print_callback(message):
  def callback():
    print(message)
  return(callback)
success = print_callback('finished')
failure = print_callback('failed')

H.add_calc2(M['every20s',date(2016,1,1,12,34),date(2016,1,1,12,37)],success,failure)
H.add_calc2(M['every30sx',date(2016,1,1,12,34),date(2016,1,1,12,37)],success,failure)
H.add_calc2(M['every30sx',date(2016,1,1,12,34),date(2016,1,1,12,37)],success,failure)

pp.pprint(M.streams)

