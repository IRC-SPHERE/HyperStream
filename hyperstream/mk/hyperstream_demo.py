
#import hyperstream
#reload(hyperstream)
#import hyperstream_bases
#reload(hyperstream_bases)
#import hyperstream_modifiers
#reload(hyperstream_modifiers)
#import hyperstream_intervals
#reload(hyperstream_intervals)
from hyperstream import *
from hyperstream_bases import *
from hyperstream_modifiers import *
from hyperstream_intervals import *

import pprint
pp = pprint.PrettyPrinter()

#H = HyperEngine()
R = ReadOnlyMemoryBase(1)
F = FileNameBase(2,'codebase/tools',MAX_DATE)
#M = ModuleBase(3,'codebase/tools',MAX_DATE)
T = ToolBase(4,'codebase/tools',MAX_DATE)
M = MemoryBase(5)

locals().update(T.versions)


#T = ToolStreamBase('codebase/tools')
#S = MemoryStreamBase(42)
#H.add_base(T)
#H.add_base(M)

#streamref = StreamRef(T.state.base_id,'clock',MIN_DATE,MAX_DATE,None,T.tool_extractor)

M['every20s'] = T['clock'](stride=delta(seconds=20))
M['every20sx'] = T['clock'](stride=delta(seconds=20),optim=17)
M['every30s'] = T['clock'](stride=delta(seconds=30))
M['every30sx'] = clock(stride=30*SEC)
# M['every30s1'] = clock_v1(stride=30*sec)
# M['every30s2'] = clock_v2(stride=30*sec)
# M['every30s3'] = clock_v3(stride=30*sec)

M['x'] = pool(timer=M['every30s'],data=M['every20s',-30*SEC],func=Last()+IData())

res = M['x',date(2016,1,1,12,34),date(2016,1,1,12,38),List()]()

# def print_callback(message):
#   def callback():
#     print(message)
#   return(callback)
# success = print_callback('finished')
# failure = print_callback('failed')
# 
# H.add_calc2(M['every20s',date(2016,1,1,12,34),date(2016,1,1,12,37)],success,failure)
# H.add_calc2(M['every30sx',date(2016,1,1,12,34),date(2016,1,1,12,37)],success,failure)
# H.add_calc2(M['every30sx',date(2016,1,1,12,34),date(2016,1,1,12,37)],success,failure)

pp.pprint(M.streams)

