
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
from hyperstream_sphere import *


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

#res = M['every30s']()

res = M['x',date(2016,1,1,12,34,0,0,UTC),date(2016,1,1,12,38,0,0,UTC),List()]()

res = M['every30sx',date(1,1,1,0,0,0,0,UTC),date(1,1,1,0,5,0,0,UTC),List()]()

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

class AttrDict(dict):
  def __getattr__(self,key):
    return(self.__getitem__(key))
  def __setattr__(self,key,value):
    self.__setitem__(key,value)

# d = AttrDict()
# d.x = 7
# print(d.x)


S = SphereBase(42,date(2016,7,13,0,0,0,0,UTC))

t1 = date(2016,4,28,20,0,0,0,UTC)
t2 = date(2016,4,29,13,0,0,0,UTC)

uid_list = S['environmental',t1,t1+HOUR,Component('uid')+Data()+List()]()
uid_set = set(uid_list)
pp.pprint(uid_set)
rooms = set()
sensor_types = set()
uid_info = {}
for uid in uid_set:
  info = AttrDict()
  uid_info[uid] = info
  match = re.match('^(.*)_(.*)$',uid)
  if match:
    (info.sensor_type,info.room) = match.groups()
  else:
    (info.sensor_type,info.room) = (u'EL',None)
  rooms.add(info.room)
  sensor_types.add(info.sensor_type)
  info.example = S['environmental',t1,t1+MINUTE,CompFilter('uid',[uid])+DelNones()+Data()+Head(3)+List()]()

pp.pprint(rooms)
pp.pprint(sensor_types)
pp.pprint(uid_info)

room_filter = AttrDict()
for room in rooms:
  room_filter[room] = CompFilter('uid',[uid for uid in uid_set if uid_info[uid].room==room])



res = S['environmental',t1,t1+MINUTE,room_filter.K+Data()+List()]()

res = S['environmental',t1,t1+HOUR,room_filter.K+DelNones()+Component('temperature-S1_K')+Data()+Average()]()

res = S['environmental',t1,t1+3*MINUTE,Component('motion-S1_K')+Data()+List()]()

M['every10s'] = clock(stride=10*SEC)
M['every_min'] = clock(stride=MINUTE)
M['x'] = pool(timer=M['every10s'],data=S['environmental',-MINUTE,delta(0),Component('motion-S1_K')],func=Data()+Count())
M['x'] = pool(timer=M['every10s'],data=S['environmental',-MINUTE,delta(0),Component('motion-S1_K')],func=Data()+List())

res = M['x',t1,t1+5*MINUTE,Data()+List()]()

pp.pprint(res)





