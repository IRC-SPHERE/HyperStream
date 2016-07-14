
from datetime import datetime as date
from datetime import timedelta as delta
from pytz import UTC

DAY = delta(days=1)
HOUR = delta(hours=1)
MINUTE = delta(minutes=1)
SEC = delta(seconds=1)
MSEC = delta(milliseconds=1)
MICROSEC = delta(microseconds=1)
MIN_DELTA = MICROSEC
MIN_DATE = date(1,1,1,0,0,0,0,UTC)
MAX_DATE = date(9999,12,31,23,59,59,999999,UTC)

# e.g. -5*SEC

#print(TimeIntervals([(date(2010,1,1),date(2015,3,1))])-TimeIntervals([(date(2011,4,1),date(2013,3,1))]))
#(2010-01-01 00:00:00,2011-04-01 00:00:00]U(2013-03-01 00:00:00,2015-03-01 00:00:00]
class TimeIntervals: # example object: (t1,t2]U(t3,t4]U...
  def __init__(self,value=[]):
    self.value=value
  def __str__(self):
    return("U".join(["("+str(a)+","+str(b)+"]" for (a,b) in self.value]))
  def __repr__(self):
    return(str(self))
  def split(self,points):
    if len(points)==0:
      return
    p = points[-1]
    for i in range(len(self.value)):
      if (self.value[i][0]<p) and (self.value[i][1]>p):
        self.value = self.value[:i]+[(self.value[i][0],p),(p,self.value[i][1])]+self.value[(i+1):]
    self.split(points[:-1])
  def compress(self):
    if len(self.value)==0:
      return
    v = self.value[:1]
    for i in range(1,len(self.value)):
      if self.value[i][0]==v[-1][1]:
        v[-1] = (v[-1][0],self.value[i][1])
      else:
        v.append(self.value[i])
    self.value = v
  def __add__(self,other):
    self_points = [point for interval in self.value for point in interval]
    other_points = [point for interval in other.value for point in interval]
    self.split(other_points)
    other.split(self_points)
    v = list(set(self.value).union(set(other.value)))
    v.sort()
    new = TimeIntervals(v)
    self.compress()
    other.compress()
    new.compress()
    return(new)
  def __sub__(self,other):
    self_points = [point for interval in self.value for point in interval]
    other_points = [point for interval in other.value for point in interval]
    self.split(other_points)
    other.split(self_points)
    v = list(set(self.value).difference(set(other.value)))
    v.sort()
    new = TimeIntervals(v)
    self.compress()
    other.compress()
    new.compress()
    return(new)

