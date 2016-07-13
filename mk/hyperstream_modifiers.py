
class Modifier(object):
  def __init__(self):
    '''Must be overridden, self.types must be a dict with key representing input type and value representing the respective output type, each either 'doc_gen' or 'data_gen' or 'doc' or 'data' '''
    self.types = {}
  def __repr__(self):
    '''If the modifier has parameters, then this method must be overridden'''
    return(self.__class__.__name__+'()')
  def __call__(self,data):
    raise NotImplementedError
  def __add__(self,other):
    return(ComposedModifier(self,other))

class ComposedModifier(Modifier):
  def __init__(self,modifier_1,modifier_2):
    self.modifier_1 = modifier_1
    self.modifier_2 = modifier_2
    self.types = {}
    for input_type in modifier_1.types:
      if modifier_2.types.has_key(modifier_1.types[input_type]):
        self.types[input_type] = modifier_2.types[modifier_1.types[input_type]]
    if len(self.types)==0:
      raise Exception('Type mismatch in composing modifiers '+repr(modifier_1)+' and '+repr(modifier_2))
  def __repr__(self):
    return(repr(self.modifier_1)+'+'+repr(self.modifier_2))
  def __call__(self,data):
    return(self.modifier_2(self.modifier_1(data)))

class Identity(Modifier):
  def __init__(self):
    self.types = {'data_gen':'data_gen','doc_gen':'doc_gen','data':'data','doc':'doc'}
  def __call__(self,data):
    return(data)

class First(Modifier):
  def __init__(self):
    self.types = {'data_gen':'data','doc_gen':'doc'}
  def __call__(self,data):
    for d in data:
      return(d)
    return(None)

class Last(Modifier):
  def __init__(self):
    self.types = {'data_gen':'data','doc_gen':'doc'}
  def __call__(self,data):
    res = None
    for d in data:
      res = d
    return(res)   

class Head(Modifier):
  def __init__(self,n):
    self.types = {'data_gen':'data_gen','doc_gen':'doc_gen'}
    self.n = n
  def __repr__(self):
    return(self.__class__.__name__+'('+str(self.n)+')')
  def __call__(self,data):
    i = 0
    for d in data:
      i = i + 1
      if i>self.n:
	break
      yield d

class Tail(Modifier):
  def __init__(self,n):
    self.types = {'data_gen':'data_gen','doc_gen':'doc_gen'}
    self.n = n
  def __repr__(self):
    return(self.__class__.__name__+'('+str(self.n)+')')
  def __call__(self,data):
    tail = ()
    for d in data:
      tail = (tail + (d,))[-self.n:]
    for d in tail:
      yield d

class XData(Modifier):
  def __init__(self):
    self.types = {'doc_gen':'data_gen'}
  def __call__(self,data):
    for doc in data:
      yield doc[1]

class XTime(Modifier):
  def __init__(self):
    self.types = {'doc_gen':'data_gen'}
  def __call__(self,data):
    for doc in data:
      yield doc[0]

class IData(Modifier):
  def __init__(self):
    self.types = {'doc':'data'}
  def __call__(self,data):
    return(data[1])

class ITime(Modifier):
  def __init__(self):
    self.types = {'doc':'data'}
  def __call__(self,data):
    return(data[0])

class List(Modifier):
  def __init__(self):
    self.types = {'data_gen':'data','doc_gen':'data'}
  def __call__(self,data):
    return(list(data))

#m = XData()+Tail(5)+Last()+Last() # gives a type mismatch exception
#m = XData()+Tail(5)+Last()
#m = Tail(5)+XData()+Head(3)+Last()

