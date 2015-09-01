'''
Created on 2015/06/01

@author: hubo
'''
from __future__ import print_function, absolute_import, division 
import copy

class EventMatcher(object):
    '''
    A matcher to match an event
    '''
    def __init__(self, indices, judgeFunc = None):
        # cut indices into real size
        for i in range(len(indices) - 1, -1, -1):
            if indices[i] is not None:
                break
        self.indices = indices[:i+1]
        if judgeFunc is not None:
            self.judge = judgeFunc
    def judge(self, event):
        return True
    def isMatch(self, event, indexStart = 0):
        if len(self.indices) > len(event.indices):
            return False
        for i in range(indexStart, len(self.indices)):
            if self.indices[i] is not None and self.indices[i] != event.indices[i]:
                return False
        return self.judge(event)
    def __repr__(self):
        cls = type(self)
        return '<EventMatcher:' + \
            repr(self.indices) + '>'
    
def withIndices(*args):
    def decorator(cls):
        cls._typename = cls.getTypename()
        for c in cls.__bases__:
            if hasattr(c, '_indicesNames'):
                cls._indicesNames = c._indicesNames + args
                return cls
        cls._indicesNames = args
        return cls
    return decorator

class Event(object):
    canignore = True
    _indicesNames = ('class',)
    '''
    A generated event with indices
    '''
    def __init__(self, *args, **kwargs):
        '''
        @param args: indices like 12,"read",... content are type-depended.
        @param kwargs:
            <indices>: input indices by name
            canignore: if the event is not processed, whether it is safe to ignore the event.
                        If it is not, the processing queue might be blocked to wait for a proper event processor.
                        Default to True.
            <others>: the properties will be set on the created event
        '''
        indicesNames = self.indicesNames()
        if kwargs and not args:
            indices = tuple(kwargs[k] for k in indicesNames if k != 'class')
        else:
            indices = args
        indices = (self._typename,) + indices
        self.indices = indices
        for k, v in zip(indicesNames, indices):
            setattr(self, k, v)
        for k, v in kwargs.items():
            if k not in indicesNames:
                setattr(self, k, v)
    @classmethod
    def indicesNames(cls):
        '''
        @return: names of indices
        '''
        return getattr(cls, '_indicesNames', ())
    @classmethod
    def getTypename(cls):
        '''
        @return: return the proper name to match
        '''
        if cls is Event:
            return None
        else:
            for c in cls.__bases__:
                if issubclass(c, Event):
                    if c is Event:
                        return cls.__name__
                    else:
                        return c.getTypename()
    @classmethod
    def createMatcher(cls, *args, **kwargs):
        '''
        @keyword _ismatch: user-defined function ismatch(event) for matching test
        @param *: indices
        @keyword **: index_name=index_value for matching criteria
        '''
        if kwargs and not args:
            return EventMatcher((cls._typename,) + tuple(kwargs.get(ind) for ind in cls.indicesNames() if ind != 'class'), kwargs.get('_ismatch'))
        else:
            return EventMatcher((cls._typename,) + args, kwargs.get('_ismatch'))
    def __repr__(self):
        cls = type(self)
        return '<' + cls.__module__ + '.' + cls.__name__ + '(' + self.getTypename() + '): {' + \
            ', '.join(repr(k) + ': ' + repr(v) for k,v in zip(self.indicesNames(), self.indices)) + '}>'
    def canignorenow(self):
        '''
        Extra criteria for an event with canignore = False.
        When this event returns True, the event is safely ignored.
        '''
        return False

