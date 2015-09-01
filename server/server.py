'''
Created on 2015/7/24

@author: hubo
'''
from config import Configurable, config
from event import Scheduler, EPollPolling, Resolver, RoutineControlEvent, CBQueue
from event import PollEvent, ConnectionWriteEvent, ConnectionControlEvent, StreamDataEvent
from event.core import TimerEvent, SystemControlEvent, SystemControlLowPriorityEvent
from event.connection import ResolveRequestEvent, ResolveResponseEvent

@config('server')
class Server(Configurable):
    '''
    Create a server with all necessary parts
    '''
    _default_pollwritepriority = 700
    _default_connectionwritepriority = 600
    _default_pollreadpriority = 500 
    _default_pollerrorpriority = 800
    _default_resolverresppriority = 490
    _default_resolverreqpriority = 650
    _default_connectioncontrolpriority = 450
    _default_routinecontrolpriority = 1000
    _default_streamdatapriority = 640
    _default_timerpriority = 900
    _default_sysctlpriority = 2000
    _default_sysctllowpriority = 10
    _default_totalwritelimit = 100000
    _default_writelimitperconnection = 5
    _default_preservefornew = 100
    _default_streamdatalimit = 100000
    _default_datalimitperstream = 5
    _default_resolverpoolsize = 256
    _default_ulimitn = 32768
    def __init__(self):
        '''
        Constructor
        '''
        self.scheduler = Scheduler(EPollPolling(), getattr(self, 'processevents', None), getattr(self, 'queuedefaultsize', None), getattr(self, 'queuemaxsize', None))
        self.scheduler.queue.addSubQueue(self.pollwritepriority, PollEvent.createMatcher(category=PollEvent.WRITE_READY), 'write', None, None, CBQueue.AutoClassQueue.initHelper('fileno'))
        self.scheduler.queue.addSubQueue(self.pollreadpriority, PollEvent.createMatcher(category=PollEvent.READ_READY), 'read', None, None, CBQueue.AutoClassQueue.initHelper('fileno'))
        self.scheduler.queue.addSubQueue(self.pollerrorpriority, PollEvent.createMatcher(category=PollEvent.ERROR), 'error')
        self.scheduler.queue.addSubQueue(self.connectioncontrolpriority, ConnectionControlEvent.createMatcher(), 'control')
        self.scheduler.queue.addSubQueue(self.connectionwritepriority, ConnectionWriteEvent.createMatcher(), 'connectionwrite', self.totalwritelimit, self.totalwritelimit, CBQueue.AutoClassQueue.initHelper('connection', self.preservefornew, subqueuelimit = self.writelimitperconnection))
        self.scheduler.queue.addSubQueue(self.streamdatapriority, StreamDataEvent.createMatcher(), 'streamdata', self.streamdatalimit, self.streamdatalimit, CBQueue.AutoClassQueue.initHelper('stream', self.preservefornew, subqueuelimit = self.datalimitperstream))
        self.scheduler.queue.addSubQueue(self.routinecontrolpriority, RoutineControlEvent.createMatcher(), 'routine')
        self.scheduler.queue.addSubQueue(self.timerpriority, TimerEvent.createMatcher(), 'timer')
        self.scheduler.queue.addSubQueue(self.resolverresppriority, ResolveResponseEvent.createMatcher(), 'resolve')
        self.scheduler.queue.addSubQueue(self.resolverreqpriority, ResolveRequestEvent.createMatcher(), 'resolvereq')
        self.scheduler.queue.addSubQueue(self.sysctlpriority, SystemControlEvent.createMatcher(), 'sysctl')
        self.scheduler.queue.addSubQueue(self.sysctllowpriority, SystemControlLowPriorityEvent.createMatcher(), 'sysctllow')
        self.resolver = Resolver(self.scheduler, self.resolverpoolsize)
    def serve(self):
        if self.ulimitn is not None:
            try:
                import resource
                curr_ulimit = resource.getrlimit(resource.RLIMIT_NOFILE)
                if curr_ulimit[0] >= self.ulimitn:
                    # We do not decrease ulimit
                    pass
                elif curr_ulimit[1] >= self.ulimitn:
                    # Only increase soft limit, keep the hard limit unchanged
                    resource.setrlimit(resource.RLIMIT_NOFILE, (self.ulimitn, curr_ulimit[1]))
                else:
                    try:
                        resource.setrlimit(resource.RLIMIT_NOFILE, (self.ulimitn, self.ulimitn))
                    except:
                        # Maybe we do not have permission to change hard limit, instead we increase soft limit to the hard limit
                        resource.setrlimit(resource.RLIMIT_NOFILE, (curr_ulimit[1], curr_ulimit[1]))
            except:
                pass
        self.resolver.start()
        self.scheduler.main()
