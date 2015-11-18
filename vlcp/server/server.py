'''
Created on 2015/7/24

@author: hubo
'''
from vlcp.config import Configurable, manager, config
from vlcp.event import Scheduler, DefaultPolling, Resolver, RoutineControlEvent, CBQueue
from vlcp.event import PollEvent, ConnectionWriteEvent, ConnectionControlEvent, StreamDataEvent
from vlcp.event.core import TimerEvent, SystemControlEvent, SystemControlLowPriorityEvent
from vlcp.event.connection import ResolveRequestEvent, ResolveResponseEvent
from .module import Module, ModuleLoader, ModuleAPICall, ModuleAPIReply, ModuleNotification, ModuleLoadStateChanged
import logging
import logging.config

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
    _default_moduleloadeventpriority = 890
    _default_sysctlpriority = 2000
    _default_sysctllowpriority = 10
    _default_moduleapicallpriority = 630
    _default_moduleapireplypriority = 420
    _default_modulenotifypriority = 410
    _default_totalwritelimit = 100000
    _default_writelimitperconnection = 5
    _default_preservefornew = 100
    _default_streamdatalimit = 100000
    _default_datalimitperstream = 5
    _default_resolverpoolsize = 256
    _default_ulimitn = 32768
    _default_startup = ()
    _default_debugging = False
    def __init__(self):
        '''
        Constructor
        '''
        if hasattr(self, 'logging'):
            if isinstance(self.logging, dict):
                logging.config.dictConfig(self.logging)
            else:
                logging.config.dictConfig(self.logging.todict())
        elif hasattr(self, 'loggingconfig'):
            logging.config.fileConfig(self.loggingconfig)
        self.scheduler = Scheduler(DefaultPolling(), getattr(self, 'processevents', None), getattr(self, 'queuedefaultsize', None), getattr(self, 'queuemaxsize', None),
                                   defaultQueueClass=CBQueue.AutoClassQueue.initHelper('_classname0'), defaultQueuePriority = 400)
        if self.debugging:
            self.scheduler.debugging = True
            self.scheduler.logger.setLevel(logging.DEBUG)
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
        self.scheduler.queue.addSubQueue(self.moduleapicallpriority, ModuleAPICall.createMatcher(), 'moduleapi', None, None, CBQueue.AutoClassQueue.initHelper('target', 2, subqueuelimit = 5))
        self.scheduler.queue.addSubQueue(self.moduleapireplypriority, ModuleAPIReply.createMatcher(), 'moduleapireply')
        self.scheduler.queue.addSubQueue(self.modulenotifypriority, ModuleNotification.createMatcher(), 'modulenotify', None, None, CBQueue.AutoClassQueue.initHelper('target', subqueuelimit=5))
        self.scheduler.queue.addSubQueue(self.moduleloadeventpriority, ModuleLoadStateChanged.createMatcher(), 'moduleload')
        self.resolver = Resolver(self.scheduler, self.resolverpoolsize)
        self.moduleloader = ModuleLoader(self)
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
        # If logging is not configured, configure it to the default (console)
        logging.basicConfig()
        self.resolver.start()
        for path in self.startup:
            self.moduleloader.subroutine(self.moduleloader.loadByPath(path))
        self.scheduler.main()

def main(configpath = None, startup = None, daemon = False, pidfile = None):
    if configpath is not None:
        manager.loadfrom(configpath)
    if startup is not None:
        manager['server.startup'] = startup
    if not manager.get('server.startup'):
        # No startup modules, try to load from __main__
        startup = []
        import __main__
        for k in dir(__main__):
            m = getattr(__main__, k)
            if isinstance(m, type) and issubclass(m, Module) and m is not Module:
                startup.append('__main__.' + k)
        manager['server.startup'] = startup
    if daemon:
        import daemon
        if not pidfile:
            pidfile = manager.get('daemon.pidfile')
        uid = manager.get('daemon.uid')
        gid = manager.get('daemon.gid')
        if gid is None:
            group = manager.get('daemon.group')
            if group is not None:
                import grp
                gid = grp.getgrnam(group)[2]
        if uid is None:
            import pwd
            user = manager.get('daemon.user')
            user_pw = pwd.getpwnam(user)
            uid = user_pw.pw_uid
            if gid is None:
                gid = user_pw.pw_gid
        if uid is not None and gid is None:
            import pwd
            gid = pwd.getpwuid(uid).pw_gid
        if pidfile:
            import fcntl
            class PidLocker(object):
                def __init__(self, path):
                    self.filepath = path
                    self.fd = None
                def __enter__(self):
                    # Create pid file
                    self.fd = os.open(pidfile, os.O_WRONLY | os.O_TRUNC | os.O_CREAT, 0o644)
                    fcntl.lockf(self.fd, fcntl.LOCK_EX|fcntl.LOCK_NB)
                    os.write(self.fd, str(os.getpid()).encode('ascii'))
                    os.fsync(self.fd)
                def __exit__(self, typ, val, tb):
                    if self.fd:
                        try:
                            fcntl.lockf(self.fd, fcntl.LOCK_UN)
                        except:
                            pass
                        os.close(self.fd)
                        self.fd = None
            locker = PidLocker(pidfile)
        else:
            locker = None
        import os
        import sys
        # Module loading is related to current path, add it to sys.path
        cwd = os.getcwd()
        if cwd not in sys.path:
            sys.path.append(cwd)
        configs = {'gid':gid,'uid':uid,'pidfile':locker}
        config_filters = ['chroot_directory', 'working_directory', 'umask', 'detach_process',
                          'prevent_core']
        if hasattr(manager, 'daemon'):
            configs.update((k,v) for k,v in manager.daemon.config_value_items() if k in config_filters)
        with daemon.DaemonContext(**configs):
            s = Server()
            s.serve()
    else:
        s = Server()
        s.serve()
