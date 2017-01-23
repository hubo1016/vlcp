'''
Created on 2015/7/24

:author: hubo
'''
from vlcp.config import Configurable, manager, config
from vlcp.event import Scheduler, DefaultPolling, RoutineControlEvent, CBQueue
from vlcp.utils.connector import Resolver
from vlcp.event import PollEvent, ConnectionWriteEvent, ConnectionControlEvent, StreamDataEvent
from vlcp.event.core import TimerEvent, SystemControlEvent, SystemControlLowPriorityEvent
from vlcp.event.connection import ResolveRequestEvent, ResolveResponseEvent
from vlcp.event.future import FutureEvent
from .module import Module, ModuleLoader, ModuleAPICall, ModuleAPIReply, ModuleNotification, ModuleLoadStateChanged
import logging
import logging.config
from vlcp.event.lock import LockEvent
import os

@config('server')
class Server(Configurable):
    '''
    Create a server with all necessary parts
    '''
    # Startup modules list. Should be a tuple of "package.classname" like::
    # 
    #    ('vlcp.service.sdn.viperflow.ViperFlow',
    #    'vlcp.service.sdn.vrouterapi.VRouterApi')
    #
    # Startup modules automatically load their dependencies, so it is not necessary
    # (though not an error) to write them explicitly.
    # 
    # If server.startup is null or empty, server tries to load modules
    # in __main__. 
    _default_startup = ()
    # enable debugging log for scheduler
    _default_debugging = False
    # File-can-write event priority, usually means socket send() can be used
    _default_pollwritepriority = 700
    # ConnectionWrite events priority
    _default_connectionwritepriority = 600
    # File-can-read event priority, usually means data received from socket
    _default_pollreadpriority = 500 
    # error event priority, usually means the socket is in an error status
    _default_pollerrorpriority = 800
    # responses from resolver
    _default_resolverresppriority = 490
    # requests to resolver
    _default_resolverreqpriority = 650
    # shutdown/reset/restart commands for connections
    _default_connectioncontrolpriority = 450
    # asynchronously starts a routine 
    _default_routinecontrolpriority = 1000
    # streams (vlcp.event.stream.Stream) data
    _default_streamdatapriority = 640
    # timers
    _default_timerpriority = 900
    # a lock can be acquired
    _default_lockpriority = 990
    # future objects been set
    _default_futurepriority = 989
    # a module is loaded/unloaded
    _default_moduleloadeventpriority = 890
    # system high-priority events
    _default_sysctlpriority = 2000
    # system low-priority events
    _default_sysctllowpriority = 10
    # module API call
    _default_moduleapicallpriority = 630
    # module API response
    _default_moduleapireplypriority = 420
    # module notify
    _default_modulenotifypriority = 410
    # default connection write queue limit for all connections
    _default_totalwritelimit = 100000
    # connection write events limit per connection
    _default_writelimitperconnection = 5
    # preserve space for newly created connections in default connection write queue
    _default_preservefornew = 100
    # stream data limit for all streams
    _default_streamdatalimit = 100000
    # stream data limit per stream
    _default_datalimitperstream = 5
    # the default multi-threading resolver pool size
    _default_resolverpoolsize = 64
    # try to set open files limit (ulimit -n) to this number if it is smaller
    _default_ulimitn = 32768
    # Use logging.config.dictConfig with this dictionary to configure the logging system.
    # It is supported in Python 2.7+
    _default_logging = None
    # Use logging.config.fileConfig with this file to configure the logging system.
    _default_loggingconfig = None
    # Force polling the sockets even if there are still unfinished events after processing
    # this number of events. May be helpful for very high stress. Should be set together
    # with server.queuemaxsize or/and server.queuedefaultsize to prevent memory overflow.
    _default_processevents = None
    # Limit the default queue size, so more events will be blocked until some events are
    # processed. This further limit the processing speed for producers to keep the system
    # stable on very high stress, but may slightly reduce the performance.
    _default_queuedefaultsize = None
    # Limit the total size of the event queue, so more events will be blocked until some events are
    # processed. This further limit the processing speed for producers to keep the system
    # stable on very high stress, but may slightly reduce the performance.
    _default_queuemaxsize = None
    def __init__(self):
        '''
        Constructor
        '''
        if hasattr(self, 'logging') and self.logging is not None:
            if isinstance(self.logging, dict):
                logging_config = dict(self.logging)
            else:
                logging_config = self.logging.todict()
            logging_config.setdefault('disable_existing_loggers', False)
            logging.config.dictConfig(logging_config)
        elif hasattr(self, 'loggingconfig') and self.loggingconfig is not None:
            logging.config.fileConfig(self.loggingconfig, disable_existing_loggers=False)
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
        self.scheduler.queue.addSubQueue(self.resolverreqpriority, ResolveRequestEvent.createMatcher(), 'resolvereq', 16)
        self.scheduler.queue.addSubQueue(self.sysctlpriority, SystemControlEvent.createMatcher(), 'sysctl')
        self.scheduler.queue.addSubQueue(self.sysctllowpriority, SystemControlLowPriorityEvent.createMatcher(), 'sysctllow')
        self.scheduler.queue.addSubQueue(self.moduleapicallpriority, ModuleAPICall.createMatcher(), 'moduleapi', None, None, CBQueue.AutoClassQueue.initHelper('target', 2, subqueuelimit = 5))
        self.scheduler.queue.addSubQueue(self.moduleapireplypriority, ModuleAPIReply.createMatcher(), 'moduleapireply')
        self.scheduler.queue.addSubQueue(self.modulenotifypriority, ModuleNotification.createMatcher(), 'modulenotify', None, None, CBQueue.AutoClassQueue.initHelper('target', subqueuelimit=5))
        self.scheduler.queue.addSubQueue(self.moduleloadeventpriority, ModuleLoadStateChanged.createMatcher(), 'moduleload')
        self.scheduler.queue.addSubQueue(self.lockpriority, LockEvent.createMatcher(), 'lock', None, None, CBQueue.AutoClassQueue.initHelper('key', subqueuelimit=1))
        self.scheduler.queue.addSubQueue(self.futurepriority, FutureEvent.createMatcher(), 'future')
        self.resolver = Resolver(self.scheduler, self.resolverpoolsize)
        self.moduleloader = ModuleLoader(self)
    def serve(self):
        """
        Start the server
        """
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
        self.moduleloader.start()
        for path in self.startup:
            self.moduleloader.subroutine(self.moduleloader.loadByPath(path))
        self.scheduler.main()

def main(configpath = None, startup = None, daemon = False, pidfile = None, fork = None):
    """
    The most simple way to start the VLCP framework
    
    :param configpath: path of a configuration file to be loaded
    
    :param startup: startup modules list. If None, `server.startup` in the configuration files
                    is used; if `server.startup` is not configured, any module defined or imported
                    into __main__ is loaded.
    
    :param daemon: if True, use python-daemon to fork and start at background. `python-daemon` must be
                   installed::
                    
                       pip install python-daemon
    
    :param pidfile: if daemon=True, this file is used for the pidfile.
    
    :param fork: use extra fork to start multiple instances
    """
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
    if fork is not None and fork > 1:
        if not hasattr(os, 'fork'):
            raise ValueError('Fork is not supported in this operating system.')
    def start_process():
        s = Server()
        s.serve()
    def main_process():
        if fork is not None and fork > 1:
            import multiprocessing
            from time import sleep
            sub_procs = []
            for i in range(0, fork):
                p = multiprocessing.Process(target = start_process)
                sub_procs.append(p)
            for i in range(0, fork):
                sub_procs[i].start()
            try:
                import signal
                def except_return(sig, frame):
                    raise SystemExit
                signal.signal(signal.SIGTERM, except_return)
                signal.signal(signal.SIGINT, except_return)
                if hasattr(signal, 'SIGHUP'):
                    signal.signal(signal.SIGHUP, except_return)
                while True:
                    sleep(2)
                    for i in range(0, fork):
                        if sub_procs[i].is_alive():
                            break
                    else:
                        break
            finally:
                for i in range(0, fork):
                    if sub_procs[i].is_alive():
                        sub_procs[i].terminate()
                for i in range(0, fork):
                    sub_procs[i].join()
        else:
            start_process()
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
            if user is not None:
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
        import sys
        # Module loading is related to current path, add it to sys.path
        cwd = os.getcwd()
        if cwd not in sys.path:
            sys.path.append(cwd)
        # Fix path issues on already-loaded modules
        for m in sys.modules.values():
            if getattr(m, '__path__', None):
                m.__path__ = [os.path.abspath(p) for p in m.__path__]
            # __file__ is used for module-relative resource locate
            if getattr(m, '__file__', None):
                m.__file__ = os.path.abspath(m.__file__)
        configs = {'gid':gid,'uid':uid,'pidfile':locker}
        config_filters = ['chroot_directory', 'working_directory', 'umask', 'detach_process',
                          'prevent_core']
        if hasattr(manager, 'daemon'):
            configs.update((k,v) for k,v in manager.daemon.config_value_items() if k in config_filters)
        if not hasattr(os, 'initgroups'):
            configs['initgroups'] = False
        with daemon.DaemonContext(**configs):
            main_process()
    else:
        main_process()
