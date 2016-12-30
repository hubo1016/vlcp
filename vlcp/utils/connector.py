'''
Created on 2015/11/30

:author: hubo

Process events multi-threaded or multi-processed
'''
from vlcp.event.runnable import RoutineContainer
import multiprocessing
import threading
import socket
import signal
import errno
import sys
from vlcp.event.event import Event, withIndices
from vlcp.event.core import POLLING_IN, PollEvent
import functools
import traceback
if sys.version_info[0] >= 3:
    from queue import Full, Queue, Empty
else:
    from Queue import Full, Queue, Empty
import logging
from vlcp.event.connection import ResolveRequestEvent, ResolveResponseEvent

@withIndices('connector', 'type')
class ConnectorControlEvent(Event):
    ADDMATCHERS = 'addmatcher'
    REMOVEMATCHERS = 'removematcher'
    SETMATCHERS = 'setmatchers'
    STOPRECEIVE = 'stopreceive'
    STARTRECEIVE = 'startreceive'
import os

@withIndices()
class MoreResultEvent(Event):
    pass

class _Pipe(object):
    "Make a pipe looks like a socket"
    def __init__(self, fd, canread = True):
        self.canread = canread
        self.canwrite = not canread
        self.fd = fd
    def setblocking(self, blocking):
        import fcntl
        if blocking:
            fcntl.fcntl(self.fd, fcntl.F_SETFL, 0)
        else:
            fcntl.fcntl(self.fd, fcntl.F_SETFL, os.O_NONBLOCK)
    def fileno(self):
        return self.fd
    def send(self, data):
        try:
            os.write(self.fd, data)
        except OSError as exc:
            raise socket.error(*exc.args)
    def recv(self, size):
        try:
            return os.read(self.fd, size)
        except OSError as exc:
            raise socket.error(*exc.args)
    def close(self):
        if self.fd:
            os.close(self.fd)
            self.fd = None
    def __del__(self):
        self.close()

class Connector(RoutineContainer):
    def __init__(self, worker_start, matchers = (), scheduler = None, mp = True, inputlimit = 0, allowcontrol = True):
        '''
        :param worker_start: func(queuein, queueout), queuein is the input queue, queueout is the
               output queue. For queuein, each object is (event, matcher) tuple; For queueout, each
               object is a tuple of events to send. Every object in queuein must have a response in
               queueout.
        :param matcheres: match events to be processed by connector.
        :param scheduler: bind to specified scheduler
        :param mp: use multiprocessing if possible. For windows, multi-threading is always used.
        :param inputlimit: input queue size limit. 0 = infinite.
        :param allowcontrol: if True, the connector accepts ConnectorControlEvent for connector configuration.
        '''
        RoutineContainer.__init__(self, scheduler, True)
        self.worker_start = worker_start
        self.matchers = set(matchers)
        self.mp = mp
        self.inputlimit = inputlimit
        self.allowcontrol = allowcontrol
        self.stopreceive = False
        self.jobs = 0
        self._moreresult_matcher = MoreResultEvent.createMatcher()
    @staticmethod
    def wrap_signal(func):
        def f(*args, **kwargs):
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            return func(*args, **kwargs)
        return f
    @staticmethod
    def connector_pipe(queuein, pipeout, worker_start):
        try:
            queueout = multiprocessing.Queue()
            worker_start(queuein, queueout)
            while True:
                try:
                    events = queueout.get()
                    if events is None:
                        break
                    pipeout.send(events)
                except EOFError:
                    break
                except OSError as exc:
                    if exc.args[0] == errno.EINTR:
                        continue
                    else:
                        break
        finally:
            pipeout.close()
    @staticmethod
    def connector_socket(queuein, pipeout, worker_start):
        try:
            queueout = Queue()
            worker_start(queuein, queueout)
            while True:
                try:
                    events = queueout.get()
                    if events is None:
                        break
                    pipeout[0].put(events)
                    while True:
                        try:
                            events = queueout.get(False)
                            pipeout[0].put(events)
                        except Empty:
                            break
                    pipeout[1].send(b'\x00')
                except EOFError:
                    break
                except OSError as exc:
                    if exc.args[0] == errno.EINTR:
                        continue
                    else:
                        break
                except:
                    pass
        finally:
            pipeout[1].close()
    def enqueue(self, queue, event, matcher):
        queue.put((event, matcher), False)
        event.canignore = True
        self.jobs += 1
        if self.jobs == 1:
            self.scheduler.setPollingDaemon(self.pipein, False)
    def sendevents(self, events):
        moreResult = False
        for e in events:
            if self._moreresult_matcher.isMatch(e):
                moreResult = True
            else:
                self.scheduler.emergesend(e)
        if not moreResult:
            self.jobs -= 1
        if self.jobs == 0:
            self.scheduler.setPollingDaemon(self.pipein, True)
    def _createobjs(self, fork, mp):
        if mp:
            queue = multiprocessing.Queue(self.inputlimit)
        else:
            queue = Queue(self.inputlimit)
        if mp:
            pipein, pipeout = multiprocessing.Pipe()
            process = multiprocessing.Process(target=self.wrap_signal(self.connector_pipe), args=(queue, pipeout, self.worker_start))
            outqueue = None
        else:
            # Use a thread instead
            # Create a socket on localhost
            if fork:
                # Linux
                pifd, pofd = os.pipe()
                pipein = _Pipe(pifd, True)
                pipeout = _Pipe(pofd, False)
                pipein.setblocking(False)
                pipeout.setblocking(True)
            else:
                addrinfo = socket.getaddrinfo('localhost', 0, socket.AF_UNSPEC, socket.SOCK_STREAM, socket.IPPROTO_TCP, socket.AI_ADDRCONFIG|socket.AI_PASSIVE)
                socket_s = socket.socket(*addrinfo[0][0:2])
                socket_s.bind(addrinfo[0][4])
                socket_s.listen(1)
                addr_target = socket_s.getsockname()
                pipeout = socket.socket(*addrinfo[0][0:2])
                pipeout.setblocking(False)
                pipeout.connect_ex(addr_target)
                pipein, _ = socket_s.accept()
                pipein.setblocking(False)
                pipeout.setblocking(True)
                socket_s.close()
            outqueue = Queue()
            process = threading.Thread(target=self.connector_socket, args=(queue, (outqueue, pipeout), self.worker_start))
        process.daemon = True
        self.pipein = pipein
        return (process, queue, pipein, outqueue)
    def main(self):
        import os
        self.resolving = set()
        if hasattr(os, 'fork'):
            fork = True
        else:
            fork = False
        mp = self.mp and fork
        (process, queue, pipein, outqueue) = self._createobjs(fork, mp)
        try:
            process.start()
            self.scheduler.registerPolling(pipein, POLLING_IN, True)
            response_matcher = PollEvent.createMatcher(pipein.fileno(), PollEvent.READ_READY)
            error_matcher = PollEvent.createMatcher(pipein.fileno(), PollEvent.ERROR)
            control_matcher = ConnectorControlEvent.createMatcher(self)
            if self.allowcontrol:
                system_matchers = (response_matcher, error_matcher, control_matcher)
            else:
                system_matchers = (response_matcher, error_matcher)
            isFull = False
            isEOF = False
            while True:
                if not isEOF:
                    if isFull or self.stopreceive:
                        yield system_matchers
                    else:
                        yield tuple(self.matchers) + system_matchers
                    if self.matcher is error_matcher:
                        isEOF = True
                if isEOF:
                    self.scheduler.unregisterPolling(pipein, self.jobs == 0)
                    self.jobs = 0
                    pipein.close()
                    pipein = None
                    for m in self.waitWithTimeout(1):
                        yield m
                    if mp:
                        process.terminate()
                    (process, queue, pipein, outqueue) = self._createobjs(fork, mp)
                    process.start()
                    self.scheduler.registerPolling(pipein, POLLING_IN, True)
                    response_matcher = PollEvent.createMatcher(pipein.fileno(), PollEvent.READ_READY)
                    error_matcher = PollEvent.createMatcher(pipein.fileno(), PollEvent.ERROR)
                    if self.allowcontrol:
                        system_matchers = (response_matcher, error_matcher, control_matcher)
                    else:
                        system_matchers = (response_matcher, error_matcher)
                    isFull = False
                elif self.matcher is control_matcher:
                    if self.event.type == ConnectorControlEvent.ADDMATCHERS:
                        for m in self.event.matchers:
                            self.matchers.add(m)
                    elif self.event.type == ConnectorControlEvent.REMOVEMATCHERS:
                        for m in self.event.matchers:
                            self.matchers.discard(m)
                    elif self.event.type == ConnectorControlEvent.SETMATCHERS:
                        self.matchers = set(self.event.matchers)
                    elif self.event.type == ConnectorControlEvent.STOPRECEIVE:
                        self.stopreceive = True
                    else:
                        self.stopreceive = False
                elif self.matcher is response_matcher:
                    if mp:
                        while pipein.poll():
                            try:
                                events = pipein.recv()
                            except EOFError:
                                isEOF = True
                                break
                            self.sendevents(events)
                    else:
                        while True:
                            try:
                                if not pipein.recv(4096):
                                    isEOF = True
                                    break
                            except socket.error as exc:
                                if exc.errno == errno.EAGAIN or exc.errno == errno.EWOULDBLOCK:
                                    break
                                elif exc.errno == errno.EINTR:
                                    continue
                                else:
                                    isEOF = True
                                    break
                        while True:
                            try:
                                events = outqueue.get(False)
                            except Empty:
                                break
                            self.sendevents(events)
                    isFull = False
                else:
                    try:
                        self.enqueue(queue, self.event, self.matcher)
                    except Full:
                        isFull = True
        finally:
            if pipein is not None:
                self.scheduler.unregisterPolling(pipein, self.jobs == 0)
                pipein.close()
            if mp:
                process.terminate()
            else:
                queue.put(None)

class ThreadPool(object):
    def __init__(self, poolsize, worker, mp = False):
        self.poolsize = poolsize
        self.worker = worker
        self.mp = mp
    def create(self, queuein, queueout):
        import os
        if hasattr(os, 'fork') and self.mp:
            pool = [multiprocessing.Process(target=self.worker, args=(queuein, queueout)) for i in range(0, self.poolsize)]
        else:
            pool = [threading.Thread(target=self.worker, args=(queuein, queueout)) for i in range(0, self.poolsize)]
        for p in pool:
            p.daemon = True
            p.start()



def async_to_async(newthread = True, mp = False):
    def decorator(func):
        @functools.wraps(func)
        def handler(*args, **kwargs):
            if mp:
                p = multiprocessing.Process(target=func, args=args, kwargs=kwargs)
                p.daemon = True
                p.start()
            elif newthread:
                t = threading.Thread(target=func, args=args, kwargs=kwargs)
                t.daemon = True
                t.start()
            else:
                func(*args, **kwargs)
        return handler
    return decorator

def async_processor(func):
    @functools.wraps(func)
    def handler(queuein, queueout):
        while True:
            try:
                r = queuein.get(True)
                if r is None:
                    queueout.put(None)
                    break
                event, matcher = r
            except OSError as exc:
                if exc.args[0] == errno.EINTR:
                    continue
                else:
                    break
            try:
                func(event, matcher, queueout)
            except:
                # Ignore
                pass
    return handler

def processor_to_async(newthread = False, mp = False):
    def decorator(func):
        @functools.wraps(func)
        @async_to_async(newthread, mp)
        def handler(event, matcher, queueout):
            try:
                output = func(event, matcher)
            except:
                queueout.put(())
            else:
                queueout.put(output)
        return handler
    return decorator

def processor(func, newthread = False, mp = False):
    return async_processor(processor_to_async()(func))

def generator_to_async(newthread = True, mp = False):
    def decorator(func):
        @functools.wraps(func)
        @async_to_async(newthread, mp)
        def handler(event, matcher, queueout):
            for es in func(event, matcher):
                queueout.put(tuple(es) + (MoreResultEvent(),))
            queueout.put(())
        return handler
    return decorator
    
def async_generator_processor(func, newthread = True, mp = False):
    return async_processor(generator_to_async(func, newthread, mp))

@withIndices('pool')
class TaskEvent(Event):
    canignore = False

@withIndices('request')
class TaskDoneEvent(Event):
    pass

class TaskPool(Connector):
    "Thread pool for small tasks"
    @staticmethod
    def _generator_wrapper(func):
        def g(event, matcher):
            try:
                for es in func():
                    yield es
            except:
                (typ, val, tr) = sys.exc_info()
                yield (TaskDoneEvent(event, exception = val),)
            else:
                yield (TaskDoneEvent(event, result = None),)
        return g
    @staticmethod
    def _processor_wrapper(func):
        def f(event, matcher):
            try:
                return (TaskDoneEvent(event, result=func()),)
            except:
                (typ, val, tr) = sys.exc_info()
                return (TaskDoneEvent(event, exception = val),)
        return f
    @staticmethod
    def _async_wrapper(func):
        def f(event, matcher, queueout):
            def sender(es):
                queueout.put(tuple(es) + (MoreResultEvent(),))
            try:
                queueout.put((TaskDoneEvent(event, result=func(sender)),))
            except:
                (typ, val, tr) = sys.exc_info()
                queueout.put((TaskDoneEvent(event, exception = val),))
        return f
    @staticmethod
    @async_processor
    def _processor(event, matcher, queueout):
        if hasattr(event, 'task'):
            processor_to_async(getattr(event, 'newthread', False))(TaskPool._processor_wrapper(event.task))(event, matcher, queueout)
        elif hasattr(event, 'gen_task'):
            generator_to_async(getattr(event, 'newthread', True))(TaskPool._generator_wrapper(event.gen_task))(event, matcher, queueout)
        elif hasattr(event, 'async_task'):
            async_to_async(getattr(event, 'newthread', True))(TaskPool._async_wrapper(event.async_task))(event, matcher, queueout)
        else:
            queueout.put(())
    def __init__(self, scheduler=None, poolsize = 64):
        self.threadpool = ThreadPool(poolsize, self._processor, False)
        Connector.__init__(self, self.threadpool.create, matchers=(TaskEvent.createMatcher(self),), scheduler=scheduler,
                           mp=False, inputlimit=poolsize, allowcontrol=False)
    def runTask(self, container, task, newthread = False):
        "Run task() in task pool. Raise an exception or get return value in container.retvalue"
        e = TaskEvent(self, task=task, newthread = newthread)
        for m in container.waitForSend(e):
            yield m
        yield (TaskDoneEvent.createMatcher(e),)
        if hasattr(container.event, 'exception'):
            raise container.event.exception
        else:
            container.retvalue = container.event.result
    def runGenTask(self, container, gentask, newthread = True):
        "Run generator gentask() in task pool, yield customized events"
        e = TaskEvent(self, gen_task = gentask, newthread = newthread)
        for m in container.waitForSend(e):
            yield m
        yield (TaskDoneEvent.createMatcher(e),)
        if hasattr(container.event, 'exception'):
            raise container.event.exception
        container.retvalue = None
    def runAsyncTask(self, container, asynctask, newthread = True):
        "Run asynctask(sender) in task pool, call sender(events) to send customized events, and also return value in container.retvalue"
        e = TaskEvent(self, async_task = asynctask, newthread = newthread)
        for m in container.waitForSend(e):
            yield m
        yield (TaskDoneEvent.createMatcher(e),)
        if hasattr(container.event, 'exception'):
            raise container.event.exception
        else:
            container.retvalue = container.event.result

class Resolver(Connector):
    logger = logging.getLogger(__name__ + '.Resolver')
    def __init__(self, scheduler = None, poolsize = 256):
        rm = ResolveRequestEvent.createMatcher()
        Connector.__init__(self, ThreadPool(poolsize, Resolver.resolver).create, (rm,), scheduler, False, poolsize, False)
        self.resolving = set()
    @staticmethod
    @processor
    def resolver(event, matcher):
        params = event.request
        try:
            addrinfo = socket.getaddrinfo(*params)
            return (ResolveResponseEvent(params, response=addrinfo),)
        except:
            et, ev, tr = sys.exc_info()
            return (ResolveResponseEvent(params, error=ev),)
    def enqueue(self, queue, event, matcher):
        if event.request in self.resolving:
            # Duplicated resolves are finished in the same time
            event.canignore = True
        else:
            Connector.enqueue(self, queue, event, matcher)
            self.resolving.add(event.request)
    def sendevents(self, events):
        Connector.sendevents(self, events)
        for e in events:
            self.resolving.remove(e.request)
