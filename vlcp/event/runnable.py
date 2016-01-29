'''
Created on 2015/6/16

:author: hubo
'''
from __future__ import print_function, absolute_import, division 
import sys
from .core import QuitException, TimerEvent, SystemControlEvent
from .event import Event, withIndices

class EventHandler(object):
    '''
    Runnable with an event handler model. 
    '''
    def __init__(self, scheduler = None, daemon = False):
        self.handlers = dict()
        self.scheduler = scheduler
        self.daemon = daemon
        self.registered = False
    def bind(self, scheduler):
        self.scheduler = scheduler
    def __iter__(self):
        '''
        Keep it like a iterator. Not very useful.
        '''
        return self
    def next(self):
        '''
        Keep it like a iterator. Not very useful.
        '''
        self.send(None)
    def __next__(self):
        '''
        Python 3 next
        '''
        self.send(None)
    def send(self, etup):
        '''
        Handle events
        '''
        return self.handlers[etup[1]](etup[0], self.scheduler)
    def _setDaemon(self):
        if not self.registered:
            self.registered = True
            self.scheduler.setDaemon(self, self.daemon)
    def registerHandler(self, matcher, handler):
        '''
        Register self to scheduler
        '''
        self.handlers[matcher] = handler
        self.scheduler.register((matcher,), self)
        self._setDaemon()
    def unregisterHandler(self, matcher):
        self.scheduler.unregister((matcher,), self)
        del self.handlers[matcher]
    def unregisterAllHandlers(self):
        self.scheduler.unregister(tuple(self.handlers.keys()), self)
        self.handlers.clear()
    def registerAllHandlers(self, handlerDict):
        '''
        Register self to scheduler
        '''
        self.handlers.update(handlerDict)
        if hasattr(handlerDict, 'keys'):
            self.scheduler.register(handlerDict.keys(), self)
        else:
            self.scheduler.register(tuple(h[0] for h in handlerDict), self)
        self._setDaemon()
    def close(self):
        self.scheduler.unregisterall(self)
        self.registered = False
    def registerExceptionHandler(self, handler):
        self.exceptionHandler = handler
    def registerQuitHandler(self, handler):
        self.quitHandler = handler
    def throw(self, typ, val = None, tb = None):
        if val is None:
            if isinstance(typ, type):
                val = typ()
            else:
                val = typ
                typ = type(val)
        if isinstance(val, QuitException):
            self.quitHandler(self.scheduler)
        else:
            self.exceptionHandler(val, self.scheduler)
    def exceptionHandler(self, val, scheduler):
        raise val
    def quitHandler(self, scheduler):
        raise StopIteration

@withIndices('type', 'routine')
class RoutineControlEvent(Event):
    canignore = False
    ASYNC_START = 'asyncstart'
    DELEGATE_FINISHED = 'delegatefinished'

class IllegalMatchersException(Exception):
    pass

class generatorwrapper(object):
    '''
    Default __repr__ of a generator is not readable, use a wrapper to improve the readability
    '''
    def __init__(self, run, name = 'iterator', classname = 'routine'):
        self.run = run
        self.name = name
        self.classname = classname
    def __iter__(self):
        return self.run
    def next(self):
        return next(self.run)
    def __next__(self):
        return next(self.run)
    def send(self, arg):
        return self.run.send(arg)
    def throw(self, typ, val = None, tb = None):
        return self.run.throw(typ, val, tb)
    def __repr__(self, *args, **kwargs):
        try:
            iterator = self.run.gi_frame.f_locals[self.name]
            try:
                return '<%s %r of %r at 0x%016X>' % (self.classname, iterator,
                                                       iterator.gi_frame.f_locals['self'],
                                                       id(iterator))
            except:
                return '<%s %r at 0x%016X>' % (self.classname, iterator, id(iterator))
        except:
            return repr(self.run)
    def close(self):
        return self.run.close()

def Routine(iterator, scheduler, asyncStart = True, container = None, manualStart = False, daemon = False):
    def run():
        iterself, re = yield
        rcMatcher = RoutineControlEvent.createMatcher(RoutineControlEvent.ASYNC_START, iterself)
        if manualStart:
            yield
        try:
            if asyncStart:
                scheduler.register((rcMatcher,), iterself)
                (event, m) = yield
                event.canignore = True
                scheduler.unregister((rcMatcher,), iterself)
            if container is not None:
                container.currentroutine = iterself
            if daemon:
                scheduler.setDaemon(iterself, True)
            matchers = next(iterator)
            try:
                scheduler.register(matchers, iterself)
            except:
                iterator.throw(IllegalMatchersException(matchers))
                raise
            while True:
                try:
                    etup = yield
                except:
                    #scheduler.unregister(matchers, iterself)
                    lmatchers = matchers
                    t,v,tr = sys.exc_info()  # @UnusedVariable
                    if container is not None:
                        container.currentroutine = iterself
                    matchers = iterator.throw(t,v)
                else:
                    #scheduler.unregister(matchers, iterself)
                    lmatchers = matchers
                    if container is not None:
                        container.event = etup[0]
                        container.matcher = etup[1]
                    if container is not None:
                        container.currentroutine = iterself
                    matchers = iterator.send(etup)
                try:
                    scheduler.unregister(set(lmatchers).difference(matchers), iterself)
                    scheduler.register(set(matchers).difference(lmatchers), iterself)
                except:
                    iterator.throw(IllegalMatchersException(matchers))
                    raise
        finally:
            if asyncStart:
                re.canignore = True
                scheduler.ignore(rcMatcher)
            if container is not None:
                container.currentroutine = iterself
            iterator.close()
            scheduler.unregisterall(iterself)
    r = generatorwrapper(run())
    next(r)
    if asyncStart:
        re = RoutineControlEvent(RoutineControlEvent.ASYNC_START, r)
        r.send((r, re))
        waiter = scheduler.send(re)
        if waiter is not None:
            # This should not happen regularly
            def latencyStart(w):
                while w:
                    yield (w,)
                    w = scheduler.send(re)
            Routine(latencyStart(waiter), scheduler, False)
    else:
        r.send((r, None))
    return r

class RoutineException(Exception):
    def __init__(self, matcher, event):
        Exception.__init__(self, matcher, event)
        self.matcher = matcher
        self.event = event

class MultipleException(Exception):
    def __init__(self, exceptions):
        Exception.__init__(self, '%d exceptions occurs in parallel execution' % (len(exceptions),) \
                           + ': ' + repr(exceptions[0]) + ', ...')
        self.exceptions = exceptions

class RoutineContainer(object):
    def __init__(self, scheduler = None, daemon = False):
        self.scheduler = scheduler
        self.daemon = daemon
    def bind(self, scheduler):
        self.scheduler = scheduler
    def main(self):
        raise NotImplementedError
    def start(self, asyncStart = False):
        r = Routine(self.main(), self.scheduler, asyncStart, self, True, self.daemon)
        self.mainroutine = r
        try:
            next(r)
        except StopIteration:
            pass
        return r
    def subroutine(self, iterator, asyncStart = True, name = None, daemon = False):
        r = Routine(iterator, self.scheduler, asyncStart, self, True, daemon)
        if name is not None:
            setattr(self, name, r)
        # Call subroutine may change the currentroutine, we should restore it
        currentroutine = getattr(self, 'currentroutine', None)
        try:
            next(r)
        except StopIteration:
            pass
        self.currentroutine = currentroutine
        return r
    def terminate(self, routine = None):
        if routine is None:
            routine = self.mainroutine
        routine.close()
    def close(self):
        self.terminate()
    def waitForSend(self, event):
        '''
        Can call without delegate
        '''
        waiter = self.scheduler.send(event)
        while waiter:
            yield (waiter,)
            waiter = self.scheduler.send(event)
    def waitWithTimeout(self, timeout, *matchers):
        if timeout is None:
            yield matchers
            self.timeout = False
        else:
            th = self.scheduler.setTimer(timeout)
            try:
                tm = TimerEvent.createMatcher(th)
                yield tuple(matchers) + (tm,)
                if self.matcher is tm:
                    self.timeout = True
                else:
                    self.timeout = False
            finally:
                self.scheduler.cancelTimer(th)
    def executeWithTimeout(self, timeout, subprocess):
        if timeout is None:
            for m in subprocess:
                yield m
            self.timeout = False
        else:
            th = self.scheduler.setTimer(timeout)
            try:
                tm = TimerEvent.createMatcher(th)
                try:
                    for m in self.withException(subprocess, tm):
                        yield m
                    self.timeout = False
                except RoutineException as exc:
                    if exc.matcher is tm:
                        self.timeout = True
                    else:
                        raise
            finally:
                self.scheduler.cancelTimer(th)
                subprocess.close()
    def doEvents(self):
        '''
        Can call without delegate
        '''
        self.scheduler.wantContinue()
        cm = SystemControlEvent.createMatcher(SystemControlEvent.CONTINUE)
        yield (cm,)
    def withException(self, subprocess, *matchers):
        try:
            for m in subprocess:
                yield tuple(m) + tuple(matchers)
                if self.matcher in matchers:
                    raise RoutineException(self.matcher, self.event)
        finally:
            subprocess.close()
    def withCallback(self, subprocess, callback, *matchers):
        try:
            for m in subprocess:
                while True:
                    yield tuple(m) + tuple(matchers)
                    if self.matcher in matchers:
                        callback(self.event, self.matcher)
                    else:
                        break
        finally:
            subprocess.close()
                
    def waitForEmpty(self, queue):
        '''
        Can call without delegate
        '''
        while True:
            m = queue.waitForEmpty()
            if m is None:
                break
            else:
                yield (m,)
    def waitForAll(self, *matchers):
        eventdict = {}
        eventlist = []
        self.scheduler.register(matchers, self.currentroutine)
        try:
            ms = len(matchers)
            while ms:
                yield ()
                self.scheduler.unregister((self.matcher,), self.currentroutine)
                ms -= 1
                eventlist.append(self.event)
                eventdict[self.matcher] = self.event
            self.eventlist = eventlist
            self.eventdict = eventdict
        except:
            self.scheduler.unregister(matchers, self.currentroutine)
            raise
    def waitForAllToProcess(self, *matchers):
        eventdict = {}
        eventlist = []
        self.scheduler.register(matchers, self.currentroutine)
        try:
            ms = len(matchers)
            while ms:
                yield ()
                self.event.canignore = True
                self.scheduler.unregister((self.matcher,), self.currentroutine)
                ms -= 1
                eventlist.append(self.event)
                eventdict[self.matcher] = self.event
            self.eventlist = eventlist
            self.eventdict = eventdict
        except:
            self.scheduler.unregister(matchers, self.currentroutine)
            raise
    def waitForAllEmpty(self, *queues):
        matchers = [m for m in (q.waitForEmpty() for q in queues) if m is not None]
        while matchers:
            for m in self.waitForAll(*matchers):
                yield m
            matchers = [m for m in (q.waitForEmpty() for q in queues) if m is not None]
    def syscall_noreturn(self, func):
        '''
        Can call without delegate
        '''
        matcher = self.scheduler.syscall(func)
        yield (matcher,)
    def syscall(self, func, ignoreException = False):
        for m in self.syscall_noreturn(func):
            yield m
        if hasattr(self.event, 'exception'):
            raise self.event.exception[1]
        else:
            self.retvalue = self.event.retvalue
    def delegate(self, subprocess):
        '''
        Run a subprocess without container support
        Many subprocess assume itself running in a specified container, it uses container reference
        like self.events. Calling the subprocess in other containers will fail.
        With delegate, you can call a subprocess in any container (or without a container):
        for m in c.delegate(c.someprocess()):
            yield m
        '''
        def delegateroutine():
            try:
                for m in subprocess:
                    yield m
            except:
                typ, val, tb = sys.exc_info()
                e = RoutineControlEvent(RoutineControlEvent.DELEGATE_FINISHED, self.currentroutine)
                e.canignore = True
                for m in self.waitForSend(e):
                    yield m
                raise
            else:
                e = RoutineControlEvent(RoutineControlEvent.DELEGATE_FINISHED, self.currentroutine)
                e.canignore = True
                for m in self.waitForSend(e):
                    yield m
        r = self.subroutine(generatorwrapper(delegateroutine(), 'subprocess', 'delegate'), True)
        finish = RoutineControlEvent.createMatcher(RoutineControlEvent.DELEGATE_FINISHED, r)
        # As long as we do not use self.event to read the event, we are safe to receive them from other contaiers
        yield (finish,)
    def beginDelegateOther(self, subprocess, container, retnames = ('retvalue',)):
        '''
        Start the delegate routine, but do not wait for result, instead returns a matcher in self.retvalue.
        Useful for advanced delegates (e.g. delegate multiple subprocesses in the same time).
        This is NOT a generator.
        :param subprocess: a subroutine
        :param container: container in which to start the routine
        :param retnames: get return values from keys
        :returns: (matcher, routine) where matcher is a event matcher to get the delegate result, routine is the created routine
        '''
        def delegateroutine():
            try:
                for m in subprocess:
                    yield m
            except:
                typ, val, tb = sys.exc_info()
                e = RoutineControlEvent(RoutineControlEvent.DELEGATE_FINISHED, container.currentroutine, exception = val)
                e.canignore = True
                for m in container.waitForSend(e):
                    yield m
                raise
            else:
                e = RoutineControlEvent(RoutineControlEvent.DELEGATE_FINISHED, container.currentroutine,
                                        result = tuple(getattr(container, n, None) for n in retnames))
                e.canignore = True
                for m in container.waitForSend(e):
                    yield m
        r = container.subroutine(generatorwrapper(delegateroutine(), 'subprocess', 'delegate'), True)
        return (RoutineControlEvent.createMatcher(RoutineControlEvent.DELEGATE_FINISHED, r), r)
    def delegateOther(self, subprocess, container, retnames = ('retvalue',)):
        '''
        Another format of delegate allows delegate a subprocess in another container, and get some returning values
        the subprocess is actually running in 'container'.
        for m in self.delegateOther(c.method(), c):
            yield m
        ret = self.retvalue
        '''
        finish, _ = self.beginDelegateOther(subprocess, container, retnames)
        yield (finish,)
        if hasattr(self.event, 'exception'):
            raise self.event.exception
        for n, v in zip(retnames, self.event.result):
            setattr(self, n, v)
    def executeAll(self, subprocesses, container = None, retnames = ('retvalue',), forceclose = True):
        '''
        Execute all subprocesses and get the return values. Return values are in self.retvalue.
        :param subprocesses: sequence of subroutines (generators)
        :param container: if specified, run subprocesses in another container.
        :param retnames: get return value from container.(name) for each name in retnames.
        :param forceclose: force close the routines on exit, so all the subprocesses are terminated
        on timeout if used with executeWithTimeout
        :returns: a list of tuples, one for each subprocess, with value of retnames inside:
        [('retvalue1',),('retvalue2',),...]
        '''
        if container is None:
            container = self
        delegates = [self.beginDelegateOther(p, container, retnames) for p in subprocesses]
        matchers = [d[0] for d in delegates]
        try:
            for m in self.waitForAll(*matchers):
                yield m
            events = [self.eventdict[m] for m in matchers]
            exceptions = [e.exception for e in events if hasattr(e, 'exception')]
            if exceptions:
                if len(exceptions) == 1:
                    raise exceptions[0]
                else:
                    raise MultipleException(exceptions)
            self.retvalue = [e.result for e in events]
        finally:
            if forceclose:
                for d in delegates:
                    try:
                        container.terminate(d[1])
                    except:
                        pass
