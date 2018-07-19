'''
Created on 2015/6/16

:author: hubo
'''
from __future__ import print_function, absolute_import, division
import sys
from .core import QuitException, TimerEvent, SystemControlEvent
from .event import Event, withIndices, M_, Diff_
import asyncio


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


class GeneratorExit_(BaseException):
    """
    Bypass PyPy3 bug
    """
    pass


def _close_generator(g):
    """
    PyPy 3 generator has a bug that calling `close` caused
    memory leak. Before it is fixed, use `throw` instead
    """
    if isinstance(g, generatorwrapper):
        g.close()
    elif _get_frame(g) is not None:
        try:
            g.throw(GeneratorExit_)
        except (StopIteration, GeneratorExit_):
            return
        else:
            raise RuntimeError("coroutine ignored GeneratorExit")


def _await(coroutine):
    """
    Return a generator
    """
    if hasattr(coroutine, '__await__'):
        return coroutine.__await__()
    else:
        return coroutine


@withIndices('type', 'routine')
class RoutineControlEvent(Event):
    DELEGATE_FINISHED = 'delegatefinished'

class IllegalMatchersException(Exception):
    pass


def _get_frame(obj):
    if hasattr(obj, 'cr_frame'):
        return obj.cr_frame
    else:
        return obj.gi_frame


class generatorwrapper(object):
    '''
    Default __repr__ of a generator is not readable, use a wrapper to improve the readability
    '''
    __slots__ = ('run', 'name', 'classname', '_iter')
    def __init__(self, run, name = 'coroutine', classname = 'routine'):
        self.run = run
        self.name = name
        self.classname = classname
        if hasattr(run, '__await__'):
            self._iter = run.__await__()
        else:
            self._iter = run
    def __iter__(self):
        return self._iter
    __await__ = __iter__
    def next(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise StopIteration
    def __next__(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise StopIteration
    def send(self, arg):
        try:
            return self._iter.send(arg)
        except StopIteration:
            raise StopIteration
    def throw(self, typ, val = None, tb = None):
        try:
            return self._iter.throw(typ, val, tb)
        except StopIteration:
            raise StopIteration
    def __repr__(self, *args, **kwargs):
        try:
            iterator = _get_frame(self.run).f_locals[self.name]
            try:
                return '<%s %r of %r at 0x%016X>' % (self.classname, iterator,
                                                       _get_frame(iterator).f_locals['self'],
                                                       id(iterator))
            except Exception:
                return '<%s %r at 0x%016X>' % (self.classname, iterator, id(iterator))
        except Exception:
            return repr(self.run)
    def close(self):
        return _close_generator(self.run)


def Routine(coroutine, scheduler, asyncStart = True, container = None, manualStart = False, daemon = False):
    """
    This wraps a normal generator to become a VLCP routine. Usually you do not need to call this yourself;
    `container.start` and `container.subroutine` calls this automatically.
    """
    def run():
        iterator = _await(coroutine)
        iterself = yield
        if manualStart:
            yield
        try:
            if asyncStart:
                scheduler.yield_(iterself)
                yield
            if container is not None:
                container.currentroutine = iterself
            if daemon:
                scheduler.setDaemon(iterself, True)
            try:
                matchers = next(iterator)
            except StopIteration:
                return
            while matchers is None:
                scheduler.yield_(iterself)
                yield
                try:
                    matchers = next(iterator)
                except StopIteration:
                    return
            try:
                scheduler.register(matchers, iterself)
            except Exception:
                try:
                    iterator.throw(IllegalMatchersException(matchers))
                except StopIteration:
                    pass
                raise
            while True:
                try:
                    etup = yield
                except GeneratorExit_:
                    raise
                except:
                    #scheduler.unregister(matchers, iterself)
                    lmatchers = matchers
                    t,v,tr = sys.exc_info()  # @UnusedVariable
                    if container is not None:
                        container.currentroutine = iterself
                    try:
                        matchers = iterator.throw(t,v)
                    except StopIteration:
                        return
                else:
                    #scheduler.unregister(matchers, iterself)
                    lmatchers = matchers
                    if container is not None:
                        container.currentroutine = iterself
                    try:
                        matchers = iterator.send(etup)
                    except StopIteration:
                        return
                while matchers is None:
                    scheduler.yield_(iterself)
                    yield
                    try:
                        matchers = next(iterator)
                    except StopIteration:
                        return
                try:
                    if hasattr(matchers, 'two_way_difference'):
                        reg, unreg = matchers.two_way_difference(lmatchers)
                    else:
                        reg = set(matchers).difference(lmatchers)
                        unreg = set(lmatchers).difference(matchers)
                    scheduler.register(reg, iterself)
                    scheduler.unregister(unreg, iterself)
                except Exception:
                    try:
                        iterator.throw(IllegalMatchersException(matchers))
                    except StopIteration:
                        pass
                    raise
        finally:
            # iterator.close() can be called in other routines, we should restore the currentroutine variable
            if container is not None:
                lastcurrentroutine = getattr(container, 'currentroutine', None)
                container.currentroutine = iterself
            else:
                lastcurrentroutine = None
            _close_generator(coroutine)
            if container is not None:
                container.currentroutine = lastcurrentroutine
            scheduler.unregisterall(iterself)
    r = generatorwrapper(run())
    next(r)
    r.send(r)
    return r

class RoutineException(Exception):
    """
    Special exception type raised from :py:method::`vlcp.event.runnable.RoutineContainer.withException`.
    e.matcher is set to the matcher and e.event is set to the matched event.
    """
    def __init__(self, matcher, event):
        Exception.__init__(self, matcher, event)
        self.matcher = matcher
        self.event = event

class MultipleException(Exception):
    """
    Special exception type raised from :py:method::`vlcp.event.runnable.RoutineContainer.executeAll`
    """
    def __init__(self, exceptions):
        Exception.__init__(self, '%d exceptions occurs in parallel execution' % (len(exceptions),) \
                           + ': ' + repr(exceptions[0]) + ', ...')
        self.exceptions = exceptions

class RoutineContainer(object):
    """
    A routine container groups several routines together and shares data among them. It is also
    used to pass important information like events, matchers and return values to the routine.
    
    Several attributes are commonly used:
        
    currentroutine
        Always set to the executing routine - which is the wrapped routine object of the routine itself
        
    mainroutine
        Set to the main routine `container.main` (started by `container.start`)
        
    """
    
    _container_cache = {}
    
    def __init__(self, scheduler = None, daemon = False):
        """
        Create the routine container.
        
        :param scheduler: The scheduler. This must be set; if None is used, it must be set with
                          `container.bind(scheduler)` before using.
        
        :param daemon: If daemon = True, the main routine `container.main` is set to be a daemon routine.
                       A daemon routine does not stop the scheduler from quitting; if all non-daemon routines
                       are quit, the scheduler stops.
        """
        self.scheduler = scheduler
        self.daemon = daemon
    def bind(self, scheduler):
        """
        If scheduler is not specified, bind the scheduler
        """
        self.scheduler = scheduler
    def main(self):
        """
        The main routine method, should be rewritten to a generator method
        """
        raise NotImplementedError
    def start(self, asyncStart = False):
        """
        Start `container.main` as the main routine.
        
        :param asyncStart: if True, start the routine in background. By default, the routine
                           starts in foreground, which means it is executed to the first
                           `yield` statement before returning. If the started routine raises
                           an exception, the exception is re-raised to the caller of `start`
        """
        r = Routine(self.main(), self.scheduler, asyncStart, self, True, self.daemon)
        self.mainroutine = r
        try:
            next(r)
        except StopIteration:
            pass
        return r
    def subroutine(self, iterator, asyncStart = True, name = None, daemon = False):
        """
        Start extra routines in this container.
        
        :param iterator: A generator object i.e the return value of a generator method `my_routine()`,
                         or a coroutine
        
        :param asyncStart: if False, start the routine in foreground. By default, the routine
                           starts in background, which means it is not executed until the current caller
                           reaches the next `yield` statement or quit.
        
        :param name: if not None, `container.<name>` is set to the routine object. This is useful when
                     you want to terminate the routine from outside.
                     
        :param daemon: if True, this routine is set to be a daemon routine.
                       A daemon routine does not stop the scheduler from quitting; if all non-daemon routines
                       are quit, the scheduler stops. 
        """
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
        """
        Stop a routine.
        
        :param routine: if None, stop the main routine. If not None, it should be a routine object. You
                        can specify a name for a subroutine, and use `container.<name>` to retrieve it.
        """
        if routine is None:
            routine = self.mainroutine
        routine.close()
    def close(self):
        """
        Same as `terminate()`
        """
        self.terminate()
    
    async def wait_for_send(self, event, *, until=None):
        '''
        Send an event to the main event queue. Can call without delegate.
        
        :param until: if the callback returns True, stop sending and return
        
        :return: the last True value the callback returns, or None
        '''
        while True:
            if until:
                r = until()
                if r:
                    return r
            waiter = self.scheduler.send(event)
            if waiter is None:
                break
            await waiter
    waitForSend = wait_for_send
    
    async def wait_with_timeout(self, timeout, *matchers):
        """
        Wait for multiple event matchers, or until timeout.
        
        :param timeout: a timeout value
        
        :param \*matchers: event matchers
        
        :return: (is_timeout, event, matcher). When is_timeout = True, event = matcher = None.
        """
        if timeout is None:
            ev, m = await M_(*matchers)
            return False, ev, m
        else:
            th = self.scheduler.setTimer(timeout)
            try:
                tm = TimerEvent.createMatcher(th)
                ev, m = await M_(*(tuple(matchers) + (tm,)))
                if m is tm:
                    return True, None, None
                else:
                    return False, ev, m
            finally:
                self.scheduler.cancelTimer(th)
    
    waitWithTimeout = wait_with_timeout
    
    async def execute_with_timeout(self, timeout, subprocess):
        """
        Execute a subprocess with timeout. If time limit exceeds, the subprocess is terminated,
        and `container.timeout` is set to True; otherwise the `container.timeout` is set to False.
        
        You can uses `executeWithTimeout` with other help functions to create time limit for them::
        
            timeout, result = await container.executeWithTimeout(10, container.executeAll([routine1(), routine2()]))
        
        :return: (is_timeout, result) When is_timeout = True, result = None
        """
        if timeout is None:
            return (False, await subprocess)
        else:
            th = self.scheduler.setTimer(timeout)
            try:
                tm = TimerEvent.createMatcher(th)
                try:
                    r = await self.with_exception(subprocess, tm)
                except RoutineException as exc:
                    if exc.matcher is tm:
                        return True, None
                    else:
                        raise
                else:
                    return False, r
            finally:
                self.scheduler.cancelTimer(th)
    
    executeWithTimeout = execute_with_timeout
    
    async def do_events(self):
        '''
        Suspend this routine until the next polling. This can be used to give CPU time for other routines and
        socket processings in long calculating procedures. Can call without delegate.
        '''
        self.scheduler.wantContinue()
        await SystemControlEvent.createMatcher(SystemControlEvent.CONTINUE)
    
    doEvents = do_events
    
    async def with_exception(self, subprocess, *matchers):
        """
        Monitoring event matchers while executing a subprocess. If events are matched before the subprocess ends,
        the subprocess is terminated and a RoutineException is raised.
        """
        def _callback(event, matcher):
            raise RoutineException(matcher, event)
        return await self.with_callback(subprocess, _callback, *matchers)
    
    withException = with_exception 
    
    @asyncio.coroutine
    def with_callback(self, subprocess, callback, *matchers, intercept_callback = None):
        """
        Monitoring event matchers while executing a subprocess. `callback(event, matcher)` is called each time
        an event is matched by any event matchers. If the callback raises an exception, the subprocess is terminated.
        
        :param intercept_callback: a callback called before a event is delegated to the inner subprocess
        """
        it_ = _await(subprocess)
        if not matchers and not intercept_callback:
            return (yield from it_)
        try:
            try:
                m = next(it_)
            except StopIteration as e:
                return e.value
            while True:
                if m is None:
                    try:
                        yield
                    except GeneratorExit_:
                        raise
                    except:
                        t,v,tr = sys.exc_info()  # @UnusedVariable
                        try:
                            m = it_.throw(t,v)
                        except StopIteration as e:
                            return e.value
                    else:                        
                        try:
                            m = next(it_)
                        except StopIteration as e:
                            return e.value
                else:
                    while True:
                        try:
                            ev, matcher = yield m + tuple(matchers)
                        except GeneratorExit_:
                            # subprocess is closed in `finally` clause
                            raise
                        except:
                            # delegate this exception inside
                            t,v,tr = sys.exc_info()  # @UnusedVariable
                            try:
                                m = it_.throw(t,v)
                            except StopIteration as e:
                                return e.value
                        else:
                            if matcher in matchers:
                                callback(ev, matcher)
                            else:
                                if intercept_callback:
                                    intercept_callback(ev, matcher)
                                break
                    try:
                        m = it_.send((ev, matcher))
                    except StopIteration as e:
                        return e.value
        finally:
            _close_generator(subprocess)
    
    withCallback = with_callback
    
    async def wait_for_empty(self, queue):
        '''
        Wait for a queue to be empty. Can call without delegate
        '''
        while True:
            m = queue.waitForEmpty()
            if m is None:
                break
            else:
                await m
    
    waitForEmpty = wait_for_empty 
    
    async def wait_for_all(self, *matchers, eventlist = None, eventdict = None, callback = None):
        """
        Wait until each matcher matches an event. When this coroutine method returns,
        `eventlist` is set to the list of events in the arriving order (may not
        be the same as the matchers); `eventdict` is set to a dictionary
        `{matcher1: event1, matcher2: event2, ...}`
        
        :param eventlist: use external event list, so when an exception occurs
                          (e.g. routine close), you can retrieve the result
                          from the passed-in list
        
        :param eventdict: use external event dict
        
        :param callback: if not None, the callback should be a callable callback(event, matcher)
                         which is called each time an event is received
        
        :return: (eventlist, eventdict)
        """
        if eventdict is None:
            eventdict = {}
        if eventlist is None:
            eventlist = []
        ms = len(matchers)
        last_matchers = Diff_(matchers)
        while ms:
            ev, m = await last_matchers
            ms -= 1
            if callback:
                callback(ev, m)
            eventlist.append(ev)
            eventdict[m] = ev
            last_matchers = Diff_(last_matchers, remove=(m,))
        return eventlist, eventdict
    
    waitForAll = wait_for_all
    
    async def wait_for_all_to_process(self, *matchers, eventlist = None, eventdict = None,
                                            callback = None):
        """
        Similar to `waitForAll`, but set `canignore=True` for these events. This ensures
        blocking events are processed correctly.
        """
        def _callback(event, matcher):
            event.canignore = True
            if callback:
                callback(event, matcher)
        return await self.wait_for_all(*matchers, eventlist=eventlist,
                                       eventdict=eventdict, callback=_callback)

    waitForAllToProcess = wait_for_all_to_process
    
    async def wait_for_all_empty(self, *queues):
        """
        Wait for multiple queues to be empty at the same time.

        Require delegate when calling from coroutines running in other containers
        """
        matchers = [m for m in (q.waitForEmpty() for q in queues) if m is not None]
        while matchers:
            await self.wait_for_all(*matchers)
            matchers = [m for m in (q.waitForEmpty() for q in queues) if m is not None]
    
    waitForAllEmpty = wait_for_all_empty
    
    @asyncio.coroutine
    def syscall_noreturn(self, func):
        '''
        Call a syscall method. A syscall method is executed outside of any routines, directly
        in the scheduler loop, which gives it chances to directly operate the event loop.
        See :py:method::`vlcp.event.core.Scheduler.syscall`.
        '''
        matcher = self.scheduler.syscall(func)
        while not matcher:
            yield
            matcher = self.scheduler.syscall(func)
        ev, _ = yield (matcher,)
        return ev
    
    async def syscall(self, func, ignoreException = False):
        """
        Call a syscall method and retrieve its return value
        """
        ev = await self.syscall_noreturn(func)
        if hasattr(ev, 'exception'):
            if ignoreException:
                return
            else:
                raise ev.exception[1]
        else:
            return ev.retvalue
        
    async def delegate(self, subprocess, forceclose = False):
        '''
        Run a subprocess without container support
        
        Many subprocess assume itself running in a specified container, it uses container reference
        like self.events. Calling the subprocess in other containers will fail.
        
        With delegate, you can call a subprocess in any container (or without a container)::
        
            r = await c.delegate(c.someprocess())
        
        :return: original return value
        '''
        finish, r = self.begin_delegate(subprocess)
        return await self.end_delegate(finish, r, forceclose)
    
    async def end_delegate(self, delegate_matcher, routine = None, forceclose = False):
        """
        Retrieve a begin_delegate result. Must be called immediately after begin_delegate
        before any other `await`, or the result might be lost.
        
        Do not use this method without thinking. Always use `RoutineFuture` when possible.
        """
        try:
            ev = await delegate_matcher
            if hasattr(ev, 'exception'):
                raise ev.exception
            else:
                return ev.result
        finally:
            if forceclose and routine:
                routine.close()

    def begin_delegate(self, subprocess):
        '''
        Start the delegate routine, but do not wait for result, instead returns a (matcher, routine) tuple.
        Useful for advanced delegates (e.g. delegate multiple subprocesses in the same time).
        This is NOT a coroutine method.
        
        WARNING: this is not a safe way for asynchronous executing and get the result. Use `RoutineFuture` instead.
        
        :param subprocess: a coroutine
        
        :returns: (matcher, routine) where matcher is a event matcher to get the delegate result, routine is the created routine
        '''
        async def delegateroutine():
            try:
                r = await subprocess
            except:
                _, val, _ = sys.exc_info()
                e = RoutineControlEvent(RoutineControlEvent.DELEGATE_FINISHED, self.currentroutine,
                                        exception=val)
                self.scheduler.emergesend(e)
                raise
            else:
                e = RoutineControlEvent(RoutineControlEvent.DELEGATE_FINISHED, self.currentroutine,
                                        result = r)
                await self.wait_for_send(e)
        r = self.subroutine(generatorwrapper(delegateroutine(), 'subprocess', 'delegate'), True)
        finish = RoutineControlEvent.createMatcher(RoutineControlEvent.DELEGATE_FINISHED, r)
        return finish, r
    
    def begin_delegate_other(self, subprocess, container, retnames = ('',)):
        '''
        DEPRECATED Start the delegate routine, but do not wait for result, instead returns a (matcher routine) tuple.
        Useful for advanced delegates (e.g. delegate multiple subprocesses in the same time).
        This is NOT a coroutine method.
        
        :param subprocess: a coroutine
        
        :param container: container in which to start the routine
        
        :param retnames: get return values from keys. '' for the return value (for compatibility with earlier versions)
        
        :returns: (matcher, routine) where matcher is a event matcher to get the delegate result, routine is the created routine
        '''
        async def delegateroutine():
            try:
                r = await subprocess
            except:
                _, val, _ = sys.exc_info()
                e = RoutineControlEvent(RoutineControlEvent.DELEGATE_FINISHED, container.currentroutine, exception = val)
                container.scheduler.emergesend(e)
                raise
            else:
                e = RoutineControlEvent(RoutineControlEvent.DELEGATE_FINISHED, container.currentroutine,
                                        result = tuple(r if n == '' else getattr(container, n, None)
                                                       for n in retnames))
                await container.waitForSend(e)
        r = container.subroutine(generatorwrapper(delegateroutine(), 'subprocess', 'delegate'), True)
        return (RoutineControlEvent.createMatcher(RoutineControlEvent.DELEGATE_FINISHED, r), r)
    
    beginDelegateOther = begin_delegate_other
    
    async def delegate_other(self, subprocess, container, retnames = ('',), forceclose = False):
        '''
        DEPRECATED Another format of delegate allows delegate a subprocess in another container, and get some returning values
        the subprocess is actually running in 'container'. ::
        
            ret = await self.delegate_other(c.method(), c)
        
        :return: a tuple for retnames values
        
        '''
        finish, r = self.beginDelegateOther(subprocess, container, retnames)
        return await self.end_delegate(finish, r, forceclose)

    delegateOther = delegate_other
    
    async def execute_all_with_names(self, subprocesses, container = None, retnames = ('',), forceclose = True):
        '''
        DEPRECATED Execute all subprocesses and get the return values.
        
        :param subprocesses: sequence of subroutines (coroutines)
        
        :param container: if specified, run subprocesses in another container.
        
        :param retnames: DEPRECATED get return value from container.(name) for each name in retnames.
                         '' for return value (to be compatible with earlier versions)
        
        :param forceclose: force close the routines on exit, so all the subprocesses are terminated
                           on timeout if used with executeWithTimeout
        
        :returns: a list of tuples, one for each subprocess, with value of retnames inside:
                  `[('retvalue1',),('retvalue2',),...]`
        '''
        if not subprocesses:
            return []
        subprocesses = list(subprocesses)
        if len(subprocesses) == 1 and (container is None or container is self) and forceclose:
            # Directly run the process to improve performance
            return [await subprocesses[0]]
        if container is None:
            container = self
        delegates = [self.begin_delegate_other(p, container, retnames) for p in subprocesses]
        matchers = [d[0] for d in delegates]
        try:
            _, eventdict = await self.wait_for_all(*matchers)
            events = [eventdict[m] for m in matchers]
            exceptions = [e.exception for e in events if hasattr(e, 'exception')]
            if exceptions:
                if len(exceptions) == 1:
                    raise exceptions[0]
                else:
                    raise MultipleException(exceptions)
            return [e.result for e in events]
        finally:
            if forceclose:
                for d in delegates:
                    try:
                        container.terminate(d[1])
                    except Exception:
                        pass
    
    executeAll = execute_all_with_names
    
    async def execute_all(self, subprocesses, forceclose=True):
        '''
        Execute all subprocesses and get the return values.
        
        :param subprocesses: sequence of subroutines (coroutines)
        
        :param forceclose: force close the routines on exit, so all the subprocesses are terminated
                           on timeout if used with executeWithTimeout
        
        :returns: a list of return values for each subprocess
        '''
        if not subprocesses:
            return []
        subprocesses = list(subprocesses)
        if len(subprocesses) == 1 and forceclose:
            return [await subprocesses[0]]
        delegates = [self.begin_delegate(p) for p in subprocesses]
        matchers = [d[0] for d in delegates]
        try:
            _, eventdict = await self.wait_for_all(*matchers)
            events = [eventdict[m] for m in matchers]
            exceptions = [e.exception for e in events if hasattr(e, 'exception')]
            if exceptions:
                if len(exceptions) == 1:
                    raise exceptions[0]
                else:
                    raise MultipleException(exceptions)
            return [e.result for e in events]
        finally:
            if forceclose:
                for d in delegates:
                    try:
                        d[1].close()
                    except Exception:
                        pass
    
    @classmethod
    def get_container(cls, scheduler):
        """
        Create temporary instance for helper functions
        """
        if scheduler in cls._container_cache:
            return cls._container_cache[scheduler]
        else:
            c = cls(scheduler)
            cls._container_cache[scheduler] = c
            return c
    
    @classmethod
    def destroy_container_cache(cls, scheduler):
        """
        Remove cached container
        """
        if scheduler in cls._container_cache:
            del cls._container_cache[scheduler]
