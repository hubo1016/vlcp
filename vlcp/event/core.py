'''
Created on 2015/6/12

:author: hubo
'''
from __future__ import print_function, absolute_import, division 
from .matchtree import MatchTree, EventTree
from .pqueue import CBQueue
from .event import Event,withIndices
from time import time, sleep
from datetime import datetime
from signal import signal, SIGTERM, SIGINT
from logging import getLogger, WARNING
from collections import deque
from threading import Lock
from vlcp.utils.indexedheap import IndexedHeap

import sys


POLLING_IN = 1
POLLING_PRI = 2
POLLING_OUT = 4
POLLING_ERR = 8
POLLING_HUP = 16

@withIndices('fileno', 'category', 'detail')
class PollEvent(Event):
    READ_READY = 1
    WRITE_READY = 4
    ERROR = 8
    HANGUP = 16

@withIndices('type')
class SystemControlEvent(Event):
    QUIT = 'quit'
    INIT = 'init'
    CONTINUE = 'continue'

@withIndices('type')
class SystemControlLowPriorityEvent(Event):
    FREE = 'free'
    LOOP = 'loop'

    
class QuitException(BaseException):
    pass

@withIndices('handle')
class TimerEvent(Event):
    pass

@withIndices()
class SyscallReturnEvent(Event):
    pass

class InterruptedBySignalException(Exception):
    "In Python 3.x, we must raise an exception to interrupt the polling, or it will be automatically retried"
    pass

class Scheduler(object):
    '''
    Event-driven scheduler
    '''
    class TimerHandle(object):
        def __init__(self, timestamp, interval):
            self.timestamp = timestamp
            self.interval = interval
    class MockPolling(object):
        def register(self, fd):
            pass
        def unregister(self, fd):
            pass
        def setdaemon(self, fd, daemon):
            pass
        def pollEvents(self, wait):
            free = False
            if wait is None:
                return ((), True)
            if wait > 10:
                wait = 10
                free = True
            sleep(wait)
            if free:
                return ((SystemControlLowPriorityEvent(SystemControlLowPriorityEvent.FREE),),True)
            else:
                return ((),True)
    logger = getLogger(__name__ + '.Scheduler')
    def __init__(self, polling = None, processevents = None, queuedefault = None, queuemax = None, defaultQueueClass = CBQueue.FifoQueue,
                 defaultQueuePriority = 0):
        '''
        Constructor
        
        :param polling: a polling source to retrieve events
        
        :param processevents: max events processed before starting another poll
        
        :param queuedefault: max length of default queue
        
        :param queuemax: total max length of the event queue
        
        :param defaultQueueClass: default queue class, see CBQueue
        
        :param defaultQueuePriority: default queue priority, see CBQueue
        '''
        self.matchtree = MatchTree()
        self.eventtree = EventTree()
        self.queue = CBQueue(None, None, queuedefault, queuemax, defaultQueueClass=defaultQueueClass, defaultQueuePriority=defaultQueuePriority)
        self.polling = polling
        if polling is None:
            # test only
            self.polling = self.MockPolling()
        self.timers = IndexedHeap()
        self.quitsignal = False
        self.quitting = False
        self.generatecontinue = False
        self.registerIndex = {}
        self.daemons = set()
        self.processevents = processevents
        #self.logger.setLevel(WARNING)
        self.debugging = False
    def register(self, matchers, runnable):
        '''
        Register an iterator(runnable) to scheduler and wait for events
        
        :param matchers: sequence of EventMatchers
        
        :param runnable: an iterator that accept send method
        
        :param daemon: if True, the runnable will be registered as a daemon.
        '''
        if getattr(self, 'syscallfunc', None) is not None and getattr(self, 'syscallrunnable', None) is None:
            # Inject this register
            self.syscallrunnable = runnable
        else:
            for m in matchers:
                self.matchtree.insert(m, runnable)
                events = self.eventtree.findAndRemove(m)
                for e in events:
                    self.queue.unblock(e)
                if m.indices[0] == PollEvent._classname0 and len(m.indices) >= 2:
                    self.polling.onmatch(m.indices[1], None if len(m.indices) <= 2 else m.indices[2], True)
            self.registerIndex[runnable] =self.registerIndex.get(runnable, set()).union(matchers)
    def unregister(self, matchers, runnable):
        '''
        Unregister an iterator(runnable) and stop waiting for events
        
        :param matchers: sequence of EventMatchers
        
        :param runnable: an iterator that accept send method
        '''
        for m in matchers:
            self.matchtree.remove(m, runnable)
            if m.indices[0] == PollEvent._classname0 and len(m.indices) >= 2:
                self.polling.onmatch(m.indices[1], None if len(m.indices) <= 2 else m.indices[2], False)
        self.registerIndex[runnable] =self.registerIndex.get(runnable, set()).difference(matchers)
    def unregisterall(self, runnable):
        '''
        Unregister all matches and detach the runnable. Automatically called when runnable returns StopIteration.
        '''
        if runnable in self.registerIndex:
            for m in self.registerIndex[runnable]:
                self.matchtree.remove(m, runnable)
                if m.indices[0] == PollEvent._classname0 and len(m.indices) >= 2:
                    self.polling.onmatch(m.indices[1], None if len(m.indices) <= 2 else m.indices[2], False)
            del self.registerIndex[runnable]
            self.daemons.discard(runnable)
    def ignore(self, matcher):
        '''
        Unblock and ignore the matched events, if any. 
        '''
        events  = self.eventtree.findAndRemove(matcher)
        for e in events:
            self.queue.unblock(e)
            e.canignore = True
    def quit(self, daemononly = False):
        '''
        Send quit event to quit the main loop
        '''
        if not self.quitting:
            self.quitting = True
            self.queue.append(SystemControlEvent(SystemControlEvent.QUIT, daemononly = daemononly), True)
    def send(self, event):
        '''
        Send a new event to the main queue. If the queue or sub-queue is full, return a wait event
        
        :returns: None if succeeded. Matcher for QueueCanWriteEvent if sub-queue is full.
        '''
        return self.queue.append(event, False)
    def emergesend(self, event):
        '''
        Force send a new event to the main queue.
        '''
        return self.queue.append(event, True)
    def setTimer(self, start, interval = None):
        '''
        Generate a TimerEvent on specified time
        
        :param start: offset for time from now (seconds), or datetime for a fixed time
        
        :param interval: if not None, the timer is regenerated by interval seconds.
        
        :returns: a timer handle to wait or cancel the timer
        '''
        if isinstance(start, datetime):
            timestamp = (start - datetime.fromtimestamp(0)).total_seconds()
        else:
            timestamp = time() + start
        if interval is not None:
            if not (interval > 0):
                raise ValueError('interval must be positive.')
        th = self.TimerHandle(timestamp, interval)
        self.timers.push(th, timestamp)
        return th
    def cancelTimer(self, timer):
        '''
        Cancel the timer
        
        :param timer: the timer handle
        '''
        try:
            self.timers.remove(timer)
        except IndexError:
            return
    def registerPolling(self, fd, options = POLLING_IN|POLLING_OUT, daemon = False):
        '''
        register a polling file descriptor
        
        :param fd: file descriptor or socket object
        
        :param options: bit mask flags. Polling object should ignore the incompatible flag.
        '''
        self.polling.register(fd, options, daemon)
    def modifyPolling(self, fd, options):
        '''
        modify options of a registered file descriptor
        '''
        self.polling.modify(fd, options)
    def setPollingDaemon(self, fd, daemon = True):
        self.polling.setdaemon(fd, daemon)
    def unregisterPolling(self, fd, daemon = False):
        '''
        Unregister a polling file descriptor
        
        :param fd: file descriptor or socket object
        '''
        self.polling.unregister(fd, daemon)
    def wantContinue(self):
        '''
        The next main loop will generate a SystemControlEvent('continue'), allowing time-consuming jobs
        to suspend and let other threads do their work
        '''
        self.generatecontinue = True        
    def setDaemon(self, runnable, isdaemon, noregister = False):
        '''
        If a runnable is a daemon, it will not keep the main loop running. The main loop will end when all alived runnables are daemons.
        '''
        if not noregister and runnable not in self.registerIndex:
            self.register((), runnable)
        if isdaemon:
            self.daemons.add(runnable)
        else:
            self.daemons.discard(runnable)
    def syscall(self, func):
        '''
        Call the func in core context (main loop).
        
        func should like::
        
            def syscall_sample(scheduler, processor):
                something...

        where processor is a function which accept an event. When calling processor, scheduler directly process this event without
        sending it to queue.
        
        An event matcher is returned to the caller, and the caller should wait for the event immediately to get the return value from the system call.
        The SyscallReturnEvent will have 'retvalue' as the return value, or 'exception' as the exception thrown: (type, value, traceback)
        
        :param func: syscall function
        
        :returns: an event matcher to wait for the SyscallReturnEvent. If None is returned, a syscall is already scheduled;
                  return to core context at first.
        
        '''
        if getattr(self, 'syscallfunc', None) is not None:
            return None
        self.syscallfunc = func
        self.syscallmatcher = SyscallReturnEvent.createMatcher()
        return self.syscallmatcher
    def _quitsignal(self, signum, frame):
        self.quitsignal = signum
        if getattr(self.polling, 'shouldraise', False):
            self.polling.shouldraise = False
            raise InterruptedBySignalException()
    def _tracesignal(self, signum, frame):
        import traceback
        self.logger.warning('Signal USR1, trace:\n%s', ''.join(traceback.format_stack()))
    def main(self, installsignal = True, sendinit = True):
        '''
        Start main loop
        '''
        if installsignal:
            sigterm = signal(SIGTERM, self._quitsignal)
            sigint = signal(SIGINT, self._quitsignal)
            try:
                from signal import SIGUSR1
                sigusr1 = signal(SIGUSR1, self._tracesignal)
            except Exception:
                pass
        try:
            if sendinit:
                self.queue.append(SystemControlEvent(SystemControlEvent.INIT), True)

            def processSyscall():
                while self.syscallfunc is not None:
                    r = getattr(self, 'syscallrunnable', None)
                    if r is None:
                        self.syscallfunc = None
                        break
                    try:
                        try:
                            retvalue = self.syscallfunc(self, processEvent)
                        except:
                            (t, v, tr) = sys.exc_info()
                            self.syscallfunc = None
                            self.syscallrunnable = None
                            r.send((SyscallReturnEvent(exception=(t, v, tr)), self.syscallmatcher))
                        else:
                            self.syscallfunc = None
                            self.syscallrunnable = None
                            r.send((SyscallReturnEvent(retvalue=retvalue), self.syscallmatcher))
                    except StopIteration:
                        self.unregisterall(r)
                    except QuitException:
                        self.unregisterall(r)
                    except:
                        self.logger.exception('processing syscall failed with exception')
                        self.unregisterall(r)

            def processEvent(event, emptys = ()):
                if self.debugging:
                    self.logger.debug('Processing event %s', repr(event))
                runnables = self.matchtree.matchesWithMatchers(event)
                for r, m in runnables:
                    try:
                        self.syscallfunc = None
                        self.syscallrunnable = None
                        if self.debugging:
                            self.logger.debug('Send event to %r, matched with %r', r, m)
                        r.send((event, m))
                    except StopIteration:
                        self.unregisterall(r)
                    except QuitException:
                        self.unregisterall(r)
                    except:
                        self.logger.exception('processing event %s failed with exception', repr(event))
                        self.unregisterall(r)
                    processSyscall()
                if not event.canignore and not event.canignorenow():
                    self.eventtree.insert(event)
                    self.queue.block(event, emptys)
                else:
                    for e in emptys:
                        processEvent(e)
            canquit = False
            self.logger.info('Main loop started')
            quitMatcher = SystemControlEvent.createMatcher(type=SystemControlEvent.QUIT)
            while len(self.registerIndex) > len(self.daemons):
                if self.debugging:
                    self.logger.debug('Blocked events: %d', len(self.queue.blockEvents))
                    self.logger.debug('Blocked events list: %r', list(self.queue.blockEvents.keys()))
                    if self.quitting:
                        self.logger.debug('Routines still not quit: %r', list(self.registerIndex.keys()))
                if self.quitsignal:
                    self.quit()
                if canquit and not self.queue.canPop() and not self.timers:
                    if self.quitting:
                        break
                    else:
                        self.quit(True)
                self.queue.append(SystemControlLowPriorityEvent(SystemControlLowPriorityEvent.LOOP), True)
                processedEvents = 0
                while self.queue.canPop() and (self.processevents is None or processedEvents < self.processevents):
                    e, qes, emptys = self.queue.pop()
                    # Queue events will not enqueue again
                    if not e.canignore and not e.canignorenow():
                        # The event might block, must process it first
                        processEvent(e, emptys)
                        for qe in qes:
                            processEvent(qe)
                    else:
                        for qe in qes:
                            processEvent(qe)
                        processEvent(e, emptys)
                    if quitMatcher.isMatch(e):
                        if e.daemononly:
                            runnables = list(self.daemons)
                        else:
                            runnables = list(self.registerIndex.keys())
                        for r in runnables:
                            try:
                                r.throw(QuitException)
                            except StopIteration:
                                self.unregisterall(r)
                            except QuitException:
                                self.unregisterall(r)
                            except:
                                self.logger.exception('Runnable quit failed with exception')
                                self.unregisterall(r)
                            processSyscall()
                    processedEvents += 1
                if len(self.registerIndex) <= len(self.daemons):
                    break
                if self.generatecontinue or self.queue.canPop():
                    wait = 0
                elif not self.timers:
                    wait = None
                else:
                    wait = self.timers.top().timestamp - time()
                    if wait < 0:
                        wait = 0
                events, canquit = self.polling.pollEvents(wait)
                for e in events:
                    self.queue.append(e, True)
                now = time() + 0.1
                while self.timers and self.timers.topPriority() < now:
                    t = self.timers.top()
                    if t.interval is not None:
                        t.timestamp += t.interval
                        self.timers.setpriority(t, t.timestamp)
                    else:
                        self.timers.pop()
                    self.queue.append(TimerEvent(t), True)
                if self.generatecontinue:
                    self.queue.append(SystemControlEvent(SystemControlEvent.CONTINUE), True)
                    self.generatecontinue = False
            if self.registerIndex:
                if len(self.registerIndex) > len(self.daemons):
                    self.logger.warning('Some runnables are not quit, doing cleanup')
                    self.logger.warning('Runnables list: %r', set(self.registerIndex.keys()).difference(self.daemons))
                for r in list(self.registerIndex.keys()):
                    try:
                        r.close()
                    except:
                        self.logger.exception('Runnable quit failed with exception')
                    finally:
                        self.unregisterall(r)
            self.logger.info('Main loop quit normally')
        finally:
            if installsignal:
                signal(SIGTERM, sigterm)
                signal(SIGINT, sigint)
                try:
                    signal(SIGUSR1, sigusr1)
                except Exception:
                    pass

def syscall_direct(*events):
    '''
    Directly process these events. This should never be used for normal events.
    '''
    def _syscall(scheduler, processor):
        for e in events:
            processor(e)
    return _syscall

def syscall_generator(generator):
    '''
    Directly process events from a generator function. This should never be used for normal events.
    '''
    def _syscall(scheduler, processor):
        for e in generator():
            processor(e)
    return _syscall

def syscall_clearqueue(queue):
    '''
    Clear a queue
    '''
    def _syscall(scheduler, processor):
        qes, qees = queue.clear()
        events = scheduler.queue.unblockqueue(queue)
        for e in events:
            scheduler.eventtree.remove(e)
        for e in qes:
            processor(e)
        for e in qees:
            processor(e)
    return _syscall

def syscall_removequeue(queue, index):
    '''
    Remove subqueue `queue[index]` from queue.
    '''
    def _syscall(scheduler, processor):
        events = scheduler.queue.unblockqueue(queue[index])
        for e in events:
            scheduler.eventtree.remove(e)
        qes, qees = queue.removeSubQueue(index)
        for e in qes:
            processor(e)
        for e in qees:
            processor(e)
    return _syscall

def syscall_clearremovequeue(queue, index):
    '''
    Clear the subqueue `queue[index]` and remove it from queue.
    '''
    def _syscall(scheduler, processor):
        qes, qees = queue[index].clear()
        events = scheduler.queue.unblockqueue(queue[index])
        for e in events:
            scheduler.eventtree.remove(e)
        qes2, qees2 = queue.removeSubQueue(index)
        for e in qes:
            processor(e)
        for e in qes2:
            processor(e)
        for e in qees:
            processor(e)
        for e in qees2:
            processor(e)
    return _syscall
