'''
Created on 2015/6/17

:author: hubo
'''
from __future__ import print_function, absolute_import, division
import select
from vlcp.event import PollEvent, SystemControlLowPriorityEvent
import errno
import time
import sys
from vlcp.event.core import InterruptedBySignalException

if hasattr(select, 'epoll'):
    class EPollPolling(object):
        '''
        Poll event from epoll
        '''
        def __init__(self, options = select.EPOLLET, maxwait = 60):
            '''
            Constructor
            '''
            self.epoll = select.epoll()
            self.defaultoption = options
            self.mask = select.EPOLLIN|select.EPOLLPRI|select.EPOLLOUT|\
                select.EPOLLRDNORM|select.EPOLLRDBAND|select.EPOLLWRNORM|\
                select.EPOLLWRBAND|select.EPOLLONESHOT|select.EPOLLET
            self.socketCounter = 0
            self.maxwait = maxwait
            self.daemons = set()
            self.shouldraise = False
        def register(self, fd, options, daemon = False):
            if hasattr(fd, 'fileno'):
                fd = fd.fileno()
            self.epoll.register(fd, options & self.mask | self.defaultoption)
            if not daemon:
                self.socketCounter+=1
            else:
                self.daemons.add(fd)
        def unregister(self, fd, daemon = False):
            try:
                if hasattr(fd, 'fileno'):
                    fd = fd.fileno()
                self.epoll.unregister(fd)
                daemon = fd in self.daemons
                if not daemon:
                    self.socketCounter-=1
                else:
                    self.daemons.discard(fd)
            except IOError:
                return
            except ValueError:
                if not daemon:
                    self.socketCounter-=1
                return
        def modify(self, fd, options):
            self.epoll.modify(fd, options & self.mask | self.defaultoption)
        def setdaemon(self, fd, daemon):
            if hasattr(fd, 'fileno'):
                fd = fd.fileno()
            isdaemon = fd in self.daemons
            if isdaemon != daemon:
                if daemon:
                    self.daemons.add(fd)
                    self.socketCounter -= 1
                else:
                    self.daemons.discard(fd)
                    self.socketCounter += 1
        def pollEvents(self, wait):
            ret = []
            epwrite = select.EPOLLOUT|select.EPOLLWRNORM|select.EPOLLWRBAND
            epread = select.EPOLLIN|select.EPOLLRDNORM|select.EPOLLRDBAND|select.EPOLLPRI
            eperr = select.EPOLLERR
            ephup = select.EPOLLHUP
            if self.socketCounter <= 0 and wait is None:
                return ([], True)
            generateFree = False
            if wait is None or (self.maxwait is not None and wait > self.maxwait):
                generateFree = True
                wait = self.maxwait
            events = []
            try:
                interrupted = False
                self.shouldraise = True
                if wait is None:
                    events = self.epoll.poll()
                else:
                    events = self.epoll.poll(wait)
                self.shouldraise = False
            except InterruptedBySignalException:
                interrupted = True
            except IOError as exc:
                if exc.args[0] == errno.EINTR:
                    interrupted = True
                else:
                    raise
            finally:
                self.shouldraise = False                
            for fd, e in events:
                if e & epwrite:
                    ret.append(PollEvent(fd, PollEvent.WRITE_READY, e & epwrite))
                if e & epread:
                    ret.append(PollEvent(fd, PollEvent.READ_READY, e & epread))
                if e & eperr:
                    ret.append(PollEvent(fd, PollEvent.ERROR, e & eperr))
                if e & ephup:
                    ret.append(PollEvent(fd, PollEvent.HANGUP, e & ephup))
            if not ret and generateFree and not interrupted:
                ret.append(SystemControlLowPriorityEvent(SystemControlLowPriorityEvent.FREE))
            return (ret, False)
        def onmatch(self, fileno, category, add):
            pass

class SelectPolling(object):
    '''
    Compatible event polling with select
    '''
    def __init__(self, options = 0, maxwait = 60):
        '''
        Constructor
        '''
        self.readfiles = set()
        self.writefiles = set()
        self.errorfiles = set()
        self.daemons = set()
        self.socketCounter = 0
        self.maxwait = maxwait
        self.shouldraise = False
    def register(self, fd, options, daemon = False):
        if hasattr(fd, 'fileno'):
            fd = fd.fileno()
        self.errorfiles.add(fd)
        if not daemon:
            self.socketCounter+=1
        else:
            self.daemons.add(fd)
    def unregister(self, fd, daemon = False):
        try:
            if hasattr(fd, 'fileno'):
                fd = fd.fileno()
            daemon = fd in self.daemons
            if not daemon:
                self.socketCounter-=1
            else:
                self.daemons.discard(fd)
            if hasattr(fd, 'fileno'):
                fd = fd.fileno()
            self.errorfiles.discard(fd)
            #self.readfiles.discard(fd)
            #self.writefiles.discard(fd)
        except IOError:
            return
    def modify(self, fd, options):
        pass
    def setdaemon(self, fd, daemon):
        if hasattr(fd, 'fileno'):
            fd = fd.fileno()
        isdaemon = fd in self.daemons
        if isdaemon != daemon:
            if daemon:
                self.daemons.add(fd)
                self.socketCounter -= 1
            else:
                self.daemons.discard(fd)
                self.socketCounter += 1
    def pollEvents(self, wait):
        ret = []
        if self.socketCounter <= 0 and wait is None:
            return ([], True)
        generateFree = False
        if wait is None or (self.maxwait is not None and wait > self.maxwait):
            generateFree = True
            wait = self.maxwait
        events = [[], [], []]
        try:
            interrupted = False
            self.shouldraise = True
            if wait is None:
                events = select.select(self.readfiles, self.writefiles, self.errorfiles)
            elif not self.readfiles and not self.writefiles and not self.errorfiles:
                time.sleep(wait)
            else:
                events = select.select(self.readfiles, self.writefiles, self.errorfiles, wait)
            self.shouldraise = False
        except InterruptedBySignalException:
            interrupted = True
        except IOError as exc:
            if exc.args[0] == errno.EINTR:
                interrupted = True
            else:
                raise IOError('Some of the fds are invalid, maybe some sockets are not unregistered', exc)        
        finally:
            self.shouldraise = False
        for fd in events[0]:
            ret.append(PollEvent(fd, PollEvent.READ_READY, PollEvent.READ_READY))
        for fd in events[1]:
            ret.append(PollEvent(fd, PollEvent.WRITE_READY, PollEvent.WRITE_READY))
        for fd in events[2]:
            ret.append(PollEvent(fd, PollEvent.ERROR, PollEvent.ERROR))
        if not ret and generateFree and not interrupted:
            ret.append(SystemControlLowPriorityEvent(SystemControlLowPriorityEvent.FREE))
        return (ret, False)
    def onmatch(self, fileno, category, add):
        if add:
            if category is None or category == PollEvent.READ_READY:
                self.readfiles.add(fileno)
            if category is None or category == PollEvent.WRITE_READY:
                self.writefiles.add(fileno)
        else:
            if category is None or category == PollEvent.READ_READY:
                self.readfiles.discard(fileno)
            if category is None or category == PollEvent.WRITE_READY:
                self.writefiles.discard(fileno)            
