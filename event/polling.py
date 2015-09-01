'''
Created on 2015/6/17

@author: hubo
'''
from __future__ import print_function, absolute_import, division
import select
from .core import PollEvent, SystemControlLowPriorityEvent
import errno

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
        def register(self, fd, options, daemon = False):
            self.epoll.register(fd, options & self.mask | self.defaultoption)
            if not daemon:
                self.socketCounter+=1
        def unregister(self, fd, daemon = False):
            try:
                self.epoll.unregister(fd)
                if not daemon:
                    self.socketCounter-=1
            except IOError:
                return
            except ValueError:
                if not daemon:
                    self.socketCounter-=1
                return
        def modify(self, fd, options):
            self.epoll.modify(fd, options & self.mask | self.defaultoption)
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
            try:
                if wait is None:
                    events = self.epoll.poll()
                else:
                    events = self.epoll.poll(wait)
                for fd, e in events:
                    if e & epwrite:
                        ret.append(PollEvent(fd, PollEvent.WRITE_READY, e & epwrite))
                    if e & epread:
                        ret.append(PollEvent(fd, PollEvent.READ_READY, e & epread))
                    if e & eperr:
                        ret.append(PollEvent(fd, PollEvent.ERROR, e & eperr))
                    if e & ephup:
                        ret.append(PollEvent(fd, PollEvent.HANGUP, e & ephup))
                if not ret and generateFree:
                    ret.append(SystemControlLowPriorityEvent(SystemControlLowPriorityEvent.FREE))
                return (ret, False)
            except IOError as exc:
                if exc.args[0] == errno.EINTR:
                    return ([], False)
