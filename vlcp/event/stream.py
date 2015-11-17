'''
Created on 2015/8/14

@author: hubo
'''

from vlcp.event import Event, withIndices

@withIndices('stream', 'type')
class StreamDataEvent(Event):
    canignore = False
    STREAM_DATA = 'data'
    STREAM_EOF = 'eof'
    STREAM_ERROR = 'error'
    def canignorenow(self):
        return self.stream.closed

class Stream(object):
    '''
    Streaming data with events
    '''
    def __init__(self, isunicode = False, encoders = [], writebufferlimit = 4096, splitsize = 1048576):
        '''
        Constructor
        '''
        self.data = b''
        self.pos = 0
        self.dataeof = False
        self.dataerror = False
        self.eof = False
        self.error = False
        self.closed = False
        self.writeclosed = False
        self.allowwrite = False
        self.dm = StreamDataEvent.createMatcher(self, None)
        self.isunicode = isunicode
        self.encoders = encoders
        self.writebuffer = []
        self.writebuffersize = 0
        self.writebufferlimit = writebufferlimit
        self.splitsize = splitsize
    def getEncoderList(self):
        return self.encoders
    def _parsedata(self, event):
        event.canignore = True
        data = event.data
        self.pos = 0
        self.dataeof = event.type == StreamDataEvent.STREAM_EOF
        self.dataerror = event.type == StreamDataEvent.STREAM_ERROR
        if len(data) > 0 or self.dataeof:
            for enc in self.encoders:
                try:
                    data = enc(data, self.dataeof)
                except:
                    self.error = True
                    self.closed = True
                    raise
        self.data = data
    def read(self, container, size = None):
        ret = []
        retsize = 0
        if self.eof:
            raise EOFError
        if self.error:
            raise IOError('Stream is broken before EOF')
        while size is None or retsize < size:
            if self.pos >= len(self.data):
                for m in self.prepareRead(container):
                    yield m
            if size is None or size - retsize > len(self.data) - self.pos:
                t = self.data[self.pos:]
                ret.append(t)
                retsize += len(t)
                self.pos = len(self.data)
                if self.dataeof:
                    self.eof = True
                    break
                if self.dataerror:
                    self.error = True
                    break
            else:
                ret.append(self.data[self.pos:self.pos + (size - retsize)])
                self.pos += (size - retsize)
                retsize = size
                break
        if self.isunicode:
            container.data = u''.join(ret)
        else:
            container.data = b''.join(ret)
        if self.error:
            raise IOError('Stream is broken before EOF')
    def readonce(self, size = None):
        if self.eof:
            raise EOFError
        if self.error:
            raise IOError('Stream is broken before EOF')
        if size is not None and size < len(self.data) - self.pos:
            ret = self.data[self.pos: self.pos + size]
            self.pos += size
            return ret
        else:
            ret = self.data[self.pos:]
            self.pos = len(self.data)
            if self.dataeof:
                self.eof = True
            return ret
    def prepareRead(self, container):
        if not self.eof and not self.error and self.pos >= len(self.data):
            yield (self.dm,)
            self._parsedata(container.event)
    def readline(self, container, size = None):
        ret = []
        retsize = 0
        if self.eof:
            raise EOFError
        if self.error:
            raise IOError('Stream is broken before EOF')
        while size is None or retsize < size:
            if self.pos >= len(self.data):
                for m in self.prepareRead(container):
                    yield m
            if size is None or size - retsize > len(self.data) - self.pos:
                t = self.data[self.pos:]
                if self.isunicode:
                    p = t.find(u'\n')
                else:
                    p = t.find(b'\n')
                if p >= 0:
                    t = t[0: p + 1]
                    ret.append(t)
                    self.retsize += len(t)
                    self.pos += len(t)
                    break
                else:
                    ret.append(t)
                    self.retsize += len(t)
                    self.pos += len(t)
                    if self.dataeof:
                        self.eof = True
                        break
                    if self.dataerror:
                        self.error = True
                        break
            else:
                t = self.data[self.pos:self.pos + (size - retsize)]
                if self.isunicode:
                    p = t.find(u'\n')
                else:
                    p = t.find(b'\n')
                if p >= 0:
                    t = t[0: p + 1]
                ret.append(t)
                self.pos += len(t)
                retsize += len(t)
                break
        if self.isunicode:
            container.data = u''.join(ret)
        else:
            container.data = b''.join(ret)
        if self.error:
            raise IOError('Stream is broken before EOF')
    def write(self, data, container, eof = False, ignoreexception = False, buffering = True, split = True):
        if self.closed or self.writeclosed:
            if not ignoreexception and not self.allowwrite:
                raise IOError('Stream is closed')
        else:
            if eof:
                buffering = False
            if buffering and self.writebufferlimit is not None and self.writebuffersize + len(data) > self.writebufferlimit:
                buffering = False
            if buffering:
                self.writebuffer.append(data)
                self.writebuffersize += len(data)
            else:
                data = data[0:0].join(self.writebuffer) + data
                del self.writebuffer[:]
                self.writebuffersize = 0
                if split:
                    for i in range(0, len(data), self.splitsize):
                        for m in container.waitForSend(StreamDataEvent(self,
                                                                       StreamDataEvent.STREAM_EOF if eof and i + self.splitsize >= len(data) else StreamDataEvent.STREAM_DATA,
                                                                       data = data[i : i + self.splitsize])):
                            yield m                    
                else:
                    for m in container.waitForSend(StreamDataEvent(self,
                                                                   StreamDataEvent.STREAM_EOF if eof else StreamDataEvent.STREAM_DATA,
                                                                   data = data)):
                        yield m
                if eof:
                    self.writeclosed = True
    def error(self, container, ignoreexception = False):
        if self.closed or self.writeclosed:
            if not ignoreexception and not self.allowwrite:
                raise IOError('Stream is closed')
        else:
            for m in container.waitForSend(StreamDataEvent(self, StreamDataEvent.STREAM_ERROR, data = b'')):
                yield m
            self.writeclosed = True
    def close(self, scheduler, allowwrite = False):
        self.closed = True
        self.allowwrite = allowwrite
        scheduler.ignore(self.dm)
    def copyTo(self, dest, container):
        if self.eof:
            for m in dest.write(u'' if self.isunicode else b'', True):
                yield m
        elif self.error:
            for m in dest.error(container):
                yield m
        else:
            try:
                while not self.eof:
                    for m in self.prepareRead(container):
                        yield m
                    data = self.readonce()
                    try:
                        for m in dest.write(data, container, self.eof):
                            yield m
                    except IOError:
                        break
            except:
                try:
                    for m in dest.error(container):
                        yield m
                except IOError:
                    pass
                raise
            finally:
                self.close(container.scheduler)

class MemoryStream(object):
    '''
    A stream with readonly data
    '''
    def __init__(self, data, encoders= []):
        '''
        Constructor
        '''
        self.data = b''
        self.preloaddata = data
        self.pos = 0
        self.dataeof = False
        self.dataerror = False
        self.eof = False
        self.error = False
        self.closed = True
        self.isunicode = not isinstance(data, bytes)
        self.encoders = encoders
    def __len__(self):
        return len(self.preloaddata)
    def getEncoderList(self):
        return self.encoders
    def _parsedata(self):
        data = self.preloaddata
        self.pos = 0
        self.dataeof = True
        self.dataerror = False
        for enc in self.encoders:
            try:
                data = enc(data, self.dataeof)
            except:
                self.error = True
                self.closed = True
                raise
        self.data = data
    def read(self, container, size = None):
        if self.eof:
            raise EOFError
        if self.error:
            raise IOError('Stream is broken before EOF')
        if not self.dataeof:
            self._parsedata()
        if size is None or size > len(self.data) - self.pos:
            t = self.data[self.pos:]
            self.pos = len(self.data)
            self.eof = True
        else:
            t = self.data[self.pos:self.pos + size]
            self.pos += size
        container.data = t
        if False:
            yield
    def readonce(self, size = None):
        if self.eof:
            raise EOFError
        if self.error:
            raise IOError('Stream is broken before EOF')
        if size is not None and size < len(self.data) - self.pos:
            ret = self.data[self.pos: self.pos + size]
            self.pos += size
            return ret
        else:
            ret = self.data[self.pos:]
            self.pos = len(self.data)
            if self.dataeof:
                self.eof = True
            return ret
    def prepareRead(self, container):
        if not self.eof and not self.error and self.pos >= len(self.data):
            self._parsedata()
        if False:
            yield
    def readline(self, container, size = None):
        if self.eof:
            raise EOFError
        if self.error:
            raise IOError('Stream is broken before EOF')
        if not self.dataeof:
            self._parsedata()
        if size is None or size > len(self.data) - self.pos:
            t = self.data[self.pos:]
            if self.isunicode:
                p = t.find(u'\n')
            else:
                p = t.find(b'\n')
            if p >= 0:
                t = t[0: p + 1]
            self.pos += len(t)
            if self.pos >= len(self.data):
                self.eof = True
        else:
            t = self.data[self.pos:self.pos + size]
            if self.isunicode:
                p = t.find(u'\n')
            else:
                p = t.find(b'\n')
            if p >= 0:
                t = t[0: p + 1]
            self.pos += len(t)
        container.data = t
        if False:
            yield
    def write(self, data, container, eof = False, ignoreexception = False):
        if not ignoreexception:
            raise IOError('Stream is closed')
        if False:
            yield
    def error(self, container, ignoreexception = False):
        if not ignoreexception:
            raise IOError('Stream is closed')
        if False:
            yield
    def close(self, scheduler = None, allowwrite = False):
        self.closed = True
    def copyTo(self, dest, container):
        if self.eof:
            for m in dest.write(u'' if self.isunicode else b'', True):
                yield m
        elif self.error:
            for m in dest.error(container):
                yield m
        else:
            try:
                while not self.eof:
                    for m in self.prepareRead(container):
                        yield m
                    data = self.readonce()
                    try:
                        for m in dest.write(data, container, self.eof):
                            yield m
                    except IOError:
                        break
            except:
                try:
                    for m in dest.error(container):
                        yield m
                except IOError:
                    pass
                raise
            finally:
                self.close(container.scheduler)
    
