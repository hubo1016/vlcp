'''
Created on 2015/8/14

:author: hubo
'''

from vlcp.event import Event, withIndices
import os

@withIndices('stream', 'type')
class StreamDataEvent(Event):
    canignore = False
    STREAM_DATA = 'data'
    STREAM_EOF = 'eof'
    STREAM_ERROR = 'error'
    def canignorenow(self):
        return self.stream.closed

class BaseStream(object):
    "Streaming base"
    def __init__(self, isunicode = False, encoders = []):
        '''
        Constructor
        '''
        self.data = b''
        self.pos = 0
        self.dataeof = False
        self.dataerror = False
        self.eof = False
        self.errored = False
        self.closed = False
        self.writeclosed = False
        self.allowwrite = False
        self.isunicode = isunicode
        self.encoders = list(encoders)
    def getEncoderList(self):
        return self.encoders
    def _parsedata(self, data, dataeof, dataerror):
        self.pos = 0
        self.dataeof = dataeof
        self.dataerror = dataerror
        if len(data) > 0 or self.dataeof:
            for enc in self.encoders:
                try:
                    data = enc(data, self.dataeof)
                except:
                    self.errored = True
                    self.closed = True
                    raise
        self.data = data
    def read(self, container, size = None):
        ret = []
        retsize = 0
        if self.eof:
            raise EOFError
        if self.errored:
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
                    self.errored = True
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
        if self.errored:
            raise IOError('Stream is broken before EOF')
    def readonce(self, size = None):
        if self.eof:
            raise EOFError
        if self.errored:
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
        if False:
            yield
        raise NotImplementedError
    def readline(self, container, size = None):
        ret = []
        retsize = 0
        if self.eof:
            raise EOFError
        if self.errored:
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
                        self.errored = True
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
        if self.errored:
            raise IOError('Stream is broken before EOF')
    def copyTo(self, dest, container):
        if self.eof:
            for m in dest.write(u'' if self.isunicode else b'', True):
                yield m
        elif self.errored:
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
    def write(self, data, container, eof = False, ignoreexception = False, buffering = True, split = True):
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

class Stream(BaseStream):
    '''
    Streaming data with events
    '''
    def __init__(self, isunicode = False, encoders = [], writebufferlimit = 4096, splitsize = 1048576):
        '''
        Constructor
        '''
        BaseStream.__init__(self, isunicode, encoders)
        self.dm = StreamDataEvent.createMatcher(self, None)
        self.writebuffer = []
        self.writebuffersize = 0
        self.writebufferlimit = writebufferlimit
        self.splitsize = splitsize
    def prepareRead(self, container):
        if not self.eof and not self.errored and self.pos >= len(self.data):
            yield (self.dm,)
            container.event.canignore = True
            self._parsedata(container.event.data, container.event.type == StreamDataEvent.STREAM_EOF,
                            container.event.type == StreamDataEvent.STREAM_ERROR)
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
                if not data:
                    split = False
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

class MemoryStream(BaseStream):
    '''
    A stream with readonly data
    '''
    def __init__(self, data, encoders= [], isunicode = None):
        '''
        Constructor
        '''
        BaseStream.__init__(self, not isinstance(data, bytes) if isunicode is None else isunicode, encoders)
        self.preloaddata = data
    def __len__(self):
        return len(self.preloaddata)
    def prepareRead(self, container):
        if not self.eof and not self.errored and self.pos >= len(self.data):
            self._parsedata(self.preloaddata, True, False)
        if False:
            yield

class FileStream(BaseStream):
    "A stream from a file-like object. The file-like object must be in blocking mode"
    def __init__(self, fobj, encoders = [], isunicode = None, size = None, readlimit = 65536):
        "Constructor"
        if isunicode is None:
            mode = getattr(fobj, 'mode', 'rb')
            if 'b' in mode:
                isunicode = False
            elif str is bytes:
                isunicode = False
            else:
                isunicode = True
        BaseStream.__init__(self, isunicode, encoders)
        self.fobj = fobj
        self.size = None
        try:
            self.size = os.fstat(fobj.fileno()).st_size
            self.size -= fobj.tell()
            if self.size > size:
                self.size = size
        except:
            if size:
                self.size = size
        self.readlimit = readlimit
        self.totalbuffered = 0
    def __len__(self):
        if self.size is None:
            raise TypeError('Cannot determine file size')
        else:
            return self.size
    def prepareRead(self, container):
        if not self.eof and not self.errored and self.pos >= len(self.data):
            try:
                readlimit = self.readlimit
                if self.size is not None and self.size - self.totalbuffered < readlimit:
                    readlimit = self.size - self.totalbuffered 
                data = self.fobj.read(readlimit)
                self.totalbuffered += len(data)
                eof = not data
                if self.size is not None and self.totalbuffered >= self.size:
                    eof = True
            except:
                self._parsedata(b'', False, True)
            else:
                self._parsedata(data, eof, False)
        if False:
            yield
    def close(self, scheduler=None, allowwrite=False):
        self.fobj.close()
        BaseStream.close(self, scheduler=scheduler, allowwrite=allowwrite)

class FileWriter(object):
    "Write to file"
    def __init__(self, fobj):
        self.fobj = fobj
    def write(self, data, container, eof = False, ignoreexception = False, buffering = True, split = True):
        try:
            if data:
                self.fobj.write(data)
            if eof:
                self.fobj.close()
        except:
            if not ignoreexception:
                raise
        if False:
            yield
    def error(self, container, ignoreexception = False):
        try:
            self.fobj.close()
        except:
            if not ignoreexception:
                raise
        if False:
            yield
