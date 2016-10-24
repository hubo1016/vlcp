'''
Created on 2015/11/26

:author: hubo
'''

import zlib
import codecs
from .gzipheader import header, tail
import time

def unicode_encoder(encoding, errors = 'strict'):
    return codecs.getincrementalencoder(encoding)(errors).encode

def unicode_decoder(encoding, errors = 'strict'):
    return codecs.getincrementaldecoder(encoding)(errors).decode

def donothing_encoder(x, iseof):
    return x

if str is bytes:
    def str_encoder(encoding, errors = 'strict'):
        return donothing_encoder
    def str_decoder(encoding, errors = 'strict'):
        return donothing_encoder
else:
    str_encoder = unicode_encoder
    str_decoder = unicode_decoder

def deflate_encoder(level = None):
    if level is None:
        obj = zlib.compressobj()
    else:
        obj = zlib.compressobj(level)
    def enc(data, final):
        ret = obj.compress(data)
        if final:
            ret += obj.flush()
        return ret
    return enc

def deflate_decoder(wbits = None):
    if wbits is None:
        obj = zlib.decompressobj()
    else:
        obj = zlib.decompressobj(wbits)
    def enc(data, final):
        ret = obj.decompress(data)
        if final:
            ret += obj.flush()
        return ret
    return enc

def _tobytes(s, encoding = 'utf-8'):
    if s is bytes:
        return s
    else:
        return s.encode(encoding)

class GzipEncoder(object):
    def __init__(self, fname = 'tmp', level = 9):
        self.crc = 0
        self.size = 0
        self.fname = _tobytes(fname)
        self.level = level
        self.writeheader = False
    def enc(self, data, final):
        buf = []
        if not self.writeheader:
            h = header.new()
            h.mtime = int(time.time())
            h.fname = self.fname
            buf.append(header.tobytes(h))
            self.compobj = zlib.compressobj(self.level, zlib.DEFLATED, -zlib.MAX_WBITS, zlib.DEF_MEM_LEVEL)
            self.writeheader = True
        buf.append(self.compobj.compress(data))
        self.crc = zlib.crc32(data, self.crc) & 0xffffffff
        self.size += len(data)
        if final:
            buf.append(self.compobj.flush())
            t = tail.new()
            t.crc32 = self.crc
            t.isize = self.size
            buf.append(tail.tobytes(t))
        return b''.join(buf)
        
def gzip_encoder(fname = 'tmp', level = 9):
    return GzipEncoder(fname, level).enc

class GzipDecoder(object):
    def __init__(self):
        self.crc = 0
        self.size = 0
        self.readheader = True
        self.readtail = False
        self.buffer = b''
    def enc(self, data, final):
        buf = []
        self.buffer += data
        while True:
            if self.readheader:
                r = header.parse(self.buffer)
                if r is None:
                    break
                h, size = r
                if h.id1 != 0x1f or h.id2 != 0x8b or h.cm != 8:
                    raise ValueError('Unsupported format')
                self.decompobj = zlib.decompressobj(-zlib.MAX_WBITS)
                self.buffer = self.buffer[size:]
                self.readheader = False
            elif self.readtail:
                r = tail.parse(self.buffer)
                if r is None:
                    break
                t, size = r
                if t.crc32 != self.crc or t.isize != self.size:
                    raise ValueError('Checksum not met')
                self.crc = 0
                self.size = 0
                self.buffer = self.buffer[size:]
                self.readtail = False
                self.readheader = True
            else:
                newdata = self.decompobj.decompress(self.buffer)
                self.crc = zlib.crc32(newdata, self.crc) & 0xFFFFFFFF
                self.size += len(newdata)
                buf.append(newdata)
                if self.decompobj.unused_data:
                    self.buffer = self.decompobj.unused_data
                    newdata = self.decompobj.flush()
                    self.crc = zlib.crc32(newdata, self.crc) & 0xFFFFFFFF
                    self.size += len(newdata)
                    buf.append(newdata)
                    self.readtail = True
                else:
                    self.buffer = b''
                    break
        if final:
            if self.buffer or not self.readheader:
                raise ValueError('Unexpected EOF')
        return b''.join(buf)

def gzip_decoder():
    return GzipDecoder().enc
