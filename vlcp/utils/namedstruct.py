'''
Created on 2015/7/8

@author: hubo
'''
from __future__ import print_function, absolute_import, division 
import struct
import logging
import warnings

class ParseError(ValueError):
    pass

class BadLenError(ParseError):
    pass

class BadFormatError(ParseError):
    pass

class NamedStruct(object):
    '''
    Store a binary struct message, which is serializable
    '''
    _pickleTypes = {}
    _pickleNames = {}
    _logger = logging.getLogger(__name__ + '.NamedStruct')
    def __init__(self, parser, data, inlineparent = None):
        '''
        Constructor
        '''
        self._parser = parser
        self._splitted = False
        self._splitting = False
        self._struct = data
        if inlineparent is None:
            self._target = self
        else:
            self._target = inlineparent
    def _unpack(self):
        self._logger.log(logging.DEBUG, 'unpacking %r', self)
        current = self
        while current is not None:
            current._splitting = True
            extra = current._parser.unpack(current._struct, current)
            last = current
            current = getattr(current, '_sub', None)
        last._extra = extra
        current = self
        while current is not None:
            current._splitting = False
            current._splitted = True
            current = getattr(current, '_sub', None)
    def __getattr__(self, name):
        if name.startswith('_'):
            raise AttributeError('NamedStruct object has no attribute %r', name)
        if self._target is not self:
            return getattr(self._target, name)
        if not self._splitted and not self._splitting:
            self._unpack()
            return getattr(self, name)
        else:
            raise AttributeError('%r is not defined' % (name,))
    def __setattr__(self, name, value):
        if not name.startswith('_'):
            if self is not self._target:
                setattr(self._target, name, value)
            if not self._splitted and not self._splitting:
                self._unpack()
            return object.__setattr__(self._target, name, value)
        else:
            return object.__setattr__(self, name, value)
    def _pack(self):
        self._logger.log(logging.DEBUG, 'packing %r', self)
        ps = []
        current = self
        while current is not None:
            ps.append(current._parser.pack(current))
            last = current
            current = getattr(current, '_sub', None)
        ps.append(getattr(last, '_extra', b''))
        self._struct = b''.join(ps)
    def _tobytes(self, *args, **kwargs):
        if self._splitted:
            self._pack()
        paddingSize = self._parser.paddingsize(self)
        return self._struct + b'\x00' * (paddingSize - len(self._struct))
    def _realsize(self):
        if self._splitted:
            current = self
            size= 0
            while current is not None:
                size += current._parser.sizeof(current)
                last = current
                current = getattr(current, '_sub', None)
            size += len(getattr(last, '_extra', b''))
            return size
        else:
            return len(self._struct)
    def __len__(self):
        return self._parser.paddingsize(self)
    def _update(self, data):
        self._struct = data
        self._splitted = False
        if hasattr(self, '_extra'):
            del self._extra
    def _subclass(self, parser):
        if not hasattr(self, '_extra') and not self._splitted:
            self._unpack()
        self._sub = parser.create(getattr(self, '_extra', b''), self._target)
        if hasattr(self, '_extra'):
            del self._extra
    def _autosubclass(self):
        self._parser.subclass(self)
    def _extend(self, newsub):
        current = self
        while hasattr(current, '_sub'):
            current = current._sub
        current._sub = newsub
        if hasattr(current, '_extra'):
            del current._extra
    def _gettype(self):
        current = self
        lastname = getattr(current._parser, 'typedef', None)
        while hasattr(current, '_sub'):
            current = current._sub
            tn = getattr(current._parser, 'typedef', None)
            if tn is not None:
                lastname = tn
        return lastname
    def _setextra(self, extradata):
        if not self._splitted:
            self._unpack()
        current = self
        while hasattr(current, '_sub'):
            current = current._sub
        current._extra = extradata
    def _getextra(self):
        if not self._splitted:
            self._unpack()
        current = self
        while hasattr(current, '_sub'):
            current = current._sub
        return getattr(current, '_extra', None)
    def _validate(self, recursive = True):
        if not self._splitted:
            self._unpack()
        if recursive:
            for k, v in self.__dict__.items():
                if not k.startswith('_') and isinstance(v, NamedStruct):
                    v._validate(recursive)
    def __copy__(self):
        return self._parser.create(self._tobytes(), None)
    def __deepcopy__(self, memo):
        return self._parser.create(self._tobytes(), None)
    def __repr__(self, *args, **kwargs):
        t = self._gettype()
        if t is None:
            return object.__repr__(self, *args, **kwargs)
        else:
            return '<%r at %016X>' % (t, id(self))
    def __getstate__(self):
        t = self._parser.typedef
        if t is not None and t in NamedStruct._pickleNames:
            return (self._tobytes(), NamedStruct._pickleNames[t], self._target)
        else:
            return (self._tobytes(), self._parser, self._target)
    def __setstate__(self, state):
        if not isinstance(state, tuple):
            raise ValueError('State should be a tuple')
        t = state[1]
        if t in NamedStruct._pickleTypes:
            NamedStruct.__init__(self, NamedStruct._pickleTypes[t].parser(), state[0], state[2])
        else:
            NamedStruct.__init__(self, t, state[0], state[2])
        if hasattr(self._parser, 'subclass'):
            self._parser.subclass(self)
    @staticmethod
    def _registerPickleType(name, typedef):
        NamedStruct._pickleNames[typedef] = name
        NamedStruct._pickleTypes[name] = typedef

DUMPTYPE_FLAT = 'flat'
DUMPTYPE_KEY = 'key'
DUMPTYPE_NONE = 'none'

def dump(val, humanread = True, dumpextra = False, typeinfo = DUMPTYPE_FLAT):
    if val is None:
        return val
    if isinstance(val, NamedStruct):
        if not val._splitted:
            val._unpack()
        t = val._gettype()
        if t is None:
            r = dict((k, dump(v, humanread, dumpextra, typeinfo)) for k, v in val.__dict__.items() if not k.startswith('_'))
        else:
            if humanread:
                r = t.formatdump(dict((k, dump(v, humanread, dumpextra, typeinfo)) for k, v in val.__dict__.items() if not k.startswith('_')))
                if hasattr(val, '_seqs'):
                    for s in val._seqs:
                        st = s._gettype()
                        if st is not None and hasattr(st, 'formatdump'):
                            r = st.formatdump(r)
                if hasattr(t, 'extraformatter'):
                    try:
                        r = t.extraformatter(r)
                    except:
                        NamedStruct._logger.log(logging.DEBUG, 'A formatter thrown an exception', exc_info = True)
            else:
                r = {'<' + repr(t) + '>' : dict((k, dump(v, humanread, dumpextra, typeinfo)) for k, v in val.__dict__.items() if not k.startswith('_'))}
        if dumpextra:
            extra = val._getextra()
            if extra:
                r['_extra'] = extra
        if t is not None:
            if typeinfo == DUMPTYPE_FLAT:
                r['_type'] = '<' + repr(t) + '>'
            elif typeinfo == DUMPTYPE_KEY:
                r = {'<' + repr(t) + '>' : r}
        return r
    elif isinstance(val, InlineStruct):
        return dict((k, dump(v, humanread, dumpextra, typeinfo)) for k, v in val.__dict__.items() if not k.startswith('_'))
    elif isinstance(val, list) or isinstance(val, tuple):
        return [dump(v, humanread, dumpextra, typeinfo) for v in val]
    else:
        return val

def _copy(buffer):
    try:
        if isinstance(buffer, memoryview):
            return buffer.tobytes()
        else:
            return buffer[:]
    except:
        return buffer[:]

def sizefromlen(limit, *properties):
    def func(namedstruct):
        v = namedstruct
        for p in properties:
            v = getattr(v, p)
        if v > limit:
            raise BadLenError('Struct length exceeds limit ' + str(limit))
        return v
    return func

def packsize(*properties):
    def func(namedstruct):
        v = namedstruct
        for p in properties[:-1]:
            v = getattr(v, p)
        setattr(v, properties[-1], len(namedstruct))
    return func

def packrealsize(*properties):
    def func(namedstruct):
        v = namedstruct
        for p in properties[:-1]:
            v = getattr(v, p)
        setattr(v, properties[-1], namedstruct._realsize())
    return func

def packvalue(value, *properties):
    def func(namedstruct):
        v = namedstruct
        for p in properties[:-1]:
            v = getattr(v, p)
        setattr(v, properties[-1], value)
    return func

class InlineStruct(object):
    '''
    Just a storage object
    '''
    def __init__(self, parent):
        self._parent = parent
    def __repr__(self, *args, **kwargs):
        return repr(dict((k,v) for k,v in self.__dict__ if not k.startswith('_')))
    def __setattr__(self, name, value):
        if not name.startswith('_'):
            if not self._parent._splitted and not self._parent._splitting:
                self._parent._unpack()
        return object.__setattr__(self, name, value)



def _never(namedstruct):
    return False

class Parser(object):
    logger = logging.getLogger(__name__ + '.Parser')
    def __init__(self, base = None, criteria = _never, padding = 8, initfunc = None, typedef = None, classifier = None, classifyby = None):
        self.subclasses = []
        self.subindices = {}
        self.base = base
        self.padding = padding
        self.isinstance = criteria
        self.initfunc = initfunc
        self.typedef = typedef
        self.classifier = classifier
        if self.base is not None:
            self.base.subclasses.append(self)
            if classifyby is not None:
                for v in classifyby:
                    self.base.subindices[v] = self
    def parse(self, buffer, inlineparent = None):
        if self.base is not None:
            return self.base.parse(buffer, inlineparent)
        r = self._parse(buffer, inlineparent)
        if r is None:
            return None
        (s, size) = r
        self.subclass(s)
        return (s, (size + self.padding - 1) // self.padding * self.padding)
    def subclass(self, namedstruct):
        cp = self
        cs = namedstruct
        while True:
            if hasattr(cs, '_sub'):
                cs = cs._sub
                cp = cs._parser
                continue
            subp = None
            clsfr = getattr(cp, 'classifier', None)
            if clsfr is not None:
                clsvalue = clsfr(namedstruct)
                subp = cp.subindices.get(clsvalue)
            if subp is None:
                for sc in cp.subclasses:
                    if sc.isinstance(namedstruct):
                        subp = sc
                        break
            if subp is None:
                break
            cs._subclass(subp)
            namedstruct._splitted = False
            cs = cs._sub
            cp = subp
    def _parse(self, buffer, inlineparent):
        raise NotImplementedError
    def new(self, inlineparent = None):
        if self.base is not None:
            s = self.base.new(inlineparent)
            s._extend(self._new(s._target))
        else:
            s = self._new(inlineparent)
        if self.initfunc is not None:
            self.initfunc(s)
        return s
    def _new(self, inlineparent = None):
        raise NotImplementedError
    def create(self, data, inlineparent = None):
        return NamedStruct(self, data, inlineparent)
    def paddingsize(self, namedstruct):
        realsize = namedstruct._realsize()
        return (realsize + self.padding - 1) // self.padding * self.padding
    def tobytes(self, namedstruct):
        return namedstruct._tobytes()


class FormatParser(Parser):
    '''
    Parsing or serializing a NamedStruct with specified format
    '''
    def __init__(self, fmt, properties, sizefunc = None, prepackfunc = None, base = None, criteria = _never, padding = 8, endian = '>', initfunc = None, typedef = None, classifier = None, classifyby = None):
        Parser.__init__(self, base, criteria, padding, initfunc, typedef, classifier, classifyby)
        self.struct = struct.Struct(endian + fmt)
        self.properties = properties
        self.emptydata = b'\x00' * self.struct.size
        self.sizefunc = sizefunc
        self.prepackfunc = prepackfunc
    def _parse(self, buffer, inlineparent = None):
        if len(buffer) < self.struct.size:
            return None
        s = NamedStruct(self, _copy(buffer[0:self.struct.size]), inlineparent)
        if self.sizefunc is not None:
            size = self.sizefunc(s)
            if len(buffer) < size:
                return None
            if size > self.struct.size:
                s._update(_copy(buffer[0:size]))
        else:
            size = self.struct.size
        return (s, size)
    def _new(self, inlineparent = None):
        s = NamedStruct(self, self.emptydata, inlineparent)
        s._unpack()
        return s
    def sizeof(self, namedstruct):
        return self.struct.size
    def unpack(self, data, namedstruct):
        try:
            result = self.struct.unpack(data[0:self.struct.size])
        except struct.error as exc:
            raise BadFormatError(exc)
        start = 0
        for p in self.properties:
            if len(p) > 1:
                if isinstance(result[start], bytes):
                    v = [r.rstrip(b'\x00') for r in result[start:start + p[1]]]
                else:
                    v = list(result[start:start + p[1]])
                start += p[1]
            else:
                v = result[start]
                if isinstance(v, bytes):
                    v = v.rstrip(b'\x00')
                start += 1
            setin = namedstruct._target
            for sp in p[0][0:-1]:
                if not hasattr(setin, sp):
                    setin2 = InlineStruct(namedstruct._target)
                    setattr(setin, sp, setin2)
                    setin = setin2
                else:
                    setin = getattr(setin, sp)
            setattr(setin, p[0][-1], v)
        return data[self.struct.size:]
    def pack(self, namedstruct):
        if self.prepackfunc is not None:
            self.prepackfunc(namedstruct)
        elements = []
        for p in self.properties:
            v = namedstruct._target
            for sp in p[0]:
                v = getattr(v, sp)
            if len(p) > 1:
                elements.extend(v[0:p[1]])
            else:
                elements.append(v)
        return self.struct.pack(*elements)

class SequencedParser(Parser):
    '''
    A parser constructed by a sequence of parsers
    '''
    def __init__(self, parserseq, sizefunc = None, prepackfunc = None, lastextra = True, base = None, criteria = _never, padding = 8, initfunc = None, typedef = None, classifier = None, classifyby = None):
        Parser.__init__(self, base, criteria, padding, initfunc, typedef, classifier, classifyby)
        self.parserseq = parserseq
        self.sizefunc = sizefunc
        self.prepackfunc = prepackfunc
        if lastextra:
            self.parserseq = parserseq[0:-1]
            self.extra = parserseq[-1]
    def _parse(self, buffer, inlineparent = None):
        s = NamedStruct(self, None, inlineparent)
        size = self._parseinner(buffer, s, True, False)
        if size is None:
            return None
        else:
            return (s, size)
    def _parseinner(self, buffer, namedstruct, copy = False, useall = True):
        s = namedstruct
        inlineparent = s._target
        s._seqs = []
        start = 0
        for p, name in self.parserseq:
            parent = None
            if name is None:
                parent = inlineparent
            if name is not None and len(name) > 1:
                # Array
                v = list(range(0, name[1]))
                for i in range(0, name[1]):
                    r = p.parse(buffer[start:], parent)
                    if r is None:
                        return None
                    v[i] = r[0]
                    start += r[1]
                setattr(inlineparent, name[0], v)
            else:
                r = p.parse(buffer[start:], parent)
                if r is None:
                    return None
                (s2, size) = r
                if name is not None:
                    setattr(inlineparent, name[0], s2)
                else:
                    s._seqs.append(s2)
                    if not s2._splitted:
                        s2._unpack()
                start += size
        s._splitted = True
        if useall:
            size = len(buffer)
        else:
            if self.sizefunc is not None:
                if copy:
                    s._update(_copy(buffer[0:start]))
                else:
                    s._update(buffer[0:start])
                size = self.sizefunc(s)
            else:
                size = start
        if copy:
            s._update(_copy(buffer[0:size]))
        else:
            s._update(buffer[0:size])
        if hasattr(self, 'extra'):
            p, name = self.extra
            if name is not None and len(name) > 1:
                extraArray = []
                while start < size:
                    r = p.parse(buffer[start:], None)
                    if r is None:
                        break
                    extraArray.append(r[0])
                    start += r[1]
                setattr(inlineparent, name[0], extraArray)
            else:
                if name is None:
                    s2 = p.create(s._struct[start:size], inlineparent)
                    s._seqs.append(s2)
                    s2._unpack()
                else:
                    setattr(inlineparent, name[0], p.create(s._struct[start:size], None))
        else:
            s._extra = s._struct[start:size]
        return size
    def unpack(self, data, namedstruct):
        if data is None:
            return getattr(namedstruct, '_extra', b'')
        size = self._parseinner(data, namedstruct, False, True)
        if size is None:
            raise BadLenError('Cannot parse struct: data is corrupted.')
        extra = getattr(namedstruct, '_extra', b'')
        if hasattr(namedstruct, '_extra'):
            del namedstruct._extra
        return extra
    def pack(self, namedstruct):
        packdata = []
        s = namedstruct
        inlineparent = s._target
        if self.prepackfunc is not None:
            self.prepackfunc(s)
        seqiter = iter(s._seqs)
        for p, name in self.parserseq:
            if name is not None and len(name) > 1:
                # Array
                v = getattr(inlineparent, name[0])
                for i in range(0, name[1]):
                    if i >= len(v):
                        packdata.append(p.tobytes(p.new()))
                    else:
                        packdata.append(p.tobytes(v[i]))
            else:
                if name is not None:
                    v = getattr(inlineparent, name[0])
                else:
                    v = next(seqiter)
                packdata.append(p.tobytes(v))
        if hasattr(self, 'extra'):
            p, name = self.extra
            if name is not None and len(name) > 1:
                v = getattr(inlineparent, name[0])
                for es in v:
                    packdata.append(p.tobytes(es))
            else:
                if name is None:
                    v = next(seqiter)
                else:
                    v = getattr(inlineparent, name[0])
                packdata.append(p.tobytes(v))
        return b''.join(packdata)
    def _new(self, inlineparent = None):
        s = NamedStruct(self, None, inlineparent)
        inlineparent = s._target
        s._seqs = []
        for p, name in self.parserseq:
            if name is not None and len(name) > 1:
                # Array
                v = list(range(0, name[1]))
                for i in range(0, name[1]):
                    v[i] = p.new()
                setattr(inlineparent, name[0], v)
            else:
                if name is not None:
                    v = p.new()
                    setattr(inlineparent, name[0], v)
                else:
                    v = p.new(inlineparent)
                    s._seqs.append(v)
                    v._unpack()
        s._splitted = True
        if hasattr(self, 'extra'):
            p, name = self.extra
            if name is not None and len(name) > 1:
                setattr(inlineparent, name[0], [])
            else:
                if name is None:
                    s2 = p.new(inlineparent)
                    s._seqs.append(s2)
                    s2._unpack()
                else:
                    setattr(inlineparent, name[0], p.new())
        else:
            s._extra = b''
        return s
    def sizeof(self, namedstruct):
        size = 0
        s = namedstruct
        if not namedstruct._splitted:
            namedstruct._unpack()
        inlineparent = s._target
        seqiter = iter(s._seqs)
        for p, name in self.parserseq:
            if name is not None and len(name) > 1:
                # Array
                v = getattr(inlineparent, name[0])
                for i in range(0, name[1]):
                    if i >= len(v):
                        size += p.paddingsize(p.new())
                    else:
                        size += p.paddingsize(v[i])
            else:
                if name is not None:
                    v = getattr(inlineparent, name[0])
                else:
                    v = next(seqiter)
                size += p.paddingsize(v)
        if hasattr(self, 'extra'):
            p, name = self.extra
            if name is not None and len(name) > 1:
                v = getattr(inlineparent, name[0])
                for es in v:
                    size += p.paddingsize(es)
            else:
                if name is None:
                    v = next(seqiter)
                else:
                    v = getattr(inlineparent, name[0])
                size += p.paddingsize(v)
        return size

class PrimitiveParser(object):
    def __init__(self, fmt, endian = '>'):
        self.struct = struct.Struct(endian + fmt)
        self.emptydata = b'\x00' * self.struct.size
        self.empty = self.struct.unpack(self.emptydata)[0]
        if isinstance(self.empty, bytes):
            self.empty = b''
    def parse(self, buffer, inlineparent = None):
        if len(buffer) < self.struct.size:
            return None
        try:
            return (self.struct.unpack(buffer[:self.struct.size])[0], self.struct.size)
        except struct.error as exc:
            raise BadFormatError(exc)
    def new(self, inlineparent = None):
        return self.empty
    def create(self, data, inlineparent = None):
        try:
            return self.struct.unpack(data)[0]
        except struct.error as exc:
            raise BadFormatError(exc)
    def sizeof(self, prim):
        return self.struct.size
    def paddingsize(self, prim):
        return self.struct.size
    def tobytes(self, prim):
        return self.struct.pack(prim)

class ArrayParser(object):
    def __init__(self, innerparser, size):
        self.innerparser = innerparser
        self.size = size
    def parse(self, buffer, inlineparent = None):
        size = 0
        v = []
        for i in range(0, self.size):  # @UnusedVariable
            r = self.innerparser.parse(buffer, None)
            if r is None:
                return None
            v.append(r[0])
            size += r[1]
        return (v, size)
    def new(self, inlineparent = None):
        v = list(range(0, self.size))
        for i in range(0, self.size):
            v[i] = self.innerparser.new()
        return v
    def create(self, data, inlineparent = None):
        if self.size > 0:
            r = self.parse(data)
            if r is None:
                raise ParseError('data is not enough to create an array of size ' + self.size)
            else:
                return r[0]
        else:
            v = []
            start = 0
            while start < len(data):
                r = self.innerparser.parse(data, None)
                if r is None:
                    break
                v.append(r[0])
                start += r[1]
            return v
    def sizeof(self, prim):
        size = 0
        arraysize = self.size
        if arraysize == 0:
            arraysize = len(prim)
        for i in range(0, arraysize):
            if i >= len(prim):
                size += self.innerparser.paddingsize(self.innerparser.new())
            else:
                size += self.innerparser.paddingsize(prim[i])
        return size
    def paddingsize(self, prim):
        return self.sizeof(prim)
    def tobytes(self, prim):
        data = []
        arraysize = self.size
        if arraysize == 0:
            arraysize = len(prim)
        for i in range(0, arraysize):
            if i >= len(prim):
                data.append(self.innerparser.tobytes(self.innerparser.new()))
            else:
                data.append(self.innerparser.tobytes(prim[i]))
        return b''.join(data)
        


class RawParser(object):
    def __init__(self, cstr = False):
        self.cstr = cstr
    def parse(self, buffer, inlineparent = None):
        return (b'', 0)
    def new(self, inlineparent = None):
        return b''
    def create(self, data, inlineparent = None):
        if self.cstr:
            return data.rstrip(b'\x00')
        else:
            return data
    def sizeof(self, prim):
        return len(prim)
    def paddingsize(self, prim):
        return len(prim)
    def tobytes(self, prim):
        return prim

class CstrParser(object):
    def __init__(self):
        pass
    def parse(self, buffer, inlineparent = None):
        for i in range(0, len(buffer)):
            if buffer[i] == b'\x00':
                return (buffer[0:i], i + 1)
        return None
    def new(self, inlineparent = None):
        return b''
    def create(self, data, inlineparent = None):
        if data[-1] != b'\x00':
            raise BadFormatError(b'Cstr is not zero-terminated')
        for i in range(0, len(data) - 1):
            if data[i] == b'\x00':
                raise BadFormatError(b'Cstr has zero inside the string')
        return data
    def sizeof(self, prim):
        return len(prim) + 1
    def paddingsize(self, prim):
        return self.sizeof(prim)
    def tobytes(self, prim):
        return prim + b'\x00'

class typedef(object):
    def parser(self):
        if not hasattr(self, '_parser'):
            self._parser = self._compile()
        return self._parser
    def parse(self, buffer):
        return self.parser().parse(buffer)
    def create(self, buffer):
        d = self.parser().create(buffer)
        if hasattr(self.parser(), 'subclass'):
            self.parser().subclass(d)
        return d
    def new(self, **kwargs):
        obj = self.parser().new()
        for k,v in kwargs.items():
            setattr(obj, k, v)
        return obj
    def __call__(self, **kwargs):
        return self.new(**kwargs)
    def tobytes(self, obj):
        return self.parser().tobytes(obj)
    def inline(self):
        return None
    def array(self, size):
        return arraytype(self, size)
    def vararray(self):
        return self.array(0)
    def __getitem__(self, size):
        return self.array(size)
    def isextra(self):
        return False

class arraytype(typedef):
    def __init__(self, innertype, size = 0):
        self.innertype = innertype
        self.size = size
    def _compile(self):
        return ArrayParser(self.innertype.parser(), self.size)
    def isextra(self):
        return self.size == 0
    def __repr__(self, *args, **kwargs):
        return '%r[%d]' % (self.innertype, self.size)

class rawtype(typedef):
    _parser = RawParser()
    def parser(self):
        return self._parser
    def array(self, size):
        raise TypeError('rawtype cannot form array')
    def isextra(self):
        return True
    def __repr__(self, *args, **kwargs):
        return 'raw'

raw = rawtype()

class varchrtype(typedef):
    _parser = RawParser(True)
    def parser(self):
        return self._parser
    def array(self, size):
        raise TypeError('varchrtype cannot form array')
    def isextra(self):
        return True
    def __repr__(self, *args, **kwargs):
        return 'varchr'

varchr = varchrtype()

class cstrtype(typedef):
    _parser = CstrParser()
    def parser(self):
        return self._parser
    def __repr__(self, *args, **kwargs):
        return 'cstr'

cstr = cstrtype()

class prim(typedef):
    def __init__(self, fmt, readablename = None, endian = '>', strict = False):
        typedef.__init__(self)
        self._format = fmt
        self._inline = (fmt, ())
        self._readablename = readablename
        self._endian = endian
        self._strict = strict
    def _compile(self):
        return PrimitiveParser(self._format, self._endian)
    def inline(self):
        if self._strict:
            return None
        else:
            return self._inline
    def __repr__(self, *args, **kwargs):
        if self._readablename is not None:
            return str(self._readablename)
        else:
            return ('prim(%r)' % (self.format,))

class chartype(prim):
    def __init__(self):
        prim.__init__(self, 'c', 'char')
    def array(self, size):
        if size == 0:
            return raw
        else:
            return prim(str(size) + 's')

char = chartype()

class fixedstruct(typedef):
    def __init__(self, fmt, properties, sizefunc = None, prepackfunc = None, base = None, criteria = _never, padding = 8, endian = '>', readablename = None, inlineself = None, initfunc = None, nstructtype = None, classifier = None, classifyby = None):
        self.sizefunc = sizefunc
        self.prepackfunc = prepackfunc
        self.base = base
        self.criteria = criteria
        self.padding = padding
        self.endian = endian
        self.format = fmt
        self.properties = properties
        self.readablename = readablename
        self.initfunc = initfunc
        self.classifier = classifier
        self.classifyby = classifyby
        if nstructtype is None:
            nstructtype = self
        self.nstructtype = nstructtype
        size = struct.calcsize(endian + self.format)
        paddingsize = (size + padding - 1) // padding * padding
        if paddingsize > size:
            paddingformat = self.format + str(paddingsize - size) + 'x'
        else:
            paddingformat = self.format
        if inlineself is None:
            if self.base is None and self.sizefunc is None and self.prepackfunc is None and self.initfunc is None and len(self.properties) <= 5:
                self._inline = (paddingformat, self.properties)
            else:
                self._inline = None
        elif inlineself:
            self._inline = (paddingformat, self.properties)
        else:
            self._inline = None
    def _compile(self):
        return FormatParser(self.format, self.properties, self.sizefunc, self.prepackfunc, 
                            None if self.base is None else self.base.parser(), self.criteria, self.padding, self.endian,self.initfunc, self.nstructtype, self.classifier, self.classifyby)
    def inline(self):
        return self._inline
    def __repr__(self, *args, **kwargs):
        if self.readablename is not None:
            return str(self.readablename)
        else:
            return 'fixed(%r)' % (self.format,)

class StructDefWarning(Warning):
    pass

class nstruct(typedef):
    def __init__(self, *members, **arguments):
        params = ['size', 'prepack', 'base', 'criteria', 'endian', 'padding', 'lastextra', 'name', 'inline', 'init', 'classifier', 'classifyby', 'endian', 'formatter', 'extend']
        for k in arguments:
            if not k in params:
                warnings.warn(StructDefWarning('Parameter %r is not recognized, is there a spelling error?' % (k,)))
        if 'name' not in arguments:
            warnings.warn(StructDefWarning('A struct is not named: %r' % (members,)))
        self.sizefunc = arguments.get('size', None)
        self.prepackfunc = arguments.get('prepack', None)
        self.base = arguments.get('base', None)
        self.criteria = arguments.get('criteria', _never)
        self.endian = arguments.get('endian', '>')
        self.padding = arguments.get('padding', 8)
        self.lastextra = arguments.get('lastextra', None)
        self.readablename = arguments.get('name', None)
        self.inlineself = arguments.get('inline', None)
        self.initfunc = arguments.get('init', None)
        self.classifier = arguments.get('classifier', None)
        self.classifyby = arguments.get('classifyby', None)
        if 'formatter' in arguments:
            self.extraformatter = arguments['formatter']
        self.formatters = {}
        self.listformatters = {}
        if self.criteria is None:
            raise ValueError('Criteria cannot be None; use default _never instead')
        if self.classifyby is not None and (isinstance(self.classifyby, str) or not hasattr(self.classifyby, '__iter__')):
            raise ValueError('classifyby must be a tuple of values')
        if self.base is not None:
            self.formatters = dict(self.base.formatters)
            self.listformatters = dict(self.base.listformatters)
            if self.inlineself:
                raise ValueError('Cannot inline a struct with a base class')
            if self.classifyby is not None and getattr(self.base, 'classifier', None) is None:
                raise ValueError('Classifier is not defined in base type %r, but sub class %r has a classify value' % (self.base, self))
            if self.classifyby is None and getattr(self.base, 'classifier', None) is not None:
                warnings.warn(StructDefWarning('Classifier is defined in base type %r, but sub class %r does not have a classifyby' % (self.base, self)))
        else:
            if self.classifyby is not None:
                raise ValueError('Classifyby is defined in %r without a base class' % (self,))
            if self.criteria is not None and self.criteria is not _never:
                raise ValueError('criteria is defined in %r without a base class' % (self,))
        self.subclasses = []
        lastinline_format = []
        lastinline_properties = []
        seqs = []
        endian = arguments.get('endian', '>')
        if not members and self.base is None and self.sizefunc is None:
            raise ValueError('Struct cannot be empty')
        mrest = len(members)
        for m in members:
            mrest -= 1
            t = m[0]
            if isinstance(t, str):
                t = prim(t)
            elif isinstance(t, tuple):
                t = nstruct(*t, padding=1, endian=self.endian)
            if isinstance(t, arraytype):
                if hasattr(t, 'formatter'):
                    if len(m) > 1:
                        self.formatters[(m[1],)] = t.formatter
                    else:
                        self.formatters[(t,)] = t.formatter
                array = t.size
                t = t.innertype
            else:
                array = None
            if hasattr(t, 'formatter'):
                if array is None:
                    if len(m) > 1:
                        self.formatters[(m[1],)] = t.formatter
                    else:
                        self.formatters[(t,)] = t.formatter
                else:
                    if len(m) > 1:
                        self.listformatters[(m[1],)] = t.formatter
                    else:
                        self.listformatters[(t,)] = t.formatter                    
            if mrest == 0 and self.lastextra:
                inline = None
            else:
                inline = t.inline()
            if inline is not None:
                if array is not None and (array == 0 or inline[1]):
                    inline = None
            if inline is not None:
                if not inline[1]:
                    if array is None:
                        if len(m) > 1:
                            lastinline_format.append(inline[0])
                            lastinline_properties.append(((m[1],),))
                        else:
                            lastinline_format.append(str(struct.calcsize(endian + inline[0])) + 'x')
                    else:
                        if len(m) > 1:
                            lastinline_format.extend([inline[0]] * array)
                            lastinline_properties.append(((m[1],),array))
                        else:
                            lastinline_format.append(str(struct.calcsize(endian + inline[0]) * array) + 'x')
                else:
                    lastinline_format.append(inline[0])
                    for prop in inline[1]:
                        if len(m) > 1:
                            if len(prop) > 1:
                                lastinline_properties.append(((m[1],) + prop[0], prop[1]))
                            else:
                                lastinline_properties.append(((m[1],) + prop[0],))
                        else:
                            lastinline_properties.append(prop)
                    if len(m) > 1:
                        if hasattr(t, 'formatters'):
                            for k,v in t.formatters.items():
                                self.formatters[(m[1],) + k] = v
                        if hasattr(t, 'listformatters'):
                            for k,v in t.listformatters.items():
                                self.listformatters[(m[1],) + k] = v
                        if hasattr(t, 'extraformatter'):
                            self.formatters[(m[1],)] = v
                    else:
                        if hasattr(t, 'formatters'):
                            for k,v in t.formatters.items():
                                self.formatters[k] = v
                        if hasattr(t, 'listformatters'):
                            for k,v in t.listformatters.items():
                                self.listformatters[k] = v
                        if hasattr(t, 'extraformatter'):
                            self.formatters[(t,)] = v                        
            else:
                if lastinline_format:
                    seqs.append((fixedstruct(''.join(lastinline_format), lastinline_properties, padding = 1, endian = self.endian), None))
                    del lastinline_format[:]
                    lastinline_properties = []
                if len(m) > 1:
                    if array is not None:
                        seqs.append((t, (m[1], array)))
                    else:
                        seqs.append((t, (m[1],)))
                else:
                    if array is not None:
                        raise ValueError('Illegal inline array: ' + repr(m))
                    seqs.append((t, None))
        self._inline = None
        if lastinline_format:
            if not seqs:
                self.fixedstruct = fixedstruct(''.join(lastinline_format), lastinline_properties, self.sizefunc,
                                         self.prepackfunc, self.base, self.criteria, self.padding, self.endian,
                                         self.readablename, self.inlineself, self.initfunc, self, self.classifier, self.classifyby)
                self._inline = self.fixedstruct.inline()
                self.lastextra = False
            else:
                seqs.append((fixedstruct(''.join(lastinline_format), lastinline_properties, padding = 1, endian = self.endian), None))
                self.seqs = seqs
                self.lastextra = False
        else:
            if not seqs:
                self.fixedstruct = fixedstruct('', (), self.sizefunc,
                                         self.prepackfunc, self.base, self.criteria, self.padding, self.endian,
                                         self.readablename, self.inlineself, self.initfunc, self, self.classifier, self.classifyby)
                self._inline = self.fixedstruct.inline()
                self.lastextra = False
            else:
                self.seqs = seqs
                if self.lastextra is None:
                    lastmember = self.seqs[-1]
                    if lastmember[1] is not None and len(lastmember[1]) > 1 and lastmember[1][1] == 0:
                        self.lastextra = True
                    elif (lastmember[1] is None or len(lastmember[1]) <= 1) and lastmember[0].isextra():
                        self.lastextra = True
                    else:
                        self.lastextra = False
        if 'extend' in arguments:
            for k,v in arguments['extend'].items():
                if isinstance(k, tuple):
                    kt = k
                else:
                    kt = (k,)
                if hasattr(v, 'formatter'):
                    self.formatters[kt] = v.formatter
                if isinstance(v, arraytype):
                    t = v.innertype
                    if hasattr(t, 'formatter'):
                        self.listformatters[kt] = t.formatter
        if self.base is not None:
            self.base.derive(self)
    def _compile(self):
        if self.base is not None:
            self.base.parser()
        if not hasattr(self, 'fixedstruct'):
            for t,name in self.seqs:
                t.parser()
        if hasattr(self, '_parser'):
            return self._parser
        if hasattr(self, 'fixedstruct'):
            p = self.fixedstruct.parser()
        else:
            p = SequencedParser([(t.parser(), name) for t,name in self.seqs], self.sizefunc, self.prepackfunc, self.lastextra,
                                None if self.base is None else self.base.parser(), self.criteria, self.padding, self.initfunc, self, self.classifier, self.classifyby)
        self._parser = p
        for sc in self.subclasses:
            sc.parser()
        return p
    def inline(self):
        return self._inline
    def __repr__(self, *args, **kwargs):
        if self.readablename is not None:
            return self.readablename
        else:
            return typedef.__repr__(self, *args, **kwargs)
    def isextra(self):
        return self.lastextra
    def derive(self, newchild):
        self.subclasses.append(newchild)
        if hasattr(self, '_parser'):
            newchild.parser()
    def formatdump(self, dumpvalue):
        try:
            for k,v in self.listformatters.items():
                current = dumpvalue
                for ks in k:
                    if isinstance(ks, str):
                        current = current[ks]
                try:
                    for i in range(0, len(current)):
                        current[i] = v(current[i])
                except:
                    NamedStruct._logger.log(logging.DEBUG, 'A formatter thrown an exception', exc_info = True)
            for k,v in self.formatters.items():
                current = dumpvalue
                last = None
                lastkey = None
                for ks in k:
                    if isinstance(ks, str):
                        last = current
                        lastkey = ks
                        current = current[ks]
                if lastkey is None:
                    try:
                        dumpvalue = v(dumpvalue)
                    except: 
                        NamedStruct._logger.log(logging.DEBUG, 'A formatter thrown an exception', exc_info = True)
                else:
                    try:
                        last[lastkey] = v(current)
                    except:
                        NamedStruct._logger.log(logging.DEBUG, 'A formatter thrown an exception', exc_info = True)
        except:
            NamedStruct._logger.log(logging.DEBUG, 'A formatter thrown an exception', exc_info = True)
        return dumpvalue

class enum(prim):
    def __init__(self, readablename = None, namespace = None, basefmt = 'I', bitwise = False, **kwargs):
        if hasattr(basefmt, '_format'):
            prim.__init__(self, basefmt._format, readablename, basefmt._endian, basefmt._strict)
        else:
            prim.__init__(self, basefmt, readablename)
        self._values = dict(kwargs)
        self._bitwise = bitwise
        for k,v in kwargs.items():
            setattr(self, k, v)
        if namespace is not None:
            for k,v in kwargs.items():
                namespace[k] = v
    def getName(self, value, defaultName = None):
        for k,v in self._values.items():
            if v == value:
                return k
        return defaultName
    def getValue(self, name, defaultValue = None):
        return self._values.get(name, defaultValue)
    def importAll(self, gs):
        for k,v in self._values.items():
            gs[k] = v
    def extend(self, namespace = None, **kwargs):
        d = dict(self._values)
        d.update(kwargs)
        return enum(self._readablename, namespace, self, self._bitwise, **d)
    def tostr(self, value):
        return str(self.formatter(value))
    def getDict(self):
        return self._values
    def __contains__(self, item):
        return item in self._values.values()
    def astype(self, primtype, bitwise = False):
        return enumref(self, primtype, bitwise)
    def formatter(self, value):
        if not self._bitwise:
            n = self.getName(value)
            if n is None:
                return value
            else:
                return n
        else:
            names = []
            for k,v in sorted(self._values.items(), key=lambda x: x[1], reverse=True):
                if (v & value) == v:
                    names.append(k)
                    value = value ^ v
            names.reverse()
            if value != 0:
                names.append(hex(value))
            if not names:
                return 0 
            return ' '.join(names)
    def merge(self, otherenum):
        return self.extend(None, **otherenum.getDict())

class enumref(prim):
    def __init__(self, refenum, basefmt = 'I', bitwise = False):
        if hasattr(basefmt, '_format'):
            prim.__init__(self, basefmt._format, refenum._readablename, basefmt._endian, basefmt._strict)
        else:
            prim.__init__(self, basefmt, refenum._readablename)
        self._ref = refenum
        self._bitwise = bitwise
    def getName(self, value, defaultName = None):
        for k,v in self._ref._values.items():
            if v == value:
                return k
        return defaultName
    def getValue(self, name, defaultValue = None):
        return self._ref._values.get(name, defaultValue)
    def importAll(self, gs):
        for k,v in self._ref._values.items():
            gs[k] = v
    def extend(self, namespace = None, **kwargs):
        d = dict(self._ref._values)
        d.update(kwargs)
        return enum(self._readablename, namespace, self, **d)
    def tostr(self, value):
        return str(self.formatter(value))
    def getDict(self):
        return self._ref._values
    def __contains__(self, item):
        return item in self._ref._values.values()
    def astype(self, primtype, bitwise = False):
        return enumref(self._ref, primtype, bitwise)
    def formatter(self, value):
        if not self._bitwise:
            n = self.getName(value)
            if n is None:
                return value
            else:
                return n
        else:
            names = []
            for k,v in sorted(self._ref._values.items(), key=lambda x: x[1], reverse=True):
                if (v & value) == v:
                    names.append(k)
                    value = value ^ v
            names.reverse()
            if value != 0:
                names.append(hex(value))
            if not names:
                return 0 
            return ' '.join(names)
    def merge(self, otherenum):
        return self.extend(None, **otherenum.getDict())

class OptionalParser(Parser):
    def __init__(self, basetypeparser, name, criteria, typedef):
        Parser.__init__(self, padding = 1, typedef=typedef)
        self.basetypeparser = basetypeparser
        self.name = name
        self.criteria = criteria
    def _parseinner(self, data, s, create = False):
        if self.criteria(s):
            if create:
                inner = self.basetypeparser.create(data, None)
                size = len(data)
            else:
                r = self.basetypeparser.parse(data, None)
                if r is None:
                    return None
                (inner, size) = r
            setattr(s._target, self.name, inner)
            return size
        else:
            return 0       
    def _parse(self, data, inlineparent = None):
        s = NamedStruct(self, None, inlineparent)
        size = self._parseinner(data, s)
        if size is None:
            return None
        else:
            return (s, size)
    def _new(self, inlineparent=None):
        return NamedStruct(self, None, inlineparent)
    def unpack(self, data, namedstruct):
        if data is None:
            return getattr(namedstruct, '_extra', b'')
        size = self._parseinner(data, namedstruct, True)
        if size is None:
            raise BadLenError('Bad Len')
        else:
            return data[size:]
    def pack(self, namedstruct):
        data = b''
        if hasattr(namedstruct, self.name):
            data = self.basetypeparser.tobytes(getattr(namedstruct, self.name))
        return data
    def sizeof(self, namedstruct):
        if hasattr(namedstruct, self.name):
            return self.basetypeparser.paddingsize(getattr(namedstruct, self.name))
        else:
            return 0

class optional(typedef):
    def __init__(self, basetype, name, criteria):
        self.basetype = basetype
        self.criteria = criteria
        if name is None:
            raise ParseError('Optional member cannot be in-line member')
        self.name = name
    def array(self, size):
        raise TypeError('optional type cannot form array')
    def _compile(self):
        return OptionalParser(self.basetype.parser(), self.name, self.criteria, self)
    def isextra(self):
        return self.basetype.isextra()
    def __repr__(self, *args, **kwargs):
        return repr(self.basetype) + '?'
