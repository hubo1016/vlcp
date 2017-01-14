'''
Created on 2016/3/14

:author: hubo
'''
import sys
from vlcp.config.config import config, Configurable

try:
    from urllib import unquote, quote
    unquote_to_bytes = unquote
    quote_from_bytes = quote
except:
    from urllib.parse import unquote_to_bytes, quote_from_bytes
from namedstruct.namedstruct import NamedStruct, EmbeddedStruct
from namedstruct import dump as namedstruct_dump
from vlcp.utils.dataobject import DataObject, ReferenceObject, WeakReferenceObject, DataObjectSet
from vlcp.utils.dataobject import dump as dataobject_dump
import base64
import json

class JSONBytes(object):
    def __init__(self, data):
        self.data = data

def encode_default(obj):
    if isinstance(obj, JSONBytes):
        return {'<vlcpjsonencode/urlencoded-bytes>': quote_from_bytes(obj.data)}
    elif isinstance(obj, bytes):
        return {'<vlcpjsonencode/urlencoded-bytes>': quote_from_bytes(obj)}
    elif isinstance(obj, NamedStruct):
        # Hacked in the internal getstate implementation...
        state = obj.__getstate__()
        if state[2] is not obj:
            return {'<vlcpjsonencode/namedstruct.NamedStruct>':{'type':state[1], 'data':base64.b64encode(state[0]), 'target':state[2]}}
        else:
            return {'<vlcpjsonencode/namedstruct.NamedStruct>':{'type':state[1], 'data':base64.b64encode(state[0])}}
    else:
        if hasattr(obj, 'jsonencode'):
            try:
                key = '<vlcpjsonencode/' + type(obj).__module__ + '.' + type(obj).__name__ + '>'
            except AttributeError:
                raise TypeError(repr(obj) + " is not JSON serializable")
            else:
                return {key : obj.jsonencode()}
        else:
            raise TypeError(repr(obj) + " is not JSON serializable")


def decode_object(obj):
    if len(obj) == 1:
        try:
            k = str(next(iter(obj.keys())))
        except Exception:
            return obj
        v = obj[k]
        if k.startswith('<vlcpjsonencode/') and k.endswith('>'):
            classname = k[16:-1]
            if classname == 'urlencoded-bytes':
                return unquote_to_bytes(v)
            elif classname == 'namedstruct.NamedStruct':
                if 'target' in v:
                    target = v['target']
                    s = EmbeddedStruct.__new__(EmbeddedStruct)
                    s.__setstate__((base64.b64decode(v['data']), v['type'], target))
                else:
                    s = NamedStruct.__new__(NamedStruct)
                    s.__setstate__((base64.b64decode(v['data']), v['type'], s))
                return s
            else:
                dotpos = classname.rfind('.')
                if dotpos == -1:
                    raise ValueError(repr(classname) + ' is not a valid type')
                package = classname[:dotpos]
                cname = classname[dotpos+1:]
                p = None
                cls = None
                try:
                    p = sys.modules[package]
                    cls = getattr(p, cname)
                except KeyError:
                    raise ValueError(repr(classname) + ' is forbidden because it is not loaded')
                except AttributeError:
                    raise ValueError(repr(classname) + ' is not defined')
                if hasattr(cls, 'jsondecode'):
                    return cls.jsondecode(v)
                else:
                    raise ValueError(repr(classname) + ' is not JSON serializable')
        else:
            return obj
    else:
        return obj

@config('jsonformat')
class JsonFormat(Configurable):
    '''
    This is an extended JSON formatter used by WebAPI module
    '''
    # Enable special format for namedstruct (https://pypi.python.org/pypi/nstruct)
    _default_namedstruct = True
    # Use human read format, so:
    # namedstruct structures are formatted with readable names;
    # very long bytes are viewed like \<10000 bytes...\> (Python 3 only)
    _default_humanread = True
    # In Python 3, Try to decode bytes to str with this encoding 
    _default_bytesdecode = 'ascii'
    # When bytes object is longer than this and humanread=True, show \<*length* bytes...\>
    # instead of the exact content
    _default_byteslimit = 256
    # Dump extra information for namedstruct structures
    _default_dumpextra = False
    # Dump type information for namedstruct structures
    _default_dumptypeinfo = 'flat'
    # Dump the content for data objects
    _default_dataobject = True
    # Dump attributes for data objects
    _default_dataattributes = True    
    def jsonencoder(self, obj):
        if isinstance(obj, NamedStruct) and self.namedstruct:
            return namedstruct_dump(obj, self.humanread, self.dumpextra, self.dumptypeinfo)
        elif isinstance(obj, (DataObject, DataObjectSet, ReferenceObject, WeakReferenceObject)) and self.dataobject:
            return dataobject_dump(obj, self.dataattributes)
        elif isinstance(obj, bytes):
            if self.humanread and len(obj) > self.byteslimit:
                return '<%d bytes...>' % (len(obj),)
            else:
                if self.bytesdecode:
                    try:
                        return obj.decode(self.bytesdecode)
                    except:
                        return repr(obj)
                else:
                    return repr(obj)
        else:
            try:
                return encode_default(obj)
            except Exception:
                return repr(obj)
    