'''
Created on 2016/8/25

:author: hubo
'''

from namedstruct import *
from namedstruct.namedstruct import BadFormatError, BadLenError, Parser, _create_struct

def _tobytes(s, encoding = 'utf-8'):
    if s is None:
        return None
    elif isinstance(s, bytes):
        return s
    else:
        return s.encode(encoding)

class UStringParser(object):
    '''
    Jute ustring type. 
    '''
    def __init__(self):
        pass
    def parse(self, buffer, inlineparent = None):
        if len(buffer) < 4:
            return None
        length = int32.create(buffer[:4])
        if length < 0:
            return (None, 4)
        if len(buffer) < 4 + length:
            return None
        else:
            return (buffer[4:4+length], 4 + length)
    def new(self, inlineparent = None):
        return b''
    def create(self, data, inlineparent = None):
        r = self.parse(data)
        if r is None:
            raise BadLenError('Ustring length not match')
        return r[0]
    def sizeof(self, prim):
        prim = _tobytes(prim)
        if prim is None:
            return 4
        else:
            return len(prim) + 4
    def paddingsize(self, prim):
        return self.sizeof(prim)
    def tobytes(self, prim, skipprepack = False):
        prim = _tobytes(prim)
        if prim is None:
            return int32.tobytes(-1)
        else:
            return int32.tobytes(len(prim)) + prim

class ustringtype(typedef):
    '''
    A int32 followed by variable length bytes
    '''
    _parser = UStringParser()
    def __init__(self, displayname = 'ustring'):
        typedef.__init__(self)
        self._displayname = displayname
    def parser(self):
        return self._parser
    def __repr__(self, *args, **kwargs):
        return self._displayname

ustring = ustringtype()
z_buffer = ustringtype('buffer')


class VectorParser(object):
    '''
    Jute vector type. 
    '''
    def __init__(self, innerparser):
        self._innerparser = innerparser
    def parse(self, buffer, inlineparent = None):
        if len(buffer) < 4:
            return None
        length = int32.create(buffer[:4])
        if length < 0:
            return (None, 4)
        start = 4
        result = []
        for i in range(0, length):
            r = self._innerparser.parse(buffer[start:], None)
            if r is None:
                return None
            (inner, size) = r
            result.append(inner)
            start += size
        return (result, start)
    def new(self, inlineparent = None):
        return []
    def create(self, data, inlineparent = None):
        r = self.parse(data)
        if r is None:
            raise BadLenError('Ustring length not match')
        return r[0]
    def sizeof(self, prim):
        if prim is None:
            return 4
        else:
            return sum(self._innerparser.paddingsize(r) for r in prim) + 4
    def paddingsize(self, prim):
        return self.sizeof(prim)
    def tobytes(self, prim, skipprepack = False):
        if prim is None:
            return int32.tobytes(-1)
        else:
            return int32.tobytes(len(prim)) + b''.join(self._innerparser.tobytes(r) for r in prim)

class vector(typedef):
    '''
    Jute vector
    '''
    def __init__(self, innertype):
        typedef.__init__(self)
        self._innertype = innertype
    def _compile(self):
        return VectorParser(self._innertype.parser())
    def __repr__(self, *args, **kwargs):
        return 'vector<' + repr(self._innertype) + '>'

# /* predefined xid's values recognized as special by the server */
zk_xid = enum('zk_xid', globals(), int32,
    WATCHER_EVENT_XID = -1, 
    PING_XID = -2,
    AUTH_XID = -4,
    SET_WATCHES_XID = -8)

# /* zookeeper event type constants */
zk_watch_event = enum('zk_watch_event', globals(), int32,
    CREATED_EVENT_DEF = 1,
    DELETED_EVENT_DEF = 2,
    CHANGED_EVENT_DEF = 3,
    CHILD_EVENT_DEF = 4,
    SESSION_EVENT_DEF = -1,
    NOTWATCHING_EVENT_DEF = -2)

zk_request_type = enum('zk_request_type', globals(), int32,
    ZOO_NOTIFY_OP = 0,
    ZOO_CREATE_OP = 1,
    ZOO_DELETE_OP = 2,
    ZOO_EXISTS_OP = 3,
    ZOO_GETDATA_OP = 4,
    ZOO_SETDATA_OP = 5,
    ZOO_GETACL_OP = 6,
    ZOO_SETACL_OP = 7,
    ZOO_GETCHILDREN_OP = 8,
    ZOO_SYNC_OP = 9,
    ZOO_PING_OP = 11,
    ZOO_GETCHILDREN2_OP = 12,
    ZOO_CHECK_OP = 13,
    ZOO_MULTI_OP = 14,
    ZOO_CREATE_SESSION_OP = -10,
    ZOO_CLOSE_SESSION_OP = -11,
    ZOO_SETAUTH_OP = 100,
    ZOO_SETWATCHES_OP = 101,
    ZOO_SASL_OP = 102,           # NOT SUPPORTED
    ZOO_ERROR_TYPE = -1
)

zk_client_state = enum('zk_client_state', globals(), int32,
    ZOO_DISCONNECTED_STATE = 0,
    ZOO_NOSYNC_CONNECTED_STATE = 1,
    ZOO_SYNC_CONNECTED_STATE = 3,
    ZOO_AUTH_FAILED_STATE = 4,
    ZOO_CONNECTED_READONLY_STATE = 5,
    ZOO_SASL_AUTHENTICATED_STATE = 6,
    ZOO_EXPIRED_STATE = -112
)

zk_err = enum('zk_err', globals(), int32,
    ZOO_ERR_OK = 0,
    ZOO_ERR_SYSTEMERROR = -1,
    ZOO_ERR_RUNTIMEINCONSISTENCY = -2,
    ZOO_ERR_DATAINCONSISTENCY = -3,
    ZOO_ERR_CONNECTIONLOSS = -4,
    ZOO_ERR_MARSHALLINGERROR = -5,
    ZOO_ERR_UNIMPLEMENTED = -6,
    ZOO_ERR_OPERATIONTIMEOUT = -7,
    ZOO_ERR_BADARGUMENTS = -8,
    ZOO_ERR_APIERROR = -100,
    ZOO_ERR_NONODE = -101,
    ZOO_ERR_NOAUTH = -102,
    ZOO_ERR_BADVERSION = -103,
    ZOO_ERR_NOCHILDRENFOREPHEMERALS = -108,
    ZOO_ERR_NODEEXISTS = -110,
    ZOO_ERR_NOTEMPTY = -111,
    ZOO_ERR_SESSIONEXPIRED = -112,
    ZOO_ERR_INVALIDCALLBACK = -113,
    ZOO_ERR_INVALIDACL = -114,
    ZOO_ERR_AUTHFAILED = -115
    )

zk_perm = enum('zk_perm', globals(), int32, True,
    ZOO_PERM_READ = 1 << 0,
    ZOO_PERM_WRITE = 1 << 1,
    ZOO_PERM_CREATE = 1 << 2,
    ZOO_PERM_DELETE = 1 << 3,
    ZOO_PERM_ADMIN = 1 << 4,
    ZOO_PERM_ALL = 0x1f
)

zk_create_flag = enum('zk_create_flag', globals(), int32, True,
                    ZOO_EPHEMERAL = 1 << 0,
                    ZOO_SEQUENCE = 1 << 1,
    )

Id = nstruct(
        (ustring, 'scheme'),
        (ustring, 'id'),
        name = 'Id',
        padding = 1
     )

ACL = nstruct(
        (zk_perm, 'perms'),
        (Id, 'id'),
        name = 'ACL',
        padding = 1
    )
Stat = nstruct(
        (int64, 'czxid'),      # created zxid
        (int64, 'mzxid'),      # last modified zxid
        (int64, 'ctime'),      # created
        (int64, 'mtime'),      # last modified
        (int32, 'version'),     # version
        (int32, 'cversion'),    # child version
        (int32, 'aversion'),    # acl version
        (int64, 'ephemeralOwner'), # owner id if ephemeral, 0 otw
        (int32, 'dataLength'),  #length of the data in the node
        (int32, 'numChildren'), #number of children of this node
        (int64, 'pzxid'),      # last modified children
        name = 'Stat',
        padding = 1
    )
# information explicitly stored by the server persistently
StatPersisted = nstruct(
        (int64, 'czxid'),      # created zxid
        (int64, 'mzxid'),      # last modified zxid
        (int64, 'ctime'),      # created
        (int64, 'mtime'),      # last modified
        (int32, 'version'),     # version
        (int32, 'cversion'),    # child version
        (int32, 'aversion'),    # acl version
        (int64, 'ephemeralOwner'), # owner id if ephemeral, 0 otw
        (int64, 'pzxid'),      # last modified children
        name = 'StatPersisted',
        padding = 1
    )

# information explicitly stored by the version 1 database of servers 
StatPersistedV1 = nstruct(
       (int64, 'czxid'), #created zxid
       (int64, 'mzxid'), #last modified zxid
       (int64, 'ctime'), #created
       (int64, 'mtime'), #last modified
       (int32, 'version'), #version
       (int32, 'cversion'), #child version
       (int32, 'aversion'), #acl version
       (int64, 'ephemeralOwner'), #owner id if ephemeral. 0 otw
       name = 'StatPersistedV1',
       padding = 1
    )

def _pack_zookeeper_length(x):
    x.length = len(x) - 4

# Every message begin with 32-bit length, excluding the field it self
ZooKeeperRequest = nstruct(
            (uint32, 'length'),
            name = 'ZooKeeperRequest',
            padding = 1,
            size = lambda x: x.length + 4,
            prepack = _pack_zookeeper_length
        )

# ZooKeeper types parsing depends on the parser state, set the "hidden field" and use _autosubclass

_TypedZooKeeperRequest = nstruct(
            name = '_TypedZooKeeperRequest',
            base = ZooKeeperRequest,
            criteria = lambda x: hasattr(x, 'zookeeper_type')
            )

ZooKeeperReply = nstruct(
            (uint32, 'length'),
            name = 'ZooKeeperRequest',
            padding = 1,
            size = lambda x: x.length + 4,
            prepack = _pack_zookeeper_length
        )

_TypedZooKeeperReply = nstruct(
            name = '_TypedZooKeeperReply',
            base = ZooKeeperReply,
            criteria = lambda x: hasattr(x, 'zookeeper_type')
        )

CONNECT_PACKET = 0x1

ConnectRequest = nstruct(
        (int32, 'protocolVersion'),
        (int64, 'lastZxidSeen'),
        (int32, 'timeOut'),
        (int64, 'sessionId'),
        (z_buffer, 'passwd'),
        (boolean, 'readOnly'),
        name = 'ConnectRequest',
        base = _TypedZooKeeperRequest,
        criteria = lambda x: x.zookeeper_type == CONNECT_PACKET,
        init = packvalue(CONNECT_PACKET, 'zookeeper_type')
    )

_ConnectResponseOptional = nstruct(
        name = '_ConnectResponseOptional',
        padding = 1,
        inline = False
    )

_ConnectResponseReadOnly = nstruct(
        (boolean, 'readOnly'),
        name = '_ConnectResponseReadOnly',
        base = _ConnectResponseOptional,
        criteria = lambda x: x._realsize() > 0
    )

ConnectResponse = nstruct(
        (int32, 'protocolVersion'),
        (int32, 'timeOut'),
        (int64, 'sessionId'),
        (z_buffer, 'passwd'),
        (_ConnectResponseOptional,),
        name = 'ConnectResponse',
        base = _TypedZooKeeperReply,
        criteria = lambda x: x.zookeeper_type == CONNECT_PACKET,
        init = packvalue(CONNECT_PACKET, 'zookeeper_type'),
        lastextra = True
    )


# All the other requests and responses are prepended by a header

HEADER_PACKET = 0x2

RequestHeader = nstruct(
        (zk_xid, 'xid'),
        (zk_request_type, 'type'),
        name = 'RequestHeader',
        base = _TypedZooKeeperRequest,
        criteria = lambda x: x.zookeeper_type != CONNECT_PACKET,
        init = packvalue(HEADER_PACKET, 'zookeeper_type'),
        classifier = lambda x: x.type
    )

ReplyHeader = nstruct(
        (int32, 'xid'),
        (int64, 'zxid'),
        (zk_err, 'err'),
        name = 'ReplyHeader',
        base = _TypedZooKeeperReply,
        criteria = lambda x: x.zookeeper_type != CONNECT_PACKET,
        init = packvalue(HEADER_PACKET, 'zookeeper_type')
    )

# ZooKeeper reply type must be determined by the request type

_TypedReply = nstruct(
        name = '_TypedReply',
        base = ReplyHeader,
        criteria = lambda x: hasattr(x, 'zookeeper_request_type') and (x.err == 0 or len(x) > 20),
        classifier = lambda x: x.zookeeper_request_type
    )

SetWatches = nstruct(
        (int64, 'relativeZxid'),
        (vector(ustring), 'dataWatches'),
        (vector(ustring), 'existWatches'),
        (vector(ustring), 'childWatches'),
        name = 'SetWatches',
        base = RequestHeader,
        classifyby = (ZOO_SETWATCHES_OP,),
        init = lambda x: (packvalue(ZOO_SETWATCHES_OP, 'type')(x),
                          packvalue(SET_WATCHES_XID, 'xid')(x))
    )

AuthPacket = nstruct(
        (int32, 'auth_type'),             # This is not used, always 0
        (ustring, 'scheme'),
        (z_buffer, 'auth'),
        name = 'AuthPacket',
        base = RequestHeader,
        classifyby = (ZOO_SETAUTH_OP,),
        init = lambda x: (packvalue(ZOO_SETAUTH_OP, 'type')(x),
                          packvalue(AUTH_XID, 'xid')(x))
    )

MultiHeader = nstruct(
        (zk_request_type, 'type'),
        (boolean, 'done'),
        (zk_err, 'err'),
        name = 'MultiHeader',
        padding = 1
    )

_GetDataRequest = nstruct(
        (ustring, 'path'),
        (boolean, 'watch'),
        name = '_GetDataRequest',
        padding = 1
    )

_SetDataRequest = nstruct(
        (ustring, 'path'),
        (z_buffer, 'data'),
        (int32, 'version'),
        name = '_SetDataRequest',
        padding = 1
    )

_SetDataResponse = nstruct(
        (Stat, 'stat'),
        name = '_SetDataResponse',
        padding = 1
    )

_GetSASLRequest = nstruct(
        (z_buffer, 'token'),
        name = '_GetSASLRequest',
        padding = 1
    )

_SetSASLRequest = nstruct(
        (z_buffer, 'token'),
        name = '_SetSASLRequest',
        padding = 1
    )

_SetSASLResponse = nstruct(
        (z_buffer, 'token'),
        name = '_SetSASLResponse',
        padding = 1
    )

_CreateRequest = nstruct(
        (ustring, 'path'),
        (z_buffer, 'data'),
        (vector(ACL), 'acl'),
        (zk_create_flag, 'flags'),
        name = '_CreateRequest',
        padding = 1
    )

_DeleteRequest = nstruct(
        (ustring, 'path'),
        (int32, 'version'),
        name = '_DeleteRequest',
        padding = 1
    )

_GetChildrenRequest = nstruct(
        (ustring, 'path'),
        (boolean, 'watch'),
        name = '_GetChildrenRequest',
        padding = 1
    )

_GetChildren2Request = nstruct(
        (ustring, 'path'),
        (boolean, 'watch'),
        name = '_GetChildren2Request',
        padding = 1
    )

_CheckVersionRequest = nstruct(
        (ustring, 'path'),
        (int32, 'version'),
        name = '_CheckVersionRequest',
        padding = 1
    )

_GetMaxChildrenRequest = nstruct(
        (ustring, 'path'),
        name = '_GetMaxChildrenRequest',
        padding = 1
    )

_GetMaxChildrenResponse = nstruct(
        (int32, 'max'),
        name = '_GetMaxChildrenResponse',
        padding = 1
    )

_SetMaxChildrenRequest = nstruct(
        (ustring, 'path'),
        (int32, 'max'),
        name = '_SetMaxChildrenRequest',
        padding = 1
    )

_SyncRequest = nstruct(
        (ustring, 'path'),
        name = '_SyncRequest',
        padding = 1
    )

_SyncResponse = nstruct(
        (ustring, 'path'),
        name = '_SyncResponse',
        padding = 1
    )

_GetACLRequest = nstruct(
        (ustring, 'path'),
        name = '_GetACLRequest',
        padding = 1
    )

_SetACLRequest = nstruct(
        (ustring, 'path'),
        (vector(ACL), 'acl'),
        (int32, 'version'),
        name = '_SetACLRequest',
        padding = 1
    )

_SetACLResponse = nstruct(
        (Stat, 'stat'),
        name = '_SetACLResponse',
        padding = 1
    )

_WatcherEvent = nstruct(
        (zk_watch_event, 'type'),  # event type
        (zk_client_state, 'state'), # state of the Keeper client runtime
        (ustring, 'path'),
        name = '_WatcherEvent',
        padding = 1
    )

_ErrorResponse = nstruct(
        (zk_err, 'err2'),
        name = '_ErrorResponse',
        padding = 1
    )

_CreateResponse = nstruct(
        (ustring, 'path'),
        name = '_CreateResponse',
        padding = 1
    )

_ExistsRequest = nstruct(
        (ustring, 'path'),
        (boolean, 'watch'),
        name = '_ExistsRequest',
        padding = 1
    )

_ExistsResponse = nstruct(
        (Stat, 'stat'),
        name = '_ExistsResponse',
        padding = 1
    )

_GetDataResponse = nstruct(
        (z_buffer, 'data'),
        (Stat, 'stat'),
        name = '_GetDataResponse',
        padding = 1
    )

_GetChildrenResponse = nstruct(
        (vector(ustring), 'children'),
        name = '_GetChildrenResponse',
        padding = 1
    )

_GetChildren2Response = nstruct(
        (vector(ustring), 'children'),
        (Stat, 'stat'),
        name = '_GetChildren2Response',
        padding = 1
    )

_GetACLResponse = nstruct(
        (vector(ACL), 'acl'),
        (Stat, 'stat'),
        name = '_GetACLResponse',
        padding = 1
    )

# Requests

CreateRequest = nstruct(
        (_CreateRequest,),
        name = 'CreateRequest',
        base = RequestHeader,
        init = packvalue(ZOO_CREATE_OP, 'type'),
        classifyby = (ZOO_CREATE_OP,)
    )

CreateResponse = nstruct(
        (_CreateResponse,),
        name = 'CreateResponse',
        base = _TypedReply,
        init = packvalue(ZOO_CREATE_OP, 'zookeeper_request_type'),
        classifyby = (ZOO_CREATE_OP,)
    )

DeleteRequest = nstruct(
        (_DeleteRequest,),
        name = 'DeleteRequest',
        base = RequestHeader,
        init = packvalue(ZOO_DELETE_OP, 'type'),
        classifyby = (ZOO_DELETE_OP,)
    )

# DeleteReply = ReplyHeader

ExistsRequest = nstruct(
        (_ExistsRequest,),
        name = 'ExistsRequest',
        base = RequestHeader,
        init = packvalue(ZOO_EXISTS_OP, 'type'),
        classifyby = (ZOO_EXISTS_OP,)
    )

ExistsResponse = nstruct(
        (_ExistsResponse,),
        name = 'ExistsResponse',
        base = _TypedReply,
        init = packvalue(ZOO_EXISTS_OP, 'zookeeper_request_type'),
        classifyby = (ZOO_EXISTS_OP,)
    )

GetDataRequest = nstruct(
        (_GetDataRequest,),
        base = RequestHeader,
        name = 'GetDataRequest',
        init = packvalue(ZOO_GETDATA_OP, 'type'),
        classifyby = (ZOO_GETDATA_OP,)
    )

GetDataResponse = nstruct(
        (_GetDataResponse,),
        base = _TypedReply,
        name = 'GetDataResponse',
        init = packvalue(ZOO_GETDATA_OP, 'zookeeper_request_type'),
        classifyby = (ZOO_GETDATA_OP,)
    )

SetDataRequest = nstruct(
        (_SetDataRequest,),
        base = RequestHeader,
        name = 'SetDataRequest',
        init = packvalue(ZOO_SETDATA_OP, 'type'),
        classifyby = (ZOO_SETDATA_OP,)
    )

SetDataResponse = nstruct(
        (_SetDataResponse,),
        base = _TypedReply,
        name = 'SetDataResponse',
        init = packvalue(ZOO_SETDATA_OP, 'zookeeper_request_type'),
        classifyby = (ZOO_SETDATA_OP,)
    )

SetSASLRequest = nstruct(
        (_SetSASLRequest,),
        base = RequestHeader,
        name = 'SetSASLRequest',
        init = packvalue(ZOO_SASL_OP, 'type'),
        classifyby = (ZOO_SASL_OP,)
    )

GetSASLRequest = SetSASLRequest

SetSASLResponse = nstruct(
        (_SetSASLResponse,),
        base = _TypedReply,
        name = 'SetSASLResponse',
        init = packvalue(ZOO_SASL_OP, 'zookeeper_request_type'),
        classifyby = (ZOO_SASL_OP,)
    )

GetSASLResponse = SetSASLResponse

GetACLRequest = nstruct(
        (_GetACLRequest,),
        base = RequestHeader,
        name = 'GetACLRequest',
        init = packvalue(ZOO_GETACL_OP, 'type'),
        classifyby = (ZOO_GETACL_OP,)
    )

GetACLResponse = nstruct(
        (_GetACLResponse,),
        base = _TypedReply,
        name = 'GetACLResponse',
        init = packvalue(ZOO_GETACL_OP, 'zookeeper_request_type'),
        classifyby = (ZOO_GETACL_OP,)
    )

SetACLRequest = nstruct(
        (_SetACLRequest,),
        base = RequestHeader,
        name = 'SetACLRequest',
        init = packvalue(ZOO_SETACL_OP, 'type'),
        classifyby = (ZOO_SETACL_OP,)
    )

SetACLResponse = nstruct(
        (_SetACLResponse,),
        base = _TypedReply,
        name = 'SetACLResponse',
        init = packvalue(ZOO_SETACL_OP, 'zookeeper_request_type'),
        classifyby = (ZOO_SETACL_OP,)
    )

GetChildrenRequest = nstruct(
        (_GetChildrenRequest,),
        base = RequestHeader,
        name = 'GetChildrenRequest',
        init = packvalue(ZOO_GETCHILDREN_OP, 'type'),
        classifyby = (ZOO_GETCHILDREN_OP,)
    )

GetChildrenResponse = nstruct(
        (_GetChildrenResponse,),
        base = _TypedReply,
        name = 'GetChildrenResponse',
        init = packvalue(ZOO_GETCHILDREN_OP, 'zookeeper_request_type'),
        classifyby = (ZOO_GETCHILDREN_OP,)
    )

SyncRequest = nstruct(
        (_SyncRequest,),
        base = RequestHeader,
        name = 'SyncRequest',
        init = packvalue(ZOO_SYNC_OP, 'type'),
        classifyby = (ZOO_SYNC_OP,)
    )

SyncResponse = nstruct(
        (_SyncResponse,),
        base = _TypedReply,
        name = 'SyncResponse',
        init = packvalue(ZOO_SYNC_OP, 'zookeeper_request_type'),
        classifyby = (ZOO_SYNC_OP,)
    )

# Ping Request and Response have no body

GetChildren2Request = nstruct(
        (_GetChildren2Request,),
        base = RequestHeader,
        name = 'GetChildren2Request',
        init = packvalue(ZOO_GETCHILDREN2_OP, 'type'),
        classifyby = (ZOO_GETCHILDREN2_OP,)
    )

GetChildren2Response = nstruct(
        (_GetChildren2Response,),
        base = _TypedReply,
        name = 'GetChildren2Response',
        init = packvalue(ZOO_GETCHILDREN2_OP, 'zookeeper_request_type'),
        classifyby = (ZOO_GETCHILDREN2_OP,)
    )

MultiOpRequest = nvariant(
        'MultiOpRequest',
        MultiHeader,
        classifier = lambda x: x.type,
        padding = 1
    )

MultiOpResponse = nvariant(
        'MultiOpResponse',
        MultiHeader,
        classifier = lambda x: x.type,
        padding = 1
    )

MultiOpCheck = nstruct(
        (_CheckVersionRequest,),
        name = 'MultiOpCheck',
        base = MultiOpRequest,
        init = packvalue(ZOO_CHECK_OP, 'type'),
        classifyby = (ZOO_CHECK_OP,)
    )

# Check does not have response body

MultiOpCreate = nstruct(
        (_CreateRequest,),
        name = 'MultiOpCreate',
        base = MultiOpRequest,
        init = packvalue(ZOO_CREATE_OP, 'type'),
        classifyby = (ZOO_CREATE_OP,)
    )

MultiOpCreateResponse = nstruct(
        (_CreateResponse,),
        name = 'MultiOpCreateResponse',
        base = MultiOpResponse,
        init = packvalue(ZOO_CREATE_OP, 'type'),
        classifyby = (ZOO_CREATE_OP,)
    )

MultiOpDelete = nstruct(
        (_DeleteRequest,),
        name = 'MultiOpDelete',
        base = MultiOpRequest,
        init = packvalue(ZOO_DELETE_OP, 'type'),
        classifyby = (ZOO_DELETE_OP,)
    )

MultiOpSetData = nstruct(
        (_SetDataRequest,),
        name = 'MultiOpSetData',
        base = MultiOpRequest,
        init = packvalue(ZOO_SETDATA_OP, 'type'),
        classifyby = (ZOO_SETDATA_OP,)
    )

MultiOpSetDataResponse = nstruct(
        (_SetDataResponse,),
        name = 'MultiOpSetDataResponse',
        base = MultiOpResponse,
        init = packvalue(ZOO_SETDATA_OP, 'type'),
        classifyby = (ZOO_SETDATA_OP,)
    )

MultiOpErrorResponse = nstruct(
        (_ErrorResponse,),
        name = 'MultiOpErrorResponse',
        base = MultiOpResponse,
        init = packvalue(ZOO_ERROR_TYPE, 'type'),
        classifyby = (ZOO_ERROR_TYPE,)
    )

MultiRequest = nstruct(
        (MultiOpRequest[0], 'requests'),
        name = 'MultiRequest',
        base = RequestHeader,
        classifyby = (ZOO_MULTI_OP,),
        init = packvalue(ZOO_MULTI_OP, 'type')
    )



MultiResponse = nstruct(
        (MultiOpResponse[0], 'responses'),
        name = 'MultiResponse',
        base = _TypedReply,
        classifyby = (ZOO_MULTI_OP,),
        init = packvalue(ZOO_MULTI_OP, 'zookeeper_request_type')
    )

WatcherEvent = nstruct(
        (_WatcherEvent,),
        name = 'WatcherEvent',
        base = ReplyHeader,
        criteria = lambda x: x.xid == WATCHER_EVENT_XID,
        init = packvalue(WATCHER_EVENT_XID, 'xid')
    )

# Helpers

def default_acl():
    return ACL(perms = ZOO_PERM_ALL, id = Id(scheme = 'world', id = 'anyone'))

def multi(*ops):
    return MultiRequest(
                requests = list(ops) + \
                            [MultiOpRequest(type = -1, done = True, err = -1)]
                )

def create(path, data, ephemeral = False, sequence = False, acl = None):
    if acl is None:
        acl = [default_acl()]
    return CreateRequest(path = path, data = data, acl = acl,
                         flags = (ZOO_EPHEMERAL if ephemeral else 0) | (ZOO_SEQUENCE if sequence else 0))



def delete(path, version = -1):
    return DeleteRequest(path = path, version = -1)

def exists(path, watch = False):
    return ExistsRequest(path = path, watch = watch)

def getdata(path, watch = False):
    return GetDataRequest(path = path, watch = watch)

def setdata(path, data, version = -1):
    return SetDataRequest(path = path, data = data, version = -1)

def getchildren(path, watch = False):
    return GetChildrenRequest(path = path, watch = watch)

def getchildren2(path, watch = False):
    return GetChildren2Request(path = path, watch = watch)

def multi_create(path, data, ephemeral = False, sequence = False, acl = None):
    if acl is None:
        acl = [default_acl()]
    return MultiOpCreate(path = path, data = data, acl = acl,
                         flags = (ZOO_EPHEMERAL if ephemeral else 0) | (ZOO_SEQUENCE if sequence else 0))

def multi_delete(path, version = -1):
    return MultiOpDelete(path = path, version = -1)

def multi_check(path, version):
    return MultiOpCheck(path, version)

def multi_setdata(path, data, version = -1):
    return MultiOpSetData(path = path, data = data, version = -1)

def getacl(path):
    return GetACLRequest(path = path)

def setacl(path, acl, version = -1):
    return SetACLRequest(path = path, acl = acl, version = version)

def sync(path):
    return SyncRequest(path = path)

