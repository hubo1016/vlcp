'''
Created on 2015/12/25

:author: hubo
'''
from vlcp.config import defaultconfig
from vlcp.protocol.jsonrpc import JsonRPC
from vlcp.service.connection.tcpserver import TcpServerBase
from vlcp.protocol.ovsdb import OVSDB

@defaultconfig
class JsonRPCServer(TcpServerBase):
    '''
    Create JsonRPC server on specified URLs, vHosts are supported.
    '''
    # Enable connection management
    _default_connmanage = True
    def __init__(self, server):
        TcpServerBase.__init__(self, server, JsonRPC)
    def _createprotocol(self, config):
        if getattr(config, 'ovsdb', False):
            return OVSDB()
        else:
            return JsonRPC()
