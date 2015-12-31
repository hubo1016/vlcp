'''
Created on 2015/12/25

:author: hubo
'''
from vlcp.config import defaultconfig
from vlcp.protocol.jsonrpc import JsonRPC
from vlcp.service.connection.tcpserver import TcpServerBase

@defaultconfig
class JsonRPCServer(TcpServerBase):
    '''
    Create HTTP server on specified URLs, vHosts are supported.
    '''
    def __init__(self, server):
        TcpServerBase.__init__(self, server, JsonRPC)
