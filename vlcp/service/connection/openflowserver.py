'''
Created on 2015/12/25

:author: hubo
'''
from vlcp.config import defaultconfig
from vlcp.protocol.openflow import Openflow
from vlcp.service.connection.tcpserver import TcpServerBase

@defaultconfig
class OpenflowServer(TcpServerBase):
    '''
    Create OpenFlow server on specified URLs, vHosts are supported.
    '''
    # Enable connection management
    _default_connmanage = True
    def __init__(self, server):
        TcpServerBase.__init__(self, server, Openflow)
