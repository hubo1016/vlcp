'''
Created on 2015/10/19

:author: hubo
'''
from vlcp.config import defaultconfig
from vlcp.protocol.http import Http, HttpRequestEvent
from vlcp.utils.http import statichttp
from vlcp.event.stream import MemoryStream
from vlcp.service.connection.tcpserver import TcpServerBase

@defaultconfig
class HttpServer(TcpServerBase):
    '''
    Create HTTP server on specified URLs, vHosts are supported.
    '''
    def __init__(self, server):
        TcpServerBase.__init__(self, server, Http)
        # Default Handlers
        @statichttp(self.apiroutine)
        def default404(env):
            for m in env.error(404, showerror = False):
                yield m
        @statichttp(self.apiroutine)
        def options(env):
            env.output(MemoryStream(b''))
            if False:
                yield
        def main():
            om = HttpRequestEvent.createMatcher(None, b'*', b'OPTIONS')
            dm = HttpRequestEvent.createMatcher()
            while True:
                yield (om, dm)
                if not self.apiroutine.event.canignore:
                    self.apiroutine.event.canignore = True
                    if self.apiroutine.matcher is om:
                        self.apiroutine.subroutine(options(self.apiroutine.event), False)
                    else:
                        self.apiroutine.subroutine(default404(self.apiroutine.event), False)
        self.apiroutine.main = main
        self.routines.append(self.apiroutine)
    