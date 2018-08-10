'''
Created on 2015/10/19

:author: hubo
'''
from vlcp.config import defaultconfig
from vlcp.protocol.http import Http, HttpRequestEvent
from vlcp.utils.http import statichttp
from vlcp.event.stream import MemoryStream
from vlcp.service.connection.tcpserver import TcpServerBase
from vlcp.event.event import M_

@defaultconfig
class HttpServer(TcpServerBase):
    '''
    Create HTTP server on specified URLs, vHosts are supported.
    '''
    def __init__(self, server):
        TcpServerBase.__init__(self, server, Http)
        # Default Handlers
        @statichttp(self.apiroutine)
        async def default404(env):
            await env.error(404, showerror = False)
        @statichttp(self.apiroutine)
        async def options(env):
            env.output(MemoryStream(b''))
        async def main():
            om = HttpRequestEvent.createMatcher(None, b'*', b'OPTIONS')
            dm = HttpRequestEvent.createMatcher()
            while True:
                ev, m = await M_(om, dm)
                if not ev.canignore:
                    ev.canignore = True
                    if m is om:
                        self.apiroutine.subroutine(options(ev), False)
                    else:
                        self.apiroutine.subroutine(default404(ev), False)
        self.apiroutine.main = main
        self.routines.append(self.apiroutine)
    