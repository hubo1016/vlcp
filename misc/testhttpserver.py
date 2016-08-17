'''
Created on 2015/8/27

:author: hubo
'''
from pprint import pprint
from vlcp.server import Server
from vlcp.event import RoutineContainer, Stream, TcpServer, MemoryStream
from vlcp.protocol.http import Http, HttpRequestEvent, escape_b, escape
from codecs import getincrementalencoder
import logging

http = Http(True)

class MainRoutine(RoutineContainer):
    def __init__(self, scheduler=None, daemon=False):
        RoutineContainer.__init__(self, scheduler=scheduler, daemon=daemon)
        self.encoder = getincrementalencoder('utf-8')
    def main(self):
        request = HttpRequestEvent.createMatcher()
        while True:
            yield (request,)            
            self.event.canignore = True
            self.subroutine(self.handlehttp(self.event))
    document = '''
<!DOCTYPE html >
<html>
<head>
<title>Test Server Page</title>
</head>
<body>
OK!<br/>
Host = %s<br/>
Path = %s<br/>
Headers = %s<br/>
</body>
</html>
'''
    def formatstr(self, tmpl, params):
        if not isinstance(tmpl, bytes):
            # Python 3
            return (tmpl % tuple(v.decode('utf-8') for v in params)).encode('utf-8')
        else:
            return (tmpl % params)
    def handlehttp(self, event):
        if event.stream is not None:
            event.stream.close(self.scheduler)
        if event.connmark == event.connection.connmark and event.connection.connected:
            #outputstream = Stream()
            document = self.formatstr(self.document, (escape_b(event.host), escape_b(event.path), escape(repr(event.headers)).encode('utf-8')))
            outputstream = MemoryStream(document)
            http.startResponse(event.connection, event.xid, b'200 OK', [], outputstream)
            #for m in outputstream.write(self.document, self, True):
                #yield m
            if False:
                yield

if __name__ == '__main__':
    logging.basicConfig()
    s = Server()
    #s.scheduler.debugging = True
    #s.scheduler.logger.setLevel(logging.DEBUG)
    #Http.debugging = True
    #Http._logger.setLevel(logging.DEBUG)
    http.createmessagequeue(s.scheduler)
    routine = MainRoutine(s.scheduler)
    routine.start()
    mgt_conn = TcpServer('ltcp://0.0.0.0:8080/', http, s.scheduler)
    mgt_conn.start()
    s.serve()
    
