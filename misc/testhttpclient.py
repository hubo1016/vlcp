'''
Created on 2015/8/27

:author: hubo
'''
from __future__ import print_function
from vlcp.server import Server
from vlcp.event import RoutineContainer, Stream, TcpServer, MemoryStream, Client
from vlcp.protocol.http import Http, HttpRequestEvent, HttpConnectionStateEvent
from codecs import getincrementalencoder
import logging
from vlcp.event.event import M_

http = Http(False)

class MainRoutine(RoutineContainer):
    def __init__(self, scheduler=None, daemon=False):
        RoutineContainer.__init__(self, scheduler=scheduler, daemon=daemon)
    async def main(self):
        conn = Client('tcp://www.baidu.com/', http, self.scheduler)
        conn.start()
        connected = http.statematcher(conn, HttpConnectionStateEvent.CLIENT_CONNECTED, False)
        notconnected = http.statematcher(conn, HttpConnectionStateEvent.CLIENT_NOTCONNECTED, False)
        _, m = await M_(connected, notconnected)
        if m is notconnected:
            print('Connect to server failed.')
        else:
            _, http_responses = await http.request_with_response(self, conn, b'www.baidu.com', b'/', b'GET', [])
            for r in http_responses:
                print('Response received:')
                print(r.status)
                print()
                print('Headers:')
                for k,v in r.headers:
                    print('%r: %r' % (k,v))
                print()
                print('Body:')
                if r.stream is None:
                    print('<Empty>')
                else:
                    try: 
                        while True:
                            data = await r.stream.read(self, 1024)
                            print(data, end = '')
                    except EOFError:
                        pass
                    print()
            _, http_responses = await http.request_with_response(self, conn, b'www.baidu.com', b'/favicon.ico', b'GET', [], keepalive = False)
            for r in http_responses:
                print('Response received:')
                print(r.status)
                print()
                print('Headers:')
                for k,v in r.headers:
                    print('%r: %r' % (k,v))
                print()
                print('Body:')
                if r.stream is None:
                    print('<Empty>')
                else:
                    data = await r.stream.read(self)
                    print('<Data: %d bytes>' % (len(data),))

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
    s.serve()
    
