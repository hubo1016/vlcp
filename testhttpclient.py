'''
Created on 2015/8/27

@author: hubo
'''
from __future__ import print_function
from server import Server
from event import RoutineContainer, Stream, TcpServer, MemoryStream, Client
from protocol.http import Http, HttpRequestEvent, HttpConnectionStateEvent
from codecs import getincrementalencoder
import logging

http = Http(False)

class MainRoutine(RoutineContainer):
    def __init__(self, scheduler=None, daemon=False):
        RoutineContainer.__init__(self, scheduler=scheduler, daemon=daemon)
    def main(self):
        conn = Client('tcp://www.baidu.com/', http, self.scheduler)
        conn.start()
        connected = http.statematcher(conn, HttpConnectionStateEvent.CLIENT_CONNECTED, False)
        notconnected = http.statematcher(conn, HttpConnectionStateEvent.CLIENT_NOTCONNECTED, False)
        yield (connected, notconnected)
        if self.matcher is notconnected:
            print('Connect to server failed.')
        else:
            for m in http.requestwithresponse(self, conn, b'www.baidu.com', b'/', b'GET', []):
                yield m
            for r in self.http_responses:
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
                            for m in r.stream.read(self, 1024):
                                yield m
                            print(self.data, end = '')
                    except EOFError:
                        pass
                    print()
            for m in http.requestwithresponse(self, conn, b'www.baidu.com', b'/favicon.ico', b'GET', [], keepalive = False):
                yield m
            for r in self.http_responses:
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
                    for m in r.stream.read(self):
                        yield m
                    print('<Data: %d bytes>' % (len(self.data),))

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
    
