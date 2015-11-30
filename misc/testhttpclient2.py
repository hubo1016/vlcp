'''
Created on 2015/8/27

@author: hubo
'''
from __future__ import print_function
from vlcp.server import main
from vlcp.server.module import Module
from vlcp.event import RoutineContainer
from vlcp.utils.webclient import WebClient
from vlcp.config.config import manager
import re
from vlcp.event.core import SystemControlLowPriorityEvent
from vlcp.protocol.http import HttpProtocolException

urlmatcher = re.compile(br'https?\://[a-zA-Z0-9%\-\._~\[\]\:\@]+/[a-zA-Z0-9%\-\._~/;+=&]+(?:\?[a-zA-Z0-9%\-\._~/;+&=]+)?')

class MainRoutine(RoutineContainer):
    def __init__(self, scheduler=None, daemon=False):
        RoutineContainer.__init__(self, scheduler=scheduler, daemon=daemon)
    def robot(self, wc, url, referer = None):
        if self.robotcount > 1000:
            raise StopIteration
        if url in self.urls:
            raise StopIteration
        headers = {}
        if referer:
            headers['Referer'] = referer
        self.urls.add(url)
        self.robotcount += 1
        try:
            for m in wc.urlopen(self, url, headers = headers, autodecompress = True, timeout = 30):
                yield m
        except (IOError, HttpProtocolException) as exc:
            print('Failed to open %r: %s' % (url, exc))
            raise StopIteration
        resp = self.retvalue
        try:
            if resp.get_header('Content-Type', 'text/html').lower().startswith('text/'):
                try:
                    for m in self.executeWithTimeout(60, resp.stream.read(self, 32768)):
                        yield m
                except Exception as exc:
                    print('Error reading ', url, str(exc))
                if not self.timeout:
                    data = self.data
                    for match in urlmatcher.finditer(data):
                        newurl = match.group()
                        self.subroutine(self.robot(wc, newurl, url), False)
                    print('Finished: ', url)
                else:
                    print('Read Timeout: ', url)
            else:
                print('Not a text type: ', url)
            for m in resp.shutdown():
                yield m
        finally:
            resp.close()
    def main(self):
        self.urls = set()
        wc = WebClient(True)
        for m in wc.urlopen(self, 'http://www.baidu.com/', autodecompress = True):
            yield m
        resp = self.retvalue
        print('Response received:')
        print(resp.fullstatus)
        print()
        print('Headers:')
        for k,v in resp.headers:
            print('%r: %r' % (k,v))
        print()
        print('Body:')
        if resp.stream is None:
            print('<Empty>')
        else:
            try: 
                while True:
                    for m in resp.stream.read(self, 1024):
                        yield m
                    #print(self.data, end = '')
            except EOFError:
                pass
            print(resp.connection.http_parsestage)
            print()
            for m in wc.urlopen(self, 'http://www.baidu.com/favicon.ico', autodecompress = True):
                yield m
            resp = self.retvalue
            print('Response received:')
            print(resp.fullstatus)
            print()
            print('Headers:')
            for k,v in resp.headers:
                print('%r: %r' % (k,v))
            print()
            print('Body:')
            if resp.stream is None:
                print('<Empty>')
            else:
                for m in resp.stream.read(self):
                    yield m
                print('<Data: %d bytes>' % (len(self.data),))
        self.robotcount = 0
        self.subroutine(self.robot(wc, 'http://www.baidu.com/'))

class MainModule(Module):
    def __init__(self, server):
        Module.__init__(self, server)
        self.routines.append(MainRoutine(self.scheduler))
    
if __name__ == '__main__':
    #manager['server.debugging'] = True
    main()
    
