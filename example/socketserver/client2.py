from __future__ import print_function
from vlcp.server.module import Module
from vlcp.event import RoutineContainer, Client
from myprotocol import MyProtocol, MyProtocolConnectionStateEvent
from vlcp.config import defaultconfig
import protocoldef as d
import logging
from namedstruct import dump
from time import time

class MyClient(RoutineContainer):
    def main(self):
        conn_up = MyProtocolConnectionStateEvent.createMatcher(MyProtocolConnectionStateEvent.UP)
        yield (conn_up,)
        conn = self.event.connection
        requests = [conn.protocol.request(conn, d.message10_sum_request(numbers = list(range(i,100+i))), self)
                                  for i in range(0, 10000)]
        def subr(k):
            r = []
            for i in range(k, len(requests), 20):
                for m in requests[i]:
                    yield m
                r.append(self.retvalue)
            self.retvalue = r
        start = time()
        for m in self.executeAll([subr(i) for i in range(0,20)]):
            yield m
        end = time()
        results = [r for r0 in self.retvalue
                   for r in r0[0]]
        print('%d results in %f secs, %d errors' % (len(results), end-start, len([m for m in results if m.type == d.ERROR])))
        for m in conn.shutdown():
            yield m

@defaultconfig
class MyProtocolClient(Module):
    _default_url = 'tcp://127.0.0.1/'
    def __init__(self, server):
        Module.__init__(self, server)
        # Should consider to use a subclass of vlcp.service.connection.tcpserver.TcpServerBase
        self.connections.append(Client(self.url, MyProtocol(), self.scheduler))
        self.routines.append(MyClient(self.scheduler))

if __name__ == '__main__':
    import logging
    logging.basicConfig()
    #logging.getLogger().setLevel(logging.DEBUG)
    from vlcp.server import main
    main()
