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
    async def main(self):
        conn_up = MyProtocolConnectionStateEvent.createMatcher(MyProtocolConnectionStateEvent.UP)
        ev = await conn_up
        conn = ev.connection
        requests = [conn.protocol.request(conn, d.message10_sum_request(numbers = list(range(i,100+i))), self)
                                  for i in range(0, 10000)]
        async def subr(k):
            r = []
            for i in range(k, len(requests), 20):
                r.append(await requests[i])
            return r
        start = time()
        result = await self.execute_all([subr(i) for i in range(0,20)])
        end = time()
        results = [r for r0 in result
                   for r in r0]
        print('%d results in %f secs, %d errors' % (len(results), end-start, len([m for m in results if m.type == d.ERROR])))
        await conn.shutdown()


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
