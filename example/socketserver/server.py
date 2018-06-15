from __future__ import print_function
from vlcp.server.module import Module
from vlcp.event import RoutineContainer, TcpServer
from vlcp.event.connection import ConnectionResetException
from myprotocol import MyProtocol, MyProtocolRequestEvent
from vlcp.config import defaultconfig
from functools import wraps
import protocoldef as d
import logging
from namedstruct import dump
from vlcp.event.event import M_

_logger = logging.getLogger('MyServer')

def _service(type, version = None):
    def wrapper(func):
        @wraps(func)
        async def service_func(self):
            async def process_routine(event):
                try:
                    conn = event.connection
                    msg = event.message
                    #_logger.debug('Request received: %r' % (dump(msg),))
                    try:
                        reply = await func(self, msg)
                        await conn.protocol.reply_to(conn,
                                                    reply,
                                                    msg,
                                                    self)
                        #_logger.debug('Send reply: %r' % (dump(reply),))
                    except (IOError, ConnectionResetException):
                        pass
                    except Exception as exc:
                        _logger.warning('Request process failed:', exc_info = True)
                        await conn.protocol.reply_to(conn,
                                                    d.message_error(
                                                        err_type = d.RUNTIME_ERROR,
                                                        details = str(exc).encode('utf-8')
                                                    ),
                                                    msg,
                                                    self)
                except (IOError, ConnectionResetException):
                    pass
                except Exception:
                    _logger.warning('Unexpected error:', exc_info = True)
            _logger.info('Start service, type = %r, version = %r', type, version)
            request_matcher = MyProtocolRequestEvent.createMatcher(type, version)
            while True:
                ev = await request_matcher
                if getattr(ev, '_processed', False):
                    continue
                ev._processed = True
                self.subroutine(process_routine(ev))
        service_func._is_service = True
        return service_func
    return wrapper

class MyServer(RoutineContainer):
    async def main(self):
        _logger.info('Server started')
        routines = []
        # Auto start all services
        for k in dir(self):
            v = getattr(self, k)
            if hasattr(v, '__func__') and getattr(v.__func__, '_is_service', False):
                routines.append(self.subroutine(v()))
        try:
            await M_()
        finally:
            for r in routines:
                r.close()
    @_service(None, None)
    async def default_error(self, message):
        return d.message_error(err_type = d.UNSUPPORTED_REQUEST,
                                details = b'Unsupported request type: %d' % (message.type,))
    @_service(d.SUM_REQUEST, d.MESSAGE_VERSION_10)
    async def sum_request(self, message):
        r = sum(message.numbers)
        if r < -0x80000000 or r > 0x7fffffff:
            return d.message_error(err_type = d.PARAMETER_ERROR,
                                            details = b'Sum is out of range')
        else:
            return d.message10_sum_reply(result = r)


@defaultconfig
class MyProtocolServer(Module):
    _default_url = 'tcp://127.0.0.1/'
    def __init__(self, server):
        Module.__init__(self, server)
        # Should consider to use a subclass of vlcp.service.connection.tcpserver.TcpServerBase
        self.connections.append(TcpServer(self.url, MyProtocol(), self.scheduler))
        self.routines.append(MyServer(self.scheduler))

if __name__ == '__main__':
    import logging
    logging.basicConfig()
    #logging.getLogger().setLevel(logging.DEBUG)
    from vlcp.server import main
    main()
