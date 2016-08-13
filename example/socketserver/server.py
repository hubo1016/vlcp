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

_logger = logging.getLogger('MyServer')

def _service(type, version = None):
    def wrapper(func):
        @wraps(func)
        def service_func(self):
            def process_routine(event):
                try:
                    conn = event.connection
                    msg = event.message
                    #_logger.debug('Request received: %r' % (dump(msg),))
                    try:
                        for m in func(self, msg):
                            yield m
                        reply = self.retvalue
                        # Precheck if there are any serialization problems
                        reply._tobytes()
                        #_logger.debug('Send reply: %r' % (dump(reply),))
                    except Exception as exc:
                        _logger.warning('Request process failed:', exc_info = True)
                        for m in conn.protocol.reply_to(conn,
                                                        d.message_error(
                                                            err_type = d.RUNTIME_ERROR,
                                                            details = str(exc).encode('utf-8')
                                                        ),
                                                        msg,
                                                        self):
                            yield m
                    else:
                        for m in conn.protocol.reply_to(conn,
                                                        reply,
                                                        msg,
                                                        self):
                            yield m
                except (IOError, ConnectionResetException):
                    pass
                except Exception:
                    _logger.warning('Unexpected error:', exc_info = True)
            _logger.info('Start service, type = %r, version = %r', type, version)
            request_matcher = MyProtocolRequestEvent.createMatcher(type, version)
            while True:
                yield (request_matcher,)
                if getattr(self.event, '_processed', False):
                    continue
                self.event._processed = True
                self.subroutine(process_routine(self.event))
        service_func._is_service = True
        return service_func
    return wrapper

class MyServer(RoutineContainer):
    def main(self):
        _logger.info('Server started')
        routines = []
        # Auto start all services
        for k in dir(self):
            v = getattr(self, k)
            if hasattr(v, '__func__') and getattr(v.__func__, '_is_service', False):
                routines.append(self.subroutine(v()))
        try:
            yield ()
        finally:
            for r in routines:
                r.close()
    @_service(None, None)
    def default_error(self, message):
        self.retvalue = d.message_error(err_type = d.UNSUPPORTED_REQUEST,
                                        details = b'Unsupported request type: %d' % (message.type,))
        if False:
            yield
    @_service(d.SUM_REQUEST, d.MESSAGE_VERSION_10)
    def sum_request(self, message):
        r = sum(message.numbers)
        if r < -0x80000000 or r > 0x7fffffff:
            self.retvalue = d.message_error(err_type = d.PARAMETER_ERROR,
                                            details = b'Sum is out of range')
        else:
            self.retvalue = d.message10_sum_reply(result = r)
        if False:
            yield

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
