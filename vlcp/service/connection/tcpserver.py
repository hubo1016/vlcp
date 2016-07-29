'''
Created on 2015/10/19

:author: hubo
'''
from vlcp.server.module import Module, api
from vlcp.event import TcpServer
from vlcp.event.runnable import RoutineContainer
from vlcp.event.connection import Client

class TcpServerBase(Module):
    '''
    Generic tcp server on specified URLs, vHosts are supported.
    '''
    _default_url = 'tcp:///'
    _default_connmanage = False
    _default_client = False
    service = True
    def _createprotocol(self, config):
        return self._protocolclass()
    def _createServers(self, config, vhostname, defaultconfig = {}, key = None, certificate = None, ca_certs = None, exists = {}, client = False):
        urls = list(getattr(config, 'urls', []))
        if hasattr(config, 'url') and config.url and config.url.strip():
            urls.append(config.url.strip())
        settings = dict(defaultconfig.items())
        if hasattr(config, 'protocol'):
            settings.update(config.protocol.items())
        key = getattr(config, 'key', key)
        certificate = getattr(config, 'certificate', certificate)
        ca_certs = getattr(config, 'ca_certs', ca_certs)
        client = getattr(config, 'client', client)
        if urls:
            defaultProtocol = self._createprotocol(config)
            if self.connmanage:
                # Patch init() and final()
                def patch(prococol):
                    orig_init = prococol.init
                    def init(conn):
                        for m in orig_init(conn):
                            yield m
                        self.managed_connections.add(conn)
                    prococol.init = init
                
                    orig_final = prococol.final
                    def final(conn):
                        try:
                            self.managed_connections.remove(conn)
                        except Exception:
                            pass
                        for m in orig_final(conn):
                            yield m
                    prococol.final = final
                patch(defaultProtocol)
            defaultProtocol.vhost = vhostname
            # Copy extra configurations to protocol
            for k,v in settings:
                setattr(defaultProtocol, k, v)
            for url in urls:
                if (vhostname, url) in exists:
                    exists.remove((vhostname, url))
                else:
                    if client:
                        self.connections.append(self._client_class(config, defaultProtocol, vhostname)(url, defaultProtocol, self.scheduler,
                                                       key, certificate, ca_certs, getattr(config, 'bindaddress', None)))
                    else:
                        self.connections.append(self._server_class(config, defaultProtocol, vhostname)(url, defaultProtocol, self.scheduler,
                                                      key, certificate, ca_certs))
        if hasattr(config, 'vhost'):
            for k,v in config.vhost.items():
                self._createServers(v, k, settings, key, certificate, ca_certs, exists, client)
    def _client_class(self, config, protocol, vhost):
        return Client
    def _server_class(self, config, protocol, vhost):
        return TcpServer
    def __init__(self, server, protocolclass):
        Module.__init__(self, server)
        self._protocolclass = protocolclass
        self.apiroutine = RoutineContainer(self.scheduler)
        self.managed_connections = set()
        self._createServers(self, '')
        self.createAPI(api(self.getservers),
                       api(self.stoplisten, self.apiroutine),
                       api(self.startlisten, self.apiroutine),
                       api(self.updateconfig, self.apiroutine),
                       api(self.getconnections))
    def unload(self, container, force=False):
        if self.connmanage:
            self.connections.extend(self.managed_connections)
            self.managed_connections.clear()
        for m in Module.unload(self, container, force=force):
            yield m
    def getservers(self, vhost = None):
        '''
        Return current servers
        :param vhost: return only servers of vhost if specified. '' to return only default servers.
                      None for all servers.
        '''
        if vhost is not None:
            return [s for s in self.connections if s.protocol.vhost == vhost]
        else:
            return list(self.connections)
    def stoplisten(self, vhost = None):
        '''
        Stop listen on current servers
        :param vhost: return only servers of vhost if specified. '' to return only default servers.
                      None for all servers.
        '''
        servers = self.getservers(vhost)
        for s in servers:
            for m in s.stoplisten():
                yield m
        self.apiroutine.retvalue = len(servers)
    def startlisten(self, vhost = None):
        '''
        Start listen on current servers
        :param vhost: return only servers of vhost if specified. '' to return only default servers.
                      None for all servers.
        '''
        servers = self.getservers(vhost)
        for s in servers:
            for m in s.startlisten():
                yield m
        self.apiroutine.retvalue = len(servers)
    def updateconfig(self):
        "Reload configurations, remove non-exist servers, add new servers, and leave others unchanged"
        exists = {}
        for s in self.connections:
            exists[(s.protocol.vhost, s.rawurl)] = s
        self._createServers(self, '', exists = exists)
        for _,v in exists.items():
            for m in v.shutdown():
                yield m
            self.connections.remove(v)
        self.apiroutine.retvalue = None
    def getconnections(self, vhost = None):
        "Return accepted connections, optionally filtered by vhost"
        if vhost is None:
            return list(self.managed_connections)
        else:
            return [c for c in self.managed_connections if c.protocol.vhost == vhost]
