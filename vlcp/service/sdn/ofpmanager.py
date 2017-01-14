'''
Created on 2016/2/19

:author: hubo
'''

from vlcp.config import defaultconfig
from vlcp.server.module import Module, api, depend, callAPI, ModuleNotification
from vlcp.event.runnable import RoutineContainer
from vlcp.service.connection import openflowserver
from vlcp.protocol.openflow import OpenflowConnectionStateEvent
from vlcp.event.connection import ConnectionResetException, ResolveRequestEvent,\
    ResolveResponseEvent
import itertools
import socket
from vlcp.event.event import Event, withIndices
from vlcp.event.core import QuitException, syscall_removequeue

def _get_endpoint(conn):
    raddr = getattr(conn, 'remoteaddr', None)
    if raddr:
        if isinstance(raddr, tuple):
            # Ignore port
            return raddr[0]
        else:
            # Unix socket
            return raddr
    else:
        return ''

@withIndices()
class TableAcquireUpdate(Event):
    pass

@withIndices('connection', 'datapathid', 'vhost')
class FlowInitialize(Event):
    pass

@withIndices()
class TableAcquireDelayEvent(Event):
    pass

@defaultconfig
@depend(openflowserver.OpenflowServer)
class OpenflowManager(Module):
    '''
    Manage Openflow Connections
    '''
    service = True
    _default_vhostbind = None
    def __init__(self, server):
        Module.__init__(self, server)
        self.apiroutine = RoutineContainer(self.scheduler)
        self.apiroutine.main = self._manage_conns
        self.routines.append(self.apiroutine)
        self.managed_conns = {}
        self.endpoint_conns = {}
        self.table_modules = set()
        self._acquiring = False
        self._acquire_updated = False
        self._lastacquire = None
        self._synchronized = False
        self.createAPI(api(self.getconnections, self.apiroutine),
                       api(self.getconnection, self.apiroutine),
                       api(self.waitconnection, self.apiroutine),
                       api(self.getdatapathids, self.apiroutine),
                       api(self.getalldatapathids, self.apiroutine),
                       api(self.getallconnections, self.apiroutine),
                       api(self.getconnectionsbyendpoint, self.apiroutine),
                       api(self.getconnectionsbyendpointname, self.apiroutine),
                       api(self.getendpoints, self.apiroutine),
                       api(self.getallendpoints, self.apiroutine),
                       api(self.acquiretable, self.apiroutine),
                       api(self.unacquiretable, self.apiroutine),
                       api(self.lastacquiredtables)
                       )
    def _add_connection(self, conn):
        vhost = conn.protocol.vhost
        conns = self.managed_conns.setdefault((vhost, conn.openflow_datapathid), [])
        remove = []
        for i in range(0, len(conns)):
            if conns[i].openflow_auxiliaryid == conn.openflow_auxiliaryid:
                ci = conns[i]
                remove = [ci]
                ep = _get_endpoint(ci)
                econns = self.endpoint_conns.get((vhost, ep))
                if econns is not None:
                    try:
                        econns.remove(ci)
                    except ValueError:
                        pass
                    if not econns:
                        del self.endpoint_conns[(vhost, ep)]
                del conns[i]
                break
        conns.append(conn)
        ep = _get_endpoint(conn)
        econns = self.endpoint_conns.setdefault((vhost, ep), [])
        econns.append(conn)
        if self._lastacquire and conn.openflow_auxiliaryid == 0:
            self.apiroutine.subroutine(self._initialize_connection(conn))
        return remove
    def _initialize_connection(self, conn):
        ofdef = conn.openflowdef
        flow_mod = ofdef.ofp_flow_mod(buffer_id = ofdef.OFP_NO_BUFFER,
                                                 out_port = ofdef.OFPP_ANY,
                                                 command = ofdef.OFPFC_DELETE
                                                 )
        if hasattr(ofdef, 'OFPG_ANY'):
            flow_mod.out_group = ofdef.OFPG_ANY
        if hasattr(ofdef, 'OFPTT_ALL'):
            flow_mod.table_id = ofdef.OFPTT_ALL
        if hasattr(ofdef, 'ofp_match_oxm'):
            flow_mod.match = ofdef.ofp_match_oxm()
        cmds = [flow_mod]
        if hasattr(ofdef, 'ofp_group_mod'):
            group_mod = ofdef.ofp_group_mod(command = ofdef.OFPGC_DELETE,
                                            group_id = ofdef.OFPG_ALL
                                            )
            cmds.append(group_mod)
        for m in conn.protocol.batch(cmds, conn, self.apiroutine):
            yield m
        if hasattr(ofdef, 'ofp_instruction_goto_table'):
            # Create default flows
            vhost = conn.protocol.vhost
            if self._lastacquire and vhost in self._lastacquire:
                _, pathtable = self._lastacquire[vhost]
                cmds = [ofdef.ofp_flow_mod(table_id = t[i][1],
                                             command = ofdef.OFPFC_ADD,
                                             priority = 0,
                                             buffer_id = ofdef.OFP_NO_BUFFER,
                                             out_port = ofdef.OFPP_ANY,
                                             out_group = ofdef.OFPG_ANY,
                                             match = ofdef.ofp_match_oxm(),
                                             instructions = [ofdef.ofp_instruction_goto_table(table_id = t[i+1][1])]
                                       )
                          for _,t in pathtable.items()
                          for i in range(0, len(t) - 1)]
                if cmds:
                    for m in conn.protocol.batch(cmds, conn, self.apiroutine):
                        yield m
        for m in self.apiroutine.waitForSend(FlowInitialize(conn, conn.openflow_datapathid, conn.protocol.vhost)):
            yield m
    def _acquire_tables(self):
        try:
            while self._acquire_updated:
                result = None
                exception = None
                # Delay the update so we are not updating table acquires for every module
                for m in self.apiroutine.waitForSend(TableAcquireDelayEvent()):
                    yield m
                yield (TableAcquireDelayEvent.createMatcher(),)
                module_list = list(self.table_modules)
                self._acquire_updated = False
                try:
                    for m in self.apiroutine.executeAll((callAPI(self.apiroutine, module, 'gettablerequest', {}) for module in module_list)):
                        yield m
                except QuitException:
                    raise
                except Exception as exc:
                    self._logger.exception('Acquiring table failed')
                    exception = exc
                else:
                    requests = [r[0] for r in self.apiroutine.retvalue]
                    vhosts = set(vh for _, vhs in requests if vhs is not None for vh in vhs)
                    vhost_result = {}
                    # Requests should be list of (name, (ancester, ancester, ...), pathname)
                    for vh in vhosts:
                        graph = {}
                        table_path = {}
                        try:
                            for r in requests:
                                if r[1] is None or vh in r[1]:
                                    for name, ancesters, pathname in r[0]:
                                        if name in table_path:
                                            if table_path[name] != pathname:
                                                raise ValueError("table conflict detected: %r can not be in two path: %r and %r" % (name, table_path[name], pathname))
                                        else:
                                            table_path[name] = pathname
                                        if name not in graph:
                                            graph[name] = (set(ancesters), set())
                                        else:
                                            graph[name][0].update(ancesters)
                                        for anc in ancesters:
                                            graph.setdefault(anc, (set(), set()))[1].add(name)
                        except ValueError as exc:
                            self._logger.error(str(exc))
                            exception = exc
                            break
                        else:
                            sequences = []
                            def dfs_sort(current):
                                sequences.append(current)
                                for d in graph[current][1]:
                                    anc = graph[d][0]
                                    anc.remove(current)
                                    if not anc:
                                        dfs_sort(d)
                            nopre_tables = sorted([k for k,v in graph.items() if not v[0]], key = lambda x: (table_path.get(name, ''),name))
                            for t in nopre_tables:
                                dfs_sort(t)
                            if len(sequences) < len(graph):
                                rest_tables = set(graph.keys()).difference(sequences)
                                self._logger.error("Circle detected in table acquiring, following tables are related: %r, vhost = %r", sorted(rest_tables), vh)
                                self._logger.error("Circle dependencies are: %s", ", ".join(repr(tuple(graph[t][0])) + "=>" + t for t in rest_tables))
                                exception = ValueError("Circle detected in table acquiring, following tables are related: %r, vhost = %r" % (sorted(rest_tables),vh))
                                break
                            elif len(sequences) > 255:
                                self._logger.error("Table limit exceeded: %d tables (only 255 allowed), vhost = %r", len(sequences), vh)
                                exception = ValueError("Table limit exceeded: %d tables (only 255 allowed), vhost = %r" % (len(sequences),vh))
                                break
                            else:
                                full_indices = list(zip(sequences, itertools.count()))
                                tables = dict((k,tuple(g)) for k,g in itertools.groupby(sorted(full_indices, key = lambda x: table_path.get(x[0], '')),
                                                           lambda x: table_path.get(x[0], '')))
                                vhost_result[vh] = (full_indices, tables)
        finally:
            self._acquiring = False
        if exception:
            for m in self.apiroutine.waitForSend(TableAcquireUpdate(exception = exception)):
                yield m
        else:
            result = vhost_result
            if result != self._lastacquire:
                self._lastacquire = result
                self._reinitall()
            for m in self.apiroutine.waitForSend(TableAcquireUpdate(result = result)):
                yield m
    def load(self, container):
        self.scheduler.queue.addSubQueue(1, TableAcquireDelayEvent.createMatcher(), 'ofpmanager_tableacquiredelay')
        for m in container.waitForSend(TableAcquireUpdate(result = None)):
            yield m
        for m in Module.load(self, container):
            yield m
    def unload(self, container, force=False):
        for m in Module.unload(self, container, force=force):
            yield m
        for m in container.syscall(syscall_removequeue(self.scheduler.queue, 'ofpmanager_tableacquiredelay')):
            yield m
    def _reinitall(self):
        for cl in self.managed_conns.values():
            for c in cl:
                self.apiroutine.subroutine(self._initialize_connection(c))
    def _manage_existing(self):
        for m in callAPI(self.apiroutine, "openflowserver", "getconnections", {}):
            yield m
        vb = self.vhostbind
        for c in self.apiroutine.retvalue:
            if vb is None or c.protocol.vhost in vb:
                self._add_connection(c)
        self._synchronized = True
        for m in self.apiroutine.waitForSend(ModuleNotification(self.getServiceName(), 'synchronized')):
            yield m
    def _wait_for_sync(self):
        if not self._synchronized:
            yield (ModuleNotification.createMatcher(self.getServiceName(), 'synchronized'),)
    def _manage_conns(self):
        vb = self.vhostbind
        self.apiroutine.subroutine(self._manage_existing(), False)
        try:
            if vb is not None:
                conn_up = OpenflowConnectionStateEvent.createMatcher(state = OpenflowConnectionStateEvent.CONNECTION_SETUP,
                                                                     _ismatch = lambda x: x.createby.vhost in vb)
                conn_down = OpenflowConnectionStateEvent.createMatcher(state = OpenflowConnectionStateEvent.CONNECTION_DOWN,
                                                                     _ismatch = lambda x: x.createby.vhost in vb)
            else:
                conn_up = OpenflowConnectionStateEvent.createMatcher(state = OpenflowConnectionStateEvent.CONNECTION_SETUP)
                conn_down = OpenflowConnectionStateEvent.createMatcher(state = OpenflowConnectionStateEvent.CONNECTION_DOWN)
            while True:
                yield (conn_up, conn_down)
                if self.apiroutine.matcher is conn_up:
                    e = self.apiroutine.event
                    remove = self._add_connection(e.connection)
                    self.scheduler.emergesend(ModuleNotification(self.getServiceName(), 'update', add = [e.connection], remove = remove))
                else:
                    e = self.apiroutine.event
                    conns = self.managed_conns.get((e.createby.vhost, e.datapathid))
                    remove = []
                    if conns is not None:
                        try:
                            conns.remove(e.connection)
                        except ValueError:
                            pass
                        else:
                            remove.append(e.connection)
                        
                        if not conns:
                            del self.managed_conns[(e.createby.vhost, e.datapathid)]
                        # Also delete from endpoint_conns
                        ep = _get_endpoint(e.connection)
                        econns = self.endpoint_conns.get((e.createby.vhost, ep))
                        if econns is not None:
                            try:
                                econns.remove(e.connection)
                            except ValueError:
                                pass
                            if not econns:
                                del self.endpoint_conns[(e.createby.vhost, ep)]
                    if remove:
                        self.scheduler.emergesend(ModuleNotification(self.getServiceName(), 'update', add = [], remove = remove))
        finally:
            self.scheduler.emergesend(ModuleNotification(self.getServiceName(), 'unsynchronized'))
    def getconnections(self, datapathid, vhost = ''):
        "Return all connections of datapath"
        for m in self._wait_for_sync():
            yield m
        self.apiroutine.retvalue = list(self.managed_conns.get((vhost, datapathid), []))
    def getconnection(self, datapathid, auxiliaryid = 0, vhost = ''):
        "Get current connection of datapath"
        for m in self._wait_for_sync():
            yield m
        self.apiroutine.retvalue = self._getconnection(datapathid, auxiliaryid, vhost)
    def _getconnection(self, datapathid, auxiliaryid = 0, vhost = ''):
        conns = self.managed_conns.get((vhost, datapathid))
        if conns is None:
            return None
        else:
            for c in conns:
                if c.openflow_auxiliaryid == auxiliaryid:
                    return c
            return None
    def waitconnection(self, datapathid, auxiliaryid = 0, timeout = 30, vhost = ''):
        "Wait for a datapath connection"
        for m in self._wait_for_sync():
            yield m
        c = self._getconnection(datapathid, auxiliaryid, vhost)
        if c is None:
            for m in self.apiroutine.waitWithTimeout(timeout, 
                            OpenflowConnectionStateEvent.createMatcher(datapathid, auxiliaryid,
                                    OpenflowConnectionStateEvent.CONNECTION_SETUP,
                                    _ismatch = lambda x: x.createby.vhost == vhost)):
                yield m
            if self.apiroutine.timeout:
                raise ConnectionResetException('Datapath %016x is not connected' % datapathid)
            self.apiroutine.retvalue = self.apiroutine.event.connection
        else:
            self.apiroutine.retvalue = c
    def getdatapathids(self, vhost = ''):
        "Get All datapath IDs"
        for m in self._wait_for_sync():
            yield m
        self.apiroutine.retvalue = [k[1] for k in self.managed_conns.keys() if k[0] == vhost]
    def getalldatapathids(self):
        "Get all datapath IDs from any vhost. Return ``(vhost, datapathid)`` pair."
        for m in self._wait_for_sync():
            yield m
        self.apiroutine.retvalue = list(self.managed_conns.keys())
    def getallconnections(self, vhost = ''):
        "Get all connections from vhost. If vhost is None, return all connections from any host"
        for m in self._wait_for_sync():
            yield m
        if vhost is None:
            self.apiroutine.retvalue = list(itertools.chain(self.managed_conns.values()))
        else:
            self.apiroutine.retvalue = list(itertools.chain(v for k,v in self.managed_conns.items() if k[0] == vhost))
    def getconnectionsbyendpoint(self, endpoint, vhost = ''):
        "Get connection by endpoint address (IP, IPv6 or UNIX socket address)"
        for m in self._wait_for_sync():
            yield m
        self.apiroutine.retvalue = self.endpoint_conns.get((vhost, endpoint))
    def getconnectionsbyendpointname(self, name, vhost = '', timeout = 30):
        "Get connection by endpoint name (Domain name, IP or IPv6 address)"
        # Resolve the name
        if not name:
            endpoint = ''
            for m in self.getconnectionbyendpoint(endpoint, vhost):
                yield m
        else:
            request = (name, 0, socket.AF_UNSPEC, socket.SOCK_STREAM, socket.IPPROTO_TCP, socket.AI_ADDRCONFIG | socket.AI_V4MAPPED)
            # Resolve hostname
            for m in self.apiroutine.waitForSend(ResolveRequestEvent(request)):
                yield m
            for m in self.apiroutine.waitWithTimeout(timeout, ResolveResponseEvent.createMatcher(request)):
                yield m
            if self.apiroutine.timeout:
                # Resolve is only allowed through asynchronous resolver
                #try:
                #    self.addrinfo = socket.getaddrinfo(self.hostname, self.port, socket.AF_UNSPEC, socket.SOCK_DGRAM if self.udp else socket.SOCK_STREAM, socket.IPPROTO_UDP if self.udp else socket.IPPROTO_TCP, socket.AI_ADDRCONFIG|socket.AI_NUMERICHOST)
                #except:
                raise IOError('Resolve hostname timeout: ' + name)
            else:
                if hasattr(self.apiroutine.event, 'error'):
                    raise IOError('Cannot resolve hostname: ' + name)
                resp = self.apiroutine.event.response
                for r in resp:
                    raddr = r[4]
                    if isinstance(raddr, tuple):
                        # Ignore port
                        endpoint = raddr[0]
                    else:
                        # Unix socket? This should not happen, but in case...
                        endpoint = raddr
                    for m in self.getconnectionsbyendpoint(endpoint, vhost):
                        yield m
                    if self.apiroutine.retvalue is not None:
                        break
    def getendpoints(self, vhost = ''):
        "Get all endpoints for vhost"
        for m in self._wait_for_sync():
            yield m
        self.apiroutine.retvalue = [k[1] for k in self.endpoint_conns if k[0] == vhost]
    def getallendpoints(self):
        "Get all endpoints from any vhost. Return ``(vhost, endpoint)`` pairs."
        for m in self._wait_for_sync():
            yield m
        self.apiroutine.retvalue = list(self.endpoint_conns.keys())
    def lastacquiredtables(self, vhost = ""):
        "Get acquired table IDs"
        return self._lastacquire.get(vhost)
    def acquiretable(self, modulename):
        "Start to acquire tables for a module on module loading."
        if not modulename in self.table_modules:
            self.table_modules.add(modulename)
            self._acquire_updated = True
            if not self._acquiring:
                self._acquiring = True
                self.apiroutine.subroutine(self._acquire_tables())
        self.apiroutine.retvalue = None
        if False:
            yield
    def unacquiretable(self, modulename):
        "When module is unloaded, stop acquiring tables for this module."
        if modulename in self.table_modules:
            self.table_modules.remove(modulename)
            self._acquire_updated = True
            if not self._acquiring:
                self._acquiring = True
                self.apiroutine.subroutine(self._acquire_tables())
        self.apiroutine.retvalue = None
        if False:
            yield
