'''
Created on 2016/2/23

:author: hubo
'''


from vlcp.config import defaultconfig
from vlcp.server.module import Module, api, depend, callAPI, ModuleNotification
from vlcp.event.runnable import RoutineContainer
from vlcp.service.sdn import ofpmanager
import vlcp.protocol.openflow.defs.openflow13 as of13
from vlcp.event.connection import ConnectionResetException
from vlcp.protocol.openflow.openflow import OpenflowProtocolException,\
    OpenflowAsyncMessageEvent
from vlcp.event.event import Event, withIndices
from contextlib import closing

def _bytes(s):
    return s.encode('ascii')

@withIndices('connection')
class OpenflowPortSynchronized(Event):
    pass

class OpenflowPortNotAppearException(Exception):
    pass

@defaultconfig
@depend(ofpmanager.OpenflowManager)
class OpenflowPortManager(Module):
    '''
    Manage Ports from Openflow Protocol
    '''
    service = True
    def __init__(self, server):
        Module.__init__(self, server)
        self.apiroutine = RoutineContainer(self.scheduler)
        self.apiroutine.main = self._manage_ports
        self.routines.append(self.apiroutine)
        self.managed_ports = {}
        self.createAPI(api(self.getports, self.apiroutine),
                       api(self.getallports, self.apiroutine),
                       api(self.getportbyno, self.apiroutine),
                       api(self.waitportbyno, self.apiroutine),
                       api(self.getportbyname, self.apiroutine),
                       api(self.waitportbyname, self.apiroutine),
                       api(self.resync, self.apiroutine)
                       )
        self._synchronized = False
    def _get_ports(self, connection, protocol, onup = False, update = True):
        ofdef = connection.openflowdef
        dpid = connection.openflow_datapathid
        vhost = connection.protocol.vhost
        add = []
        try:
            if hasattr(ofdef, 'ofp_multipart_request'):
                # Openflow 1.3, use ofp_multipart_request to get ports
                for m in protocol.querymultipart(ofdef.ofp_multipart_request(type=ofdef.OFPMP_PORT_DESC), connection, self.apiroutine):
                    yield m
                ports = self.managed_ports.setdefault((vhost, dpid), {})
                for msg in self.apiroutine.openflow_reply:
                    for p in msg.ports:
                        add.append(p)
                        ports[p.port_no] = p
            else:
                # Openflow 1.0, use features_request
                if onup:
                    # Use the features_reply on connection setup
                    reply = connection.openflow_featuresreply
                else:
                    request = ofdef.ofp_msg()
                    request.header.type = ofdef.OFPT_FEATURES_REQUEST
                    for m in protocol.querywithreply(request):
                        yield m
                    reply = self.apiroutine.retvalue
                ports = self.managed_ports.setdefault((vhost, dpid), {})
                for p in reply.ports:
                    add.append(p)
                    ports[p.port_no] = p
            if update:
                for m in self.apiroutine.waitForSend(OpenflowPortSynchronized(connection)):
                    yield m
                for m in self.apiroutine.waitForSend(ModuleNotification(self.getServiceName(), 'update',
                                                                         datapathid = connection.openflow_datapathid,
                                                                         connection = connection,
                                                                         vhost = protocol.vhost,
                                                                         add = add, remove = [],
                                                                         reason = 'connected')):
                    yield m
        except ConnectionResetException:
            pass
        except OpenflowProtocolException:
            pass
    def _get_existing_ports(self):
        for m in callAPI(self.apiroutine, 'openflowmanager', 'getallconnections', {'vhost':None}):
            yield m
        with closing(self.apiroutine.executeAll([self._get_ports(c, c.protocol, False, False) for c in self.apiroutine.retvalue if c.openflow_auxiliaryid == 0])) as g:
            for m in g:
                yield m
        self._synchronized = True
        for m in self.apiroutine.waitForSend(ModuleNotification(self.getServiceName(), 'synchronized')):
            yield m
    def _wait_for_sync(self):
        if not self._synchronized:
            yield (ModuleNotification.createMatcher(self.getServiceName(), 'synchronized'),)
    def _manage_ports(self):
        try:
            self.apiroutine.subroutine(self._get_existing_ports())
            conn_update = ModuleNotification.createMatcher('openflowmanager', 'update')
            port_status = OpenflowAsyncMessageEvent.createMatcher(of13.OFPT_PORT_STATUS, None, 0)
            while True:
                yield (conn_update, port_status)
                if self.apiroutine.matcher is port_status:
                    e = self.apiroutine.event
                    m = e.message
                    c = e.connection
                    if (c.protocol.vhost, c.openflow_datapathid) in self.managed_ports:
                        if m.reason == c.openflowdef.OFPPR_ADD:
                            # A new port is added
                            self.managed_ports[(c.protocol.vhost, c.openflow_datapathid)][m.desc.port_no] = m.desc
                            self.scheduler.emergesend(ModuleNotification(self.getServiceName(), 'update',
                                                                         datapathid = c.openflow_datapathid,
                                                                         connection = c,
                                                                         vhost = c.protocol.vhost,
                                                                         add = [m.desc], remove = [],
                                                                         reason = 'add'))
                        elif m.reason == c.openflowdef.OFPPR_DELETE:
                            try:
                                del self.managed_ports[(c.protocol.vhost, c.openflow_datapathid)][m.desc.port_no]
                                self.scheduler.emergesend(ModuleNotification(self.getServiceName(), 'update',
                                                                             datapathid = c.openflow_datapathid,
                                                                             connection = c,
                                                                             vhost = c.protocol.vhost,
                                                                             add = [], remove = [m.desc],
                                                                             reason = 'delete'))
                            except KeyError:
                                pass
                        elif m.reason == c.openflowdef.OFPPR_MODIFY:
                            try:
                                self.scheduler.emergesend(ModuleNotification(self.getServiceName(), 'modified',
                                                                             datapathid = c.openflow_datapathid,
                                                                             connection = c,
                                                                             vhost = c.protocol.vhost,
                                                                             old = self.managed_ports[(c.protocol.vhost, c.openflow_datapathid)][m.desc.port_no],
                                                                             new = m.desc,
                                                                             reason = 'modified'))
                            except KeyError:
                                self.scheduler.emergesend(ModuleNotification(self.getServiceName(), 'update',
                                                                             datapathid = c.openflow_datapathid,
                                                                             connection = c,
                                                                             vhost = c.protocol.vhost,
                                                                             add = [m.desc], remove = [],
                                                                             reason = 'add'))
                            self.managed_ports[(c.protocol.vhost, c.openflow_datapathid)][m.desc.port_no] = m.desc
                else:
                    e = self.apiroutine.event
                    for c in e.remove:
                        if c.openflow_auxiliaryid == 0 and (c.protocol.vhost, c.openflow_datapathid) in self.managed_ports:
                            self.scheduler.emergesend(ModuleNotification(self.getServiceName(), 'update',
                                                 datapathid = c.openflow_datapathid,
                                                 connection = c,
                                                 vhost = c.protocol.vhost,
                                                 add = [], remove = list(self.managed_ports[(c.protocol.vhost, c.openflow_datapathid)].values()),
                                                 reason = 'disconnected'))
                            del self.managed_ports[(c.protocol.vhost, c.openflow_datapathid)]
                    for c in e.add:
                        if c.openflow_auxiliaryid == 0:
                            self.apiroutine.subroutine(self._get_ports(c, c.protocol, True, True))
        finally:
            self.scheduler.emergesend(ModuleNotification(self.getServiceName(), 'unsynchronized'))
    def getports(self, datapathid, vhost = ''):
        "Return all ports of a specifed datapath"
        for m in self._wait_for_sync():
            yield m
        r = self.managed_ports.get((vhost, datapathid))
        if r is None:
            self.apiroutine.retvalue = None
        else:
            self.apiroutine.retvalue = list(r.values())
    def getallports(self, vhost = None):
        "Return all ``(datapathid, port, vhost)`` tuples, optionally filterd by vhost"
        for m in self._wait_for_sync():
            yield m
        if vhost is None:
            self.apiroutine.retvalue = [(dpid, p, vh) for (vh, dpid),v in self.managed_ports.items() for p in v.values()]
        else:
            self.apiroutine.retvalue = [(dpid, p, vh) for (vh, dpid),v in self.managed_ports.items() if vh == vhost for p in v.values()]
    def getportbyno(self, datapathid, portno, vhost = ''):
        "Return port with specified OpenFlow portno"
        for m in self._wait_for_sync():
            yield m
        self.apiroutine.retvalue = self._getportbyno(datapathid, portno, vhost)
    def _getportbyno(self, datapathid, portno, vhost = ''):
        ports = self.managed_ports.get((vhost, datapathid))
        if ports is None:
            return None
        else:
            return ports.get(portno)
    def waitportbyno(self, datapathid, portno, timeout = 30, vhost = ''):
        """
        Wait for the specified OpenFlow portno to appear, or until timeout.
        """
        for m in self._wait_for_sync():
            yield m
        def waitinner():
            ports = self.managed_ports.get((vhost, datapathid))
            if ports is None:
                for m in callAPI(self.apiroutine, 'openflowmanager', 'waitconnection', {'datapathid': datapathid, 'vhost':vhost, 'timeout': timeout}):
                    yield m
                c = self.apiroutine.retvalue
                ports = self.managed_ports.get((vhost, datapathid))
                if ports is None:
                    yield (OpenflowPortSynchronized.createMatcher(c),)
                ports = self.managed_ports.get((vhost, datapathid))
                if ports is None:
                    raise ConnectionResetException('Datapath %016x is not connected' % datapathid)
            if portno not in ports:
                yield (OpenflowAsyncMessageEvent.createMatcher(of13.OFPT_PORT_STATUS, datapathid, 0, _ismatch = lambda x: x.message.desc.port_no == portno),)
                self.apiroutine.retvalue = self.apiroutine.event.message.desc
            else:
                self.apiroutine.retvalue = ports[portno]
        for m in self.apiroutine.executeWithTimeout(timeout, waitinner()):
            yield m
        if self.apiroutine.timeout:
            raise OpenflowPortNotAppearException('Port %d does not appear on datapath %016x' % (portno, datapathid))
    def getportbyname(self, datapathid, name, vhost = ''):
        "Return port with specified port name"
        for m in self._wait_for_sync():
            yield m
        self.apiroutine.retvalue = self._getportbyname(datapathid, name, vhost)
    def _getportbyname(self, datapathid, name, vhost = ''):
        if not isinstance(name, bytes):
            name = _bytes(name)
        ports = self.managed_ports.get((vhost, datapathid))
        if ports is None:
            return None
        else:
            for p in ports.values():
                if p.name == name:
                    return p
            return None
    def waitportbyname(self, datapathid, name, timeout = 30, vhost = ''):
        """
        Wait for a port with the specified port name to appear, or until timeout
        """
        for m in self._wait_for_sync():
            yield m
        if not isinstance(name, bytes):
            name = _bytes(name)
        def waitinner():
            ports = self.managed_ports.get((vhost, datapathid))
            if ports is None:
                for m in callAPI(self.apiroutine, 'openflowmanager', 'waitconnection', {'datapathid': datapathid, 'vhost':vhost, 'timeout': timeout}):
                    yield m
                c = self.apiroutine.retvalue
                ports = self.managed_ports.get((vhost, datapathid))
                if ports is None:
                    yield (OpenflowPortSynchronized.createMatcher(c),)
                ports = self.managed_ports.get((vhost, datapathid))
                if ports is None:
                    raise ConnectionResetException('Datapath %016x is not connected' % datapathid)
            for p in ports.values():
                if p.name == name:
                    self.apiroutine.retvalue = p
                    return
            yield (OpenflowAsyncMessageEvent.createMatcher(of13.OFPT_PORT_STATUS, datapathid, 0, _ismatch = lambda x: x.message.desc.name == name),)
            self.apiroutine.retvalue = self.apiroutine.event.message.desc
        for m in self.apiroutine.executeWithTimeout(timeout, waitinner()):
            yield m
        if self.apiroutine.timeout:
            raise OpenflowPortNotAppearException('Port %r does not appear on datapath %016x' % (name, datapathid))
    def resync(self, datapathid, vhost = ''):
        '''
        Resync with current ports
        '''
        # Sometimes when the OpenFlow connection is very busy, PORT_STATUS message may be dropped.
        # We must deal with this and recover from it
        # Save current manged_ports
        if (vhost, datapathid) not in self.managed_ports:
            self.apiroutine.retvalue = None
            return
        else:
            last_ports = set(self.managed_ports[(vhost, datapathid)].keys())
        add = set()
        remove = set()
        ports = {}
        for _ in range(0, 10):
            for m in callAPI(self.apiroutine, 'openflowmanager', 'getconnection', {'datapathid': datapathid, 'vhost':vhost}):
                yield m
            c = self.apiroutine.retvalue
            if c is None:
                # Disconnected, will automatically resync when reconnected
                self.apiroutine.retvalue = None
                return
            ofdef = c.openflowdef
            protocol = c.protocol
            try:
                if hasattr(ofdef, 'ofp_multipart_request'):
                    # Openflow 1.3, use ofp_multipart_request to get ports
                    for m in protocol.querymultipart(ofdef.ofp_multipart_request(type=ofdef.OFPMP_PORT_DESC), c, self.apiroutine):
                        yield m
                    for msg in self.apiroutine.openflow_reply:
                        for p in msg.ports:
                            ports[p.port_no] = p
                else:
                    # Openflow 1.0, use features_request
                    request = ofdef.ofp_msg()
                    request.header.type = ofdef.OFPT_FEATURES_REQUEST
                    for m in protocol.querywithreply(request):
                        yield m
                    reply = self.apiroutine.retvalue
                    for p in reply.ports:
                        ports[p.port_no] = p
            except ConnectionResetException:
                break
            except OpenflowProtocolException:
                break
            else:
                if (vhost, datapathid) not in self.managed_ports:
                    self.apiroutine.retvalue = None
                    return
                current_ports = set(self.managed_ports[(vhost, datapathid)])
                # If a port is already removed
                remove.intersection_update(current_ports)
                # If a port is already added
                add.difference_update(current_ports)
                # If a port is not acquired, we do not add it
                acquired_keys = set(ports.keys())
                add.difference_update(acquired_keys)
                # Add and remove previous added/removed ports
                current_ports.difference_update(remove)
                current_ports.update(add)
                # If there are changed ports, the changed ports may or may not appear in the acquired port list
                # We only deal with following situations:
                # 1. If both lack ports, we add them
                # 2. If both have additional ports, we remote them
                to_add = acquired_keys.difference(current_ports.union(last_ports))
                to_remove = current_ports.intersection(last_ports).difference(acquired_keys)
                if not to_add and not to_remove and current_ports == last_ports:
                    break
                else:
                    add.update(to_add)
                    remove.update(to_remove)
                    current_ports.update(to_add)
                    current_ports.difference_update(to_remove)
                    last_ports = current_ports
        # Actual add and remove
        mports = self.managed_ports[(vhost, datapathid)]
        add_ports = []
        remove_ports = []
        for k in add:
            if k not in mports:
                add_ports.append(ports[k])
            mports[k] = ports[k]
        for k in remove:
            try:
                oldport = mports.pop(k)
            except KeyError:
                pass
            else:
                remove_ports.append(oldport)
        for m in self.apiroutine.waitForSend(ModuleNotification(self.getServiceName(), 'update',
                                                                 datapathid = datapathid,
                                                                 connection = c,
                                                                 vhost = vhost,
                                                                 add = add_ports, remove = remove_ports,
                                                                 reason = 'resync')):
            yield m
        self.apiroutine.retvalue = None
