'''
Created on 2016/3/31

:author: hubo
'''
from __future__ import print_function, absolute_import, division
from vlcp.utils.dataobject import DataObject, DataObjectSet, updater, DataObjectUpdateEvent, watch_context,\
    multiwaitif, dump, set_new, ReferenceObject
from vlcp.server.module import depend, Module, callAPI, ModuleLoadStateChanged,\
    api
import vlcp.service.kvdb.objectdb as objectdb
from vlcp.config.config import defaultconfig
from vlcp.event.runnable import RoutineContainer, RoutineException
from uuid import uuid1
from vlcp.server import main

class PhysicalNetwork(DataObject):
    _prefix = 'vlcptest.physicalnetwork'
    _indices = ('id',)

class LogicalNetwork(DataObject):
    _prefix = 'vlcptest.logicalnetwork'
    _indices = ('id',)

class PhysicalNetworkMap(DataObject):
    _prefix = 'vlcptest.physicalnetworkmap'
    _indices = ('id',)
    def __init__(self, prefix=None, deleted=False):
        DataObject.__init__(self, prefix=prefix, deleted=deleted)
        self.networks = DataObjectSet()
        self.network_allocation = dict()
        self.ports = DataObjectSet()

class LogicalNetworkMap(DataObject):
    _prefix = 'vlcptest.logicalnetworkmap'
    _indices = ('id',)
    def __init__(self, prefix=None, deleted=False):
        DataObject.__init__(self, prefix=prefix, deleted=deleted)
        self.ports = DataObjectSet()

class PhysicalPort(DataObject):
    _prefix = 'vlcptest.physicalport'
    _indices = ('systemid', 'bridge', 'name')

class LogicalPort(DataObject):
    _prefix = 'vlcptest.logicalport'
    _indices = ('id',)

class PhysicalNetworkSet(DataObject):
    _prefix = 'vlcptest.physicalnetworkset'

class LogicalNetworkSet(DataObject):
    _prefix = 'vlcptest.logicalnetworkset'

class LogicalPortSet(DataObject):
    _prefix = 'vlcptest.logicalportset'
    
class PhysicalPortSet(DataObject):
    _prefix = 'vlcptest.physicalportset'

@defaultconfig
@depend(objectdb.ObjectDB)
class TestObjectDB(Module):
    def __init__(self, server):
        Module.__init__(self, server)
        self.apiroutine = RoutineContainer(self.scheduler)
        self.apiroutine.main = self._main
        self.routines.append(self.apiroutine)
        self._reqid = 0
        self._ownerid = uuid1().hex
        self.createAPI(api(self.createlogicalnetwork, self.apiroutine),
                       api(self.createlogicalnetworks, self.apiroutine),
                       api(self.createphysicalnetwork, self.apiroutine),
                       api(self.createphysicalnetworks, self.apiroutine),
                       api(self.createphysicalport, self.apiroutine),
                       api(self.createphysicalports, self.apiroutine),
                       api(self.createlogicalport, self.apiroutine),
                       api(self.createlogicalports, self.apiroutine))
    def _monitor(self):
        update_event = DataObjectUpdateEvent.createMatcher()
        while True:
            yield (update_event,)
            self._logger.info('Database update: %r', self.apiroutine.event)
    def _dumpkeys(self, keys):
        self._reqid += 1
        reqid = ('testobjectdb', self._reqid)
        for m in callAPI(self.apiroutine, 'objectdb', 'mget', {'keys': keys, 'requestid': reqid}):
            yield m
        retobjs = self.apiroutine.retvalue
        with watch_context(keys, retobjs, reqid, self.apiroutine):
            self.apiroutine.retvalue = [dump(v) for v in retobjs]
    def _updateport(self, key):
        unload_matcher = ModuleLoadStateChanged.createMatcher(self.target, ModuleLoadStateChanged.UNLOADING)
        def updateinner():
            self._reqid += 1
            reqid = ('testobjectdb', self._reqid)
            for m in callAPI(self.apiroutine, 'objectdb', 'get', {'key': key, 'requestid': reqid}):
                yield m
            portobj = self.apiroutine.retvalue
            with watch_context([key], [portobj], reqid, self.apiroutine):
                if portobj is not None:
                    @updater
                    def write_status(portobj):
                        if portobj is None:
                            raise ValueError('Already deleted')
                        if not hasattr(portobj, 'owner'):
                            portobj.owner = self._ownerid
                            portobj.status = 'READY'
                            return [portobj]
                        else:
                            raise ValueError('Already managed')
                    try:
                        for m in callAPI(self.apiroutine, 'objectdb', 'transact', {'keys': [portobj], 'updater': write_status}):
                            yield m
                    except ValueError:
                        pass
                    else:
                        for m in portobj.waitif(self.apiroutine, lambda x: x.isdeleted() or hasattr(x, 'owner')):
                            yield m
                        self._logger.info('Port managed: %r', dump(portobj))
                        while True:
                            for m in portobj.waitif(self.apiroutine, lambda x: True, True):
                                yield m
                            if portobj.isdeleted():
                                self._logger.info('Port deleted: %r', dump(portobj))
                                break
                            else:
                                self._logger.info('Port updated: %r', dump(portobj))
        try:
            for m in self.apiroutine.withException(updateinner(), unload_matcher):
                yield m
        except RoutineException:
            pass
    def _waitforchange(self, key):
        for m in callAPI(self.apiroutine, 'objectdb', 'watch', {'key': key, 'requestid': 'testobjectdb'}):
            yield m
        setobj = self.apiroutine.retvalue
        with watch_context([key], [setobj], 'testobjectdb', self.apiroutine):
            for m in setobj.wait(self.apiroutine):
                yield m
            oldset = set()
            while True:
                for weakref in setobj.set.dataset().difference(oldset):
                    self.apiroutine.subroutine(self._updateport(weakref.getkey()))
                oldset = set(setobj.set.dataset())
                for m in setobj.waitif(self.apiroutine, lambda x: not x.isdeleted(), True):
                    yield m
    def _main(self):
        routines = []
        routines.append(self._monitor())
        keys = [LogicalPortSet.default_key(), PhysicalPortSet.default_key()]
        for k in keys:
            routines.append(self._waitforchange(k))
        for m in self.apiroutine.executeAll(routines, retnames = ()):
            yield m
    def load(self, container):
        @updater
        def initialize(phynetset, lognetset, logportset, phyportset):
            if phynetset is None:
                phynetset = PhysicalNetworkSet()
                phynetset.set = DataObjectSet()
            if lognetset is None:
                lognetset = LogicalNetworkSet()
                lognetset.set = DataObjectSet()
            if logportset is None:
                logportset = LogicalPortSet()
                logportset.set = DataObjectSet()
            if phyportset is None:
                phyportset = PhysicalPortSet()
                phyportset.set = DataObjectSet()
            return [phynetset, lognetset, logportset, phyportset]
        for m in callAPI(container, 'objectdb', 'transact', {'keys':[PhysicalNetworkSet.default_key(),
                                                                   LogicalNetworkSet.default_key(),
                                                                   LogicalPortSet.default_key(),
                                                                   PhysicalPortSet.default_key()],
                                                             'updater': initialize}):
            yield m
        for m in Module.load(self, container):
            yield m
    def createphysicalnetwork(self, type = 'vlan', id = None, **kwargs):
        new_network, new_map = self._createphysicalnetwork(type, id, **kwargs)
        @updater
        def create_phy(physet, phynet, phymap):
            phynet = set_new(phynet, new_network)
            phymap = set_new(phymap, new_map)
            physet.set.dataset().add(phynet.create_weakreference())
            return [physet, phynet, phymap]
        for m in callAPI(self.apiroutine, 'objectdb', 'transact', {'keys':[PhysicalNetworkSet.default_key(),
                                                                           new_network.getkey(),
                                                                           new_map.getkey()],'updater':create_phy}):
            yield m
        for m in self._dumpkeys([new_network.getkey()]):
            yield m
        self.apiroutine.retvalue = self.apiroutine.retvalue[0]
    def createphysicalnetworks(self, networks):
        new_networks = [self._createphysicalnetwork(**n) for n in networks]
        @updater
        def create_phys(physet, *phynets):
            return_nets = [None, None] * len(new_networks)
            for i in range(0, len(new_networks)):
                return_nets[i * 2] = set_new(phynets[i * 2], new_networks[i][0])
                return_nets[i * 2 + 1] = set_new(phynets[i * 2 + 1], new_networks[i][1])
                physet.set.dataset().add(new_networks[i][0].create_weakreference())
            return [physet] + return_nets
        keys = [sn.getkey() for n in new_networks for sn in n]
        for m in callAPI(self.apiroutine, 'objectdb', 'transact', {'keys':[PhysicalNetworkSet.default_key()] + keys,'updater':create_phys}):
            yield m
        for m in self._dumpkeys([n[0].getkey() for n in new_networks]):
            yield m
    def _createlogicalnetwork(self, physicalnetwork, id = None, **kwargs):
        if not id:
            id = str(uuid1())
        new_network = LogicalNetwork.create_instance(id)
        for k,v in kwargs.items():
            setattr(new_network, k, v)
        new_network.physicalnetwork = ReferenceObject(PhysicalNetwork.default_key(physicalnetwork))
        new_networkmap = LogicalNetworkMap.create_instance(id)
        new_networkmap.network = new_network.create_reference()
        return new_network,new_networkmap
    def createlogicalnetworks(self, networks):
        new_networks = [self._createlogicalnetwork(**n) for n in networks]
        physical_networks = list(set(n[0].physicalnetwork.getkey() for n in new_networks))
        physical_maps = [PhysicalNetworkMap.default_key(PhysicalNetwork._getIndices(k)[1][0]) for k in physical_networks]
        @updater
        def create_logs(logset, *networks):
            phy_maps = list(networks[len(new_networks) * 2 : len(new_networks) * 2 + len(physical_networks)])
            phy_nets = list(networks[len(new_networks) * 2 + len(physical_networks):])
            phy_dict = dict(zip(physical_networks, zip(phy_nets, phy_maps)))
            return_nets = [None, None] * len(new_networks)
            for i in range(0, len(new_networks)):
                return_nets[2 * i] = set_new(networks[2 * i], new_networks[i][0])
                return_nets[2 * i + 1] = set_new(networks[2 * i + 1], new_networks[i][1])
            for n in return_nets[::2]:
                phynet, phymap = phy_dict.get(n.physicalnetwork.getkey())
                if phynet is None:
                    _, (phyid,) = PhysicalNetwork._getIndices(n.physicalnetwork.getkey())
                    raise ValueError('Physical network %r does not exist' % (phyid,))
                else:
                    if phynet.type == 'vlan':
                        if hasattr(n, 'vlanid'):
                            n.vlanid = int(n.vlanid)
                            if n.vlanid <= 0 or n.vlanid >= 4095:
                                raise ValueError('Invalid VLAN ID')
                            # VLAN id is specified
                            if str(n.vlanid) in phymap.network_allocation:
                                raise ValueError('VLAN ID %r is already allocated in physical network %r' % (n.vlanid,phynet.id))
                            else:
                                for start,end in phynet.vlanrange:
                                    if start <= n.vlanid <= end:
                                        break
                                else:
                                    raise ValueError('VLAN ID %r is not in vlan range of physical network %r' % (n.vlanid,phynet.id))
                            phymap.network_allocation[str(n.vlanid)] = n.create_weakreference()
                        else:
                            # Allocate a new VLAN id
                            for start,end in phynet.vlanrange:
                                for vlanid in range(start, end + 1):
                                    if str(vlanid) not in phymap.network_allocation:
                                        break
                                else:
                                    continue
                                break
                            else:
                                raise ValueError('Not enough VLAN ID to be allocated in physical network %r' % (phynet.id,))
                            n.vlanid = vlanid
                            phymap.network_allocation[str(vlanid)] = n.create_weakreference()
                    else:
                        if phymap.network_allocation:
                            raise ValueError('Physical network %r is already allocated by another logical network', (phynet.id,))
                        phymap.network_allocation['native'] = n.create_weakreference()
                    phymap.networks.dataset().add(n.create_weakreference())
                logset.set.dataset().add(n.create_weakreference())
            return [logset] + return_nets + phy_maps
        for m in callAPI(self.apiroutine, 'objectdb', 'transact', {'keys': [LogicalNetworkSet.default_key()] +\
                                                                            [sn.getkey() for n in new_networks for sn in n] +\
                                                                            physical_maps +\
                                                                            physical_networks,
                                                                   'updater': create_logs}):
            yield m
        for m in self._dumpkeys([n[0].getkey() for n in new_networks]):
            yield m
    def createlogicalnetwork(self, physicalnetwork, id = None, **kwargs):
        n = {'physicalnetwork':physicalnetwork, 'id':id}
        n.update(kwargs)
        for m in self.createlogicalnetworks([n]):
            yield m
        self.apiroutine.retvalue = self.apiroutine.retvalue[0]
    def _createphysicalnetwork(self, type = 'vlan', id = None, **kwargs):
        if not id:
            id = str(uuid1())
        if type == 'vlan':
            if 'vlanrange' not in kwargs:
                raise ValueError(r'Must specify vlanrange with network type="vlan"')
            vlanrange = kwargs['vlanrange']
            # Check
            try:
                lastend = 0
                for start, end in vlanrange:
                    if start <= lastend:
                        raise ValueError('VLAN sequences overlapped or disordered')
                    lastend = end
                if lastend >= 4095:
                    raise ValueError('VLAN ID out of range')
            except Exception as exc:
                raise ValueError('vlanrange format error: %s' % (str(exc),))
        else:
            type = 'native'
        new_network = PhysicalNetwork.create_instance(id)
        new_network.type = type
        for k,v in kwargs.items():
            setattr(new_network, k, v)
        new_networkmap = PhysicalNetworkMap.create_instance(id)
        new_networkmap.network = new_network.create_reference()
        return (new_network, new_networkmap)
    def createphysicalport(self, physicalnetwork, name, systemid = '%', bridge = '%', **kwargs):
        p = {'physicalnetwork':physicalnetwork, 'name':name, 'systemid':systemid,'bridge':bridge}
        p.update(kwargs)
        for m in self.createphysicalports([p]):
            yield m
        self.apiroutine.retvalue = self.apiroutine.retvalue[0]
    def _createphysicalport(self, physicalnetwork, name, systemid = '%', bridge = '%', **kwargs):
        new_port = PhysicalPort.create_instance(systemid, bridge, name)
        new_port.physicalnetwork = ReferenceObject(PhysicalNetwork.default_key(physicalnetwork))
        for k,v in kwargs.items():
            setattr(new_port, k, v)
        return new_port
    def createphysicalports(self, ports):
        new_ports = [self._createphysicalport(**p) for p in ports]
        physical_networks = list(set([p.physicalnetwork.getkey() for p in new_ports]))
        physical_maps = [PhysicalNetworkMap.default_key(*PhysicalNetwork._getIndices(k)[1]) for k in physical_networks]
        @updater
        def create_ports(portset, *objs):
            old_ports = objs[:len(new_ports)]
            phymaps = list(objs[len(new_ports):len(new_ports) + len(physical_networks)])
            phynets = list(objs[len(new_ports) + len(physical_networks):])
            phydict = dict(zip(physical_networks, zip(phynets, phymaps)))
            return_ports = [None] * len(new_ports)
            for i in range(0, len(new_ports)):
                return_ports[i] = set_new(old_ports[i], new_ports[i])
            for p in return_ports:
                phynet, phymap = phydict[p.physicalnetwork.getkey()]
                if phynet is None:
                    _, (phyid,) = PhysicalNetwork._getIndices(p.physicalnetwork.getkey())
                    raise ValueError('Physical network %r does not exist' % (phyid,))
                phymap.ports.dataset().add(p.create_weakreference())
            portset.set.dataset().add(p.create_weakreference())
            return [portset] + return_ports + phymaps
        for m in callAPI(self.apiroutine, 'objectdb', 'transact', {'keys': [PhysicalPortSet.default_key()] +\
                                                                            [p.getkey() for p in new_ports] +\
                                                                            physical_maps +\
                                                                            physical_networks,
                                                                   'updater': create_ports}):
            yield m
        for m in self._dumpkeys([p.getkey() for p in new_ports]):
            yield m
    def createlogicalport(self, logicalnetwork, id = None, **kwargs):
        p = {'logicalnetwork':logicalnetwork, 'id':id}
        p.update(kwargs)
        for m in self.createlogicalports([p]):
            yield m
        self.apiroutine.retvalue = self.apiroutine.retvalue[0]
    def _createlogicalport(self, logicalnetwork, id = None, **kwargs):
        if not id:
            id = str(uuid1())
        new_port = LogicalPort.create_instance(id)
        new_port.logicalnetwork = ReferenceObject(LogicalNetwork.default_key(logicalnetwork))
        for k,v in kwargs.items():
            setattr(new_port, k, v)
        return new_port
    def createlogicalports(self, ports):
        new_ports = [self._createlogicalport(**p) for p in ports]
        logical_networks = list(set([p.logicalnetwork.getkey() for p in new_ports]))
        logical_maps = [LogicalNetworkMap.default_key(*LogicalNetwork._getIndices(k)[1]) for k in logical_networks]
        @updater
        def create_ports(portset, *objs):
            old_ports = objs[:len(new_ports)]
            logmaps = list(objs[len(new_ports):len(new_ports) + len(logical_networks)])
            lognets = list(objs[len(new_ports) + len(logical_networks):])
            logdict = dict(zip(logical_networks, zip(lognets, logmaps)))
            return_ports = [None] * len(new_ports)
            for i in range(0, len(new_ports)):
                return_ports[i] = set_new(old_ports[i], new_ports[i])
            for p in return_ports:
                lognet, logmap = logdict[p.logicalnetwork.getkey()]
                if lognet is None:
                    _, (logid,) = LogicalNetwork._getIndices(p.logicalnetwork.getkey())
                    raise ValueError('Logical network %r does not exist' % (logid,))
                logmap.ports.dataset().add(p.create_weakreference())
            portset.set.dataset().add(p.create_weakreference())
            return [portset] + return_ports + logmaps
        for m in callAPI(self.apiroutine, 'objectdb', 'transact', {'keys': [LogicalPortSet.default_key()] +\
                                                                            [p.getkey() for p in new_ports] +\
                                                                            logical_maps +\
                                                                            logical_networks,
                                                                   'updater': create_ports}):
            yield m
        for m in self._dumpkeys([p.getkey() for p in new_ports]):
            yield m
    
if __name__ == '__main__':
    main("/etc/vlcp.conf", ("__main__.TestObjectDB", "vlcp.service.manage.webapi.WebAPI"))
    
