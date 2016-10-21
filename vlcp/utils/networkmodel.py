#! /usr/bin/python
# --*-- utf-8 --*--

from vlcp.utils.dataobject import DataObject,DataObjectSet

class PhysicalNetwork(DataObject):
    _prefix = 'viperflow.physicalnetwork'
    _indices = ("id",)

class PhysicalNetworkMap(DataObject):
    _prefix = 'viperflow.physicalnetworkmap'
    _indices = ("id",)
    
    def __init__(self,prefix = None,deleted = False):
        super(PhysicalNetworkMap,self).__init__(
                prefix = prefix,deleted = deleted)
        self.logicnetworks = DataObjectSet()
        self.network_allocation = dict()
        self.ports = DataObjectSet()

class PhysicalNetworkSet(DataObject):
    _prefix = 'viperflow.physicalnetworkset'

    def __init__(self,prefix = None,deleted = None):
        super(PhysicalNetworkSet,self).__init__(
                prefix = prefix,deleted = deleted)

        self.set = DataObjectSet()

class PhysicalPort(DataObject):
    _prefix = 'viperflow.physicalport'
    _indices = ('vhost','systemid','bridge','name')

class PhysicalPortSet(DataObject):
    _prefix = 'viperflow.physicalportset'

    def __init__(self,prefix = None,deleted = False):
        super(PhysicalPortSet,self).__init__(prefix = prefix,
                deleted = deleted)

        self.set = DataObjectSet()

class LogicalNetwork(DataObject):
    _prefix = 'viperflow.logicnetwork'
    _indices = ("id",)

class LogicalNetworkMap(DataObject):
    _prefix = 'viperflow.logicnetworkmap'
    _indices = ("id",)
    
    def __init__(self,prefix = None,deleted = None):
        super(LogicalNetworkMap,self).__init__(
                prefix = prefix,deleted = deleted)
        self.ports = DataObjectSet()
        self.subnets = DataObjectSet()

class LogicalNetworkSet(DataObject):
    _prefix = 'viperflow.logicalnetworkset'

    def __init__(self,prefix = None,deleted = None):
        super(LogicalNetworkSet,self).__init__(
                prefix = prefix,deleted = deleted)
        self.set = DataObjectSet()

class LogicalPort(DataObject):
    _prefix = 'viperflow.logicalport'
    _indices = ("id",)
    _unique_keys = (('_mac_address_index', ('network', 'mac_address')),
                    ('_ip_address_index', ('network', 'ip_address')))

class LogicalPortSet(DataObject):
    _prefix = 'viperflow.logcialportset'
    
    def __init__(self,prefix = None,deleted = None):
        super(LogicalPortSet,self).__init__(prefix = prefix,
                deleted = deleted)
        self.set = DataObjectSet()

class SubNet(DataObject):
    _prefix = 'viperflow.subnet'
    _indices = ('id',)


class SubNetMap(DataObject):
    _prefix = 'viperflow.subnetmap'
    _indices = ("id",)

    def __init__(self, prefix=None, deleted=None):
        super(SubNetMap, self).__init__(
                prefix=prefix, deleted=deleted)
        self.allocated_ips = dict()


class SubNetSet(DataObject):
    _prefix = 'viperflow.subnetset'

    def __init__(self, prefix=None, deleted=None):
        super(SubNetSet, self).__init__(prefix=prefix, deleted=deleted)
        self.set = DataObjectSet()


class VRouter(DataObject):
    _prefix = 'viperflow.vrouter'
    _indices = ('id',)

    def __init__(self,prefix=None,deleted=None):
        super(VRouter,self).__init__(prefix=prefix,deleted=deleted)
        self.interfaces = DataObjectSet()
        self.routes = list()


class DVRouterForwardSet(DataObject):
    _prefix = 'viperflow.dvrouterforwardset'

    def __init__(self,prefix=None,deleted=None):
        super(DVRouterForwardSet,self).__init__(prefix=prefix,deleted=deleted)
        self.set = DataObjectSet()

class DVRouterForwardInfo(DataObject):
    _prefix = 'viperflow.dvrouterforwardinfo'
    _indices = ("from_pynet","to_pynet")

    def __init__(self,prefix=None,deleted=None):
        super(DVRouterForwardInfo,self).__init__(prefix=prefix,deleted=deleted)
        self.info = []


class DVRouterForwardInfoRef(DataObject):
    _prefix = 'viperflow.dvrouterforwardrefinfo'
    _indices = ("from_pynet","to_pynet")

    def __init__(self,prefix=None,deleted=None):
        super(DVRouterForwardInfoRef,self).__init__(prefix=prefix,deleted=deleted)
        self.info = []


class DVRouterExternalAddressInfo(DataObject):
    _prefix = 'viperflow.dvrouterexternaladdressinfo'

    def __init__(self, prefix=None,deleted=None):
        super(DVRouterExternalAddressInfo,self).__init__(prefix=prefix,deleted=deleted)
        self.info = []

class VRouterSet(DataObject):
    _prefix = 'viperflow.vrouterset'

    def __init__(self,prefix=None,deleted=None):
        super(VRouterSet,self).__init__(prefix=prefix,deleted=deleted)
        self.set = DataObjectSet()


class RouterPort(DataObject):
    _prefix = 'viperflow.routerport'
    _indices = ('id',)


class VXLANEndpointSet(DataObject):
    _prefix = 'viperflow.vxlanendpointset'
    _indices = ('id',)    
    def __init__(self,prefix = None,deleted = None):
        super(VXLANEndpointSet, self).__init__(
                prefix = prefix,deleted = deleted)
        self.endpointlist = []


LogicalNetwork._register_auto_remove('VXLANEndpointSet', lambda x: [VXLANEndpointSet.default_key(x.id)])


class LogicalPortVXLANInfo(DataObject):
    _prefix = 'viperflow.logicalportvxlaninfo'
    _indices = ('id',)
    def __init__(self, prefix=None, deleted=False):
        super(LogicalPortVXLANInfo, self).__init__(prefix=prefix, deleted=deleted)
        self.endpoints = []

LogicalPort._register_auto_remove('LogicalPortVXLANInfo', lambda x: [LogicalPortVXLANInfo.default_key(x.id)])

