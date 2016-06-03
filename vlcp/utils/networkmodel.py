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

class LogicalPortSet(DataObject):
    _prefix = 'viperflow.logcialportset'
    
    def __init__(self,prefix = None,deleted = None):
        super(LogicalPortSet,self).__init__(prefix = prefix,
                deleted = deleted)
        self.set = DataObjectSet()

class SubNet(DataObject):
    _prefix = 'viperflow.subnet'
    _indices = 'id'


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


class VXLANEndpointSet(DataObject):
    _prefix = 'viperflow.vxlanendpointset'
    _indices = ('id',)    
    def __init__(self,prefix = None,deleted = None):
        super(VXLANEndpointSet, self).__init__(
                prefix = prefix,deleted = deleted)
        self.endpointlist = []

