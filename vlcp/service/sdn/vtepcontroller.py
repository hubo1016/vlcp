'''
Created on 2016/12/1

:author: hubo
'''
from vlcp.config import defaultconfig
from vlcp.server.module import Module, api
from vlcp.event.runnable import RoutineContainer

@defaultconfig
class VtepController(Module):
    def __init__(self, server):
        Module.__init__(self, server)
        self.apiroutine = RoutineContainer(self.scheduler)
        self.createAPI(api(self.listphysicalports, self.apiroutine),
                       api(self.updatelogicalswitch, self.apiroutine),
                       api(self.unbindlogicalswitch, self.apiroutine))
    
    def listphysicalports(self, physicalswitch = None):
        '''
        Get physical ports list from this controller, grouped by physical switch name
        
        :param physicalswitch: physicalswitch name. Return all ports if is None.
        
        :return: dictionary: {physicalswitch: [physicalports]} e.g. {'ps1': ['port1', 'port2']}
        '''
        pass
    
    def updatelogicalswitch(self, physicalswitch, physicalport, vlanid, logicalnetwork, logicalports):
        '''
        Bind VLAN on physicalport to specified logical network, and update logical port vxlan info
        
        :param physicalswitch: physical switch name, should be the name in PhysicalSwitch table of OVSDB vtep database
        
        :param physicalport: physical port name, should be the name in OVSDB vtep database
        
        :param vlanid: the vlan tag used for this logicalswitch
        
        :param logicalnetwork: the logical network id, will also be the logical switch id
        
        :param logicalports: a list of logical port IDs. The VXLAN info of these ports will be updated.
        '''
    
    def unbindlogicalswitch(self, physicalswitch, physicalport, vlanid, logicalnetwork):
        '''
        Remove bind of a physical port
        
        :param physicalswitch: physical switch name, should be the name in PhysicalSwitch table of OVSDB vtep database
        
        :param physicalport: physical port name, should be the name in OVSDB vtep database
        
        :param vlanid: the vlan tag used for this logicalswitch
        
        :param logicalnetwork: the logical network id, will also be the logical switch id
        '''
        