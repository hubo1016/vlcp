"""
Physical network with type=local can have unlimited logical networks,
but it cannot have physical ports. So, logical ports in the same logical
network can access each other only when they are on the same host.
"""

from vlcp.server.module import Module,publicapi
from vlcp.event.runnable import RoutineContainer
from vlcp.config.config import defaultconfig
from vlcp.utils.networkplugin import createphysicalnetwork,\
    updatephysicalnetwork, default_physicalnetwork_keys, deletephysicalnetwork,\
    deletephysicalport, default_physicalport_keys, createlogicalnetwork,\
    default_logicalnetwork_keys, updatelogicalnetwork,\
    deletelogicalnetwork


@defaultconfig
class NetworkLocalDriver(Module):
    """
    Network driver for local networks. Local networks cannot have physical ports; logical networks
    in local networks do not have external connectivities, only endpoints on the same server can
    access each other.
    """
    def __init__(self,server):
        super(NetworkLocalDriver,self).__init__(server)
        self.app_routine = RoutineContainer(self.scheduler)
        self.app_routine.main = self._main
        self.routines.append(self.app_routine)
        self.createAPI(
                       publicapi(self.createphysicalnetwork,
                                    criteria=lambda type: type == 'local'),
                       publicapi(self.updatephysicalnetwork,
                                    criteria=lambda type: type == 'local'),
                       publicapi(self.deletephysicalnetwork,
                                    criteria=lambda type: type == 'local'),
                       publicapi(self.createphysicalport,
                                    criteria=lambda type: type == 'local'),
                       publicapi(self.updatephysicalport,
                                    criteria=lambda type: type == 'local'),
                       publicapi(self.deletephysicalport,
                                    criteria=lambda type: type == 'local'),
                       publicapi(self.createlogicalnetwork,
                                    criteria=lambda type: type == 'local'),
                       publicapi(self.updatelogicalnetwork,
                                    criteria=lambda type: type == 'local'),
                       publicapi(self.deletelogicalnetwork,
                                    criteria=lambda type: type == "local"),
                       #used in IOprocessing module
                       publicapi(self.createioflowparts,
                                    criteria=lambda connection,logicalnetwork,
                                    physicalport,logicalnetworkid,physicalportid:
                                    logicalnetwork.physicalnetwork.type == "local")

                       )

    async def _main(self):
        self._logger.info("network_local_driver running ---")

    def createphysicalnetwork(self, type):
        # create an new physical network
        return createphysicalnetwork(type), default_physicalnetwork_keys
    
    def updatephysicalnetwork(self, type):
        # update a physical network
        return updatephysicalnetwork(), None
    
    def deletephysicalnetwork(self, type):
        return deletephysicalnetwork(), default_physicalnetwork_keys
    
    def createphysicalport(self, type):
        raise ValueError("A physical network with type=local cannot have physical ports")
        
    def updatephysicalport(self, type):
        raise ValueError("A physical network with type=local cannot have physical ports")
   
    def deletephysicalport(self, type):
        return deletephysicalport(), default_physicalport_keys

    def createlogicalnetwork(self,type):
        return createlogicalnetwork(), default_logicalnetwork_keys

    def updatelogicalnetwork(self,type):
        return updatelogicalnetwork(), None

    def deletelogicalnetwork(self,type):
        return deletelogicalnetwork(), default_logicalnetwork_keys

    def createioflowparts(self,connection,logicalnetwork,physicalport,logicalnetworkid,physicalportid):
        return [], [], [], [], []

