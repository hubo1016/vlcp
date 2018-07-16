"""
Physical network with type=native does not have isolation technique,
so there can be only one logical network in each physical network.
"""
from vlcp.server.module import Module, publicapi
from vlcp.event.runnable import RoutineContainer
from vlcp.config.config import defaultconfig
from vlcp.utils.networkplugin import createphysicalnetwork,\
    updatephysicalnetwork, default_physicalnetwork_keys, deletephysicalnetwork,\
    deletephysicalport, default_physicalport_keys, createlogicalnetwork,\
    default_logicalnetwork_keys, default_processor, updatelogicalnetwork,\
    deletelogicalnetwork, createphysicalport, updatephysicalport

@defaultconfig
class NetworkNativeDriver(Module):
    """
    Network driver for native networks. Native network is a physical network
    provides only one logical network capacity. Packets from the logical network
    is directly forwarded to the physical network.
    """
    def __init__(self,server):
        super(NetworkNativeDriver,self).__init__(server)
        self.app_routine = RoutineContainer(self.scheduler)
        self.app_routine.main = self._main
        self.routines.append(self.app_routine)
        self.createAPI(
                       publicapi(self.createphysicalnetwork,
                                    criteria=lambda type: type == 'native'),
                       publicapi(self.updatephysicalnetwork,
                                    criteria=lambda type: type == 'native'),
                       publicapi(self.deletephysicalnetwork,
                                    criteria=lambda type: type == 'native'),
                       publicapi(self.createphysicalport,
                                    criteria=lambda type: type == 'native'),
                       publicapi(self.updatephysicalport,
                                    criteria=lambda type: type == 'native'),
                       publicapi(self.deletephysicalport,
                                    criteria=lambda type: type == 'native'),
                       publicapi(self.createlogicalnetwork,
                                    criteria=lambda type: type == 'native'),
                       publicapi(self.updatelogicalnetwork,
                                    criteria=lambda type: type == 'native'),
                       publicapi(self.deletelogicalnetwork,
                                    criteria=lambda type: type == "native"),
                       #used in IOprocessing module
                       publicapi(self.createioflowparts,
                                    criteria=lambda connection,logicalnetwork,
                                    physicalport,logicalnetworkid,physicalportid:
                                    logicalnetwork.physicalnetwork.type == "native")

                       )

    async def _main(self):
        self._logger.info("network_native_driver running ---")

    def createphysicalnetwork(self, type):
        # create an new physical network
        return createphysicalnetwork(type), default_physicalnetwork_keys
    
    def updatephysicalnetwork(self, type):
        # update a physical network
        return updatephysicalnetwork(), None
    
    def deletephysicalnetwork(self, type):
        return deletephysicalnetwork(), default_physicalnetwork_keys
    
    def createphysicalport(self, type):
        return createphysicalport(), default_physicalport_keys
        
    def updatephysicalport(self, type):
        return updatephysicalport(), None
   
    def deletephysicalport(self, type):
        return deletephysicalport(), default_physicalport_keys

    def createlogicalnetwork(self, type):
        def logicalnetwork_processor(logicalnetwork, logicalnetworkmap, physicalnetwork,
                                     physicalnetworkmap, walk, write, *, parameters):
            if physicalnetworkmap.logicnetworks.dataset():
                raise ValueError("physical network with type=native can only have one logical network")
            return default_processor(logicalnetwork, parameters=parameters, excluding=('id', 'physicalnetwork'))
        return createlogicalnetwork(create_processor=logicalnetwork_processor), default_logicalnetwork_keys

    def updatelogicalnetwork(self, type):
        return updatelogicalnetwork(), None

    def deletelogicalnetwork(self, type):
        return deletelogicalnetwork(), default_logicalnetwork_keys

    def createioflowparts(self,connection,logicalnetwork,physicalport,logicalnetworkid,physicalportid):

        #
        #  1. used in IOProcessing , when physicalport add to logicalnetwork 
        #     return : input flow match vxlan vni, input flow vlan parts actions
        #              output flow vxlan parts actions, output group bucket
        #
        
        input_match_oxm = []
        input_action = []
        output_action = [
                    connection.openflowdef.ofp_action_output(
                           port = physicalportid 
                        )
                ]

        output_group_bucket_action = [
                    connection.openflowdef.ofp_action_output(
                            port = physicalportid
                        )
                ]
        output_action2 = [
                    connection.openflowdef.ofp_action_output(
                           port = connection.openflowdef.OFPP_IN_PORT
                        )
                ]
        return input_match_oxm,input_action,output_action,output_group_bucket_action,output_action2
