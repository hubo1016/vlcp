"""
Physical network 
"""
from vlcp.server.module import Module, publicapi
from vlcp.event.runnable import RoutineContainer
from vlcp.utils.networkmodel import PhysicalNetworkMap, PhysicalNetwork
from vlcp.utils.ethernet import ETHERTYPE_8021Q
from vlcp.config.config import defaultconfig
from vlcp.utils.networkplugin import createphysicalnetwork,\
    updatephysicalnetwork, default_physicalnetwork_keys, deletephysicalnetwork,\
    deletephysicalport, default_physicalport_keys, createlogicalnetwork,\
    default_logicalnetwork_keys, default_processor, updatelogicalnetwork,\
    deletelogicalnetwork, createphysicalport, updatephysicalport,\
    default_logicalnetwork_delete_check
from vlcp.utils.exceptions import WalkKeyNotRetrieved
from vlcp.utils.walkerlib import ensure_keys


@defaultconfig
class NetworkVlanDriver(Module):
    """
    Network driver for VXLAN networks. When creating a VXLAN type physical network,
    you must specify an extra option ``vlanrange``.
    """
    def __init__(self,server):
        super(NetworkVlanDriver,self).__init__(server)
        self.app_routine = RoutineContainer(self.scheduler)
        self.app_routine.main = self._main
        self.routines.append(self.app_routine)
        self.createAPI(
                       publicapi(self.createphysicalnetwork,
                                    criteria=lambda type: type == 'vlan'),
                       publicapi(self.updatephysicalnetwork,
                                    criteria=lambda type: type == 'vlan'),
                       publicapi(self.deletephysicalnetwork,
                                    criteria=lambda type: type == 'vlan'),
                       publicapi(self.createphysicalport,
                                    criteria=lambda type: type == 'vlan'),
                       publicapi(self.updatephysicalport,
                                    criteria=lambda type: type == 'vlan'),
                       publicapi(self.deletephysicalport,
                                    criteria=lambda type: type == 'vlan'),
                       publicapi(self.createlogicalnetwork,
                                    criteria=lambda type: type == 'vlan'),
                       publicapi(self.updatelogicalnetwork,
                                    criteria=lambda type: type == 'vlan'),
                       publicapi(self.deletelogicalnetwork,
                                    criteria=lambda type: type == "vlan"),
                       #used in IOprocessing module
                       publicapi(self.createioflowparts,
                                    criteria=lambda connection,logicalnetwork,
                                    physicalport,logicalnetworkid,physicalportid:
                                    logicalnetwork.physicalnetwork.type == "vlan")
                       )

    async def _main(self):
        self._logger.info("network_vlan_driver running ---")

    def createphysicalnetwork(self, type):
        # create an new physical network
        def create_physicalnetwork_processor(physicalnetwork, walk, write, *, parameters):
            if 'vlanrange' not in parameters:
                raise ValueError('must specify vlanrange with physical network type=vlan')
            _check_vlanrange(parameters['vlanrange'])
            return default_processor(physicalnetwork, parameters=parameters, excluding=('id', 'type'))
        return createphysicalnetwork(type, create_processor=create_physicalnetwork_processor), default_physicalnetwork_keys
    
    def updatephysicalnetwork(self, type):
        # update a physical network
        def update_physicalnetwork_keys(id_, parameters):
            if 'vlanrange' in parameters:
                return (PhysicalNetworkMap.default_key(id_),)
            else:
                return ()
        def update_physicalnetwork_processor(physicalnetwork, walk, write, *, parameters):
            if 'vlanrange' in parameters:
                _check_vlanrange(parameters['vlanrange'])
                try:
                    phymap = walk(PhysicalNetworkMap.default_key(physicalnetwork.id))
                except WalkKeyNotRetrieved:
                    pass
                else:
                    _check_vlanrange_allocation(parameters['vlanrange'], phymap.network_allocation)
            return default_processor(physicalnetwork, parameters=parameters, disabled=('type',))
        return updatephysicalnetwork(update_processor=update_physicalnetwork_processor), update_physicalnetwork_keys
    
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
            if 'vlanid' in parameters:
                # Allocate this vlanid
                vlanid = int(parameters['vlanid'])
                if _isavaliablevlanid(physicalnetwork.vlanrange, physicalnetworkmap.network_allocation, vlanid):
                    physicalnetworkmap.network_allocation[str(vlanid)] = logicalnetwork.create_weakreference()
                    write(physicalnetworkmap.getkey(), physicalnetworkmap)
                else:
                    raise ValueError("Specified VLAN ID " + str(vlanid) + " allocated or not in range")
            else:
                # Allocate a vlanid from range
                vlanid = _findavaliablevlanid(physicalnetwork.vlanrange, physicalnetworkmap.network_allocation)
                if vlanid is None:
                    raise ValueError("no available VLAN id in physical network " + physicalnetwork.id)
                physicalnetworkmap.network_allocation[str(vlanid)] = logicalnetwork.create_weakreference()
                write(physicalnetworkmap.getkey(), physicalnetworkmap)
            logicalnetwork.vlanid = vlanid
            write(logicalnetwork.getkey(), logicalnetwork)
            return default_processor(logicalnetwork, parameters=parameters, excluding=('id', 'physicalnetwork', 'vlanid'))
        # Process logical networks with specified IDs first
        return createlogicalnetwork(create_processor=logicalnetwork_processor,
                                    reorder_dict=lambda x: sorted(x.items(), key=lambda y: 'vlanid' in y[1], reverse=True)),\
               default_logicalnetwork_keys

    def updatelogicalnetwork(self, type):
        # When updating VLAN ids, Must first deallocate all VLAN ids, then allocate all
        # Chaining two walkers for this
        def update_logicalnetwork_keys(id_, parameters):
            if 'vlanid' in parameters:
                return (PhysicalNetwork.default_key(id_),
                        PhysicalNetworkMap.default_key(id_))
            else:
                return ()
        def deallocate_processor(logicalnetwork, walk, write, *, parameters):
            if 'vlanid' in parameters:
                try:
                    phymap = walk(PhysicalNetworkMap._network.leftkey(logicalnetwork.physicalnetwork))
                except WalkKeyNotRetrieved:
                    pass
                else:
                    del phymap.network_allocation[str(logicalnetwork.vlanid)]
                    write(phymap.getkey(), phymap)
            return False
        deallocate_walker = updatelogicalnetwork(update_processor=deallocate_processor)
        def allocate_processor(logicalnetwork, walk, write, *, parameters):
            if 'vlanid' in parameters:
                try:
                    phynet = walk(logicalnetwork.physicalnetwork.getkey())
                    phymap = walk(PhysicalNetworkMap._network.leftkey(logicalnetwork.physicalnetwork))
                except WalkKeyNotRetrieved:
                    ensure_keys(PhysicalNetworkMap._network.leftkey(logicalnetwork.physicalnetwork))
                else:
                    vlanid = int(parameters['vlanid'])
                    if _isavaliablevlanid(phynet.vlanrange, phymap.network_allocation, vlanid):
                        phymap.network_allocation[str(vlanid)] = logicalnetwork.create_weakreference()
                        write(phymap.getkey(), phymap)
                    else:
                        raise ValueError("Specified VLAN ID " + str(vlanid) + " allocated or not in range")
                    logicalnetwork.vlanid = vlanid
                    write(logicalnetwork.getkey(), logicalnetwork)
            return default_processor(logicalnetwork, parameters=parameters, excluding=('id', 'vlanid'),
                                                                            disabled=('physicalnetwork',))
        allocate_walker = updatelogicalnetwork(update_processor=allocate_processor)
        def walker(walk, write, timestamp, parameters_dict):
            deallocate_walker(walk, write, timestamp, parameters_dict)
            allocate_walker(walk, write, timestamp, parameters_dict)
        return walker, update_logicalnetwork_keys

    def deletelogicalnetwork(self, type):
        def check_processor(logicalnetwork, logicalnetworkmap,
                            physicalnetwork, physicalnetworkmap,
                            walk, write, *, parameters):
            default_logicalnetwork_delete_check(logicalnetwork, logicalnetworkmap,
                                                physicalnetwork, physicalnetworkmap,
                                                walk, write, parameters=parameters)
            del physicalnetworkmap.network_allocation[str(logicalnetwork.vlanid)]
            write(physicalnetworkmap.getkey(), physicalnetworkmap)
        return deletelogicalnetwork(check_processor=check_processor), default_logicalnetwork_keys

    def createioflowparts(self,connection,logicalnetwork,physicalport,logicalnetworkid,physicalportid):

        #
        #  1. used in IOProcessing , when physicalport add to logicalnetwork 
        #     return : input flow match vlan oxm, input flow vlan parts actions
        #              output flow vlan parts actions, output group bucket
        #
        
        input_match_oxm = [
                    connection.openflowdef.create_oxm(
                        connection.openflowdef.OXM_OF_VLAN_VID,
                        logicalnetwork.vlanid|connection.openflowdef.OFPVID_PRESENT)
                ]

        input_action = [
                   connection.openflowdef.ofp_action(type = 
                        connection.openflowdef.OFPAT_POP_VLAN)    
              ]

        output_action = [
                    connection.openflowdef.ofp_action_push(ethertype=ETHERTYPE_8021Q),
                    connection.openflowdef.ofp_action_set_field(
                            field = connection.openflowdef.create_oxm(
                                    connection.openflowdef.OXM_OF_VLAN_VID,
                                    logicalnetwork.vlanid |
                                    connection.openflowdef.OFPVID_PRESENT
                                )
                        ),
                    connection.openflowdef.ofp_action_output(
                            port = physicalportid 
                        )
                ]
        output_action2 = [
                    connection.openflowdef.ofp_action_push(ethertype=ETHERTYPE_8021Q),
                    connection.openflowdef.ofp_action_set_field(
                            field = connection.openflowdef.create_oxm(
                                    connection.openflowdef.OXM_OF_VLAN_VID,
                                    logicalnetwork.vlanid |
                                    connection.openflowdef.OFPVID_PRESENT
                                )
                        ),
                    connection.openflowdef.ofp_action_output(
                            port = connection.openflowdef.OFPP_IN_PORT
                        )
                ]
        
        # this action is same as ouput_action  on type vlan
        output_group_bucket_action = [
                    connection.openflowdef.ofp_action_push(ethertype=ETHERTYPE_8021Q),
                    connection.openflowdef.ofp_action_set_field(
                            field = connection.openflowdef.create_oxm(
                                    connection.openflowdef.OXM_OF_VLAN_VID,
                                    logicalnetwork.vlanid |
                                    connection.openflowdef.OFPVID_PRESENT
                                )
                        ),
                    connection.openflowdef.ofp_action_output(
                            port = physicalportid
                        )
                ]

        return input_match_oxm,input_action,output_action,output_group_bucket_action,output_action2
#
# utils function
#
def _check_vlanrange(vlanrange):
    lastend = 0
    for start,end in vlanrange:
        if start <= 0 or end > 4095:
            raise ValueError('VLAN ID out of range (1 - 4095)')
        if start > end or start <= lastend:
            raise ValueError('VLAN sequences overlapped or disordered: [%r, %r]' % (start, end))
        lastend = end


def _check_vlanrange_allocation(vlanrange, allocation):
    allocated_ids = sorted(int(k) for k in allocation.keys())
    range_iter = iter(vlanrange)
    current_range = None
    for id_ in allocated_ids:
        while current_range is None or current_range[1] < id_:
            try:
                current_range = next(range_iter)
            except StopIteration:
                raise ValueError("Allocated VLAN ID " + str(id_) + " not in new VLAN range")
        if current_range[0] > id_:
            raise ValueError("Allocated VLAN ID " + str(id_) + " not in new VLAN range")


def _findavaliablevlanid(vlanrange,allocated):
    
    vlanid = None
    for vr in vlanrange:
        find = False
        for v in range(vr[0], vr[1] + 1):
            if str(v) not in allocated:
                vlanid = v
                find = True
                break

        if find:
            break
    return vlanid

def _isavaliablevlanid(vlanrange,allocated,vlanid):
    
    find = False
    for start ,end in vlanrange:
        if start <= int(vlanid) <= end:
            find = True
            break

    if find:
        if str(vlanid) not in allocated:
            find = True
        else:
            find = False
    else:
        find = False

    return find
