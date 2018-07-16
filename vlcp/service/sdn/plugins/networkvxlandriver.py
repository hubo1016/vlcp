from vlcp.server.module import Module, publicapi
from vlcp.event.runnable import RoutineContainer
from vlcp.utils.networkmodel import PhysicalNetworkMap, PhysicalNetwork
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
class NetworkVxlanDriver(Module):
    """
    Network driver for VXLAN networks. When creating a VXLAN type physical network,
    you must specify an extra option ``vnirange``.
    """
    def __init__(self,server):
        super(NetworkVxlanDriver,self).__init__(server)
        self.app_routine = RoutineContainer(self.scheduler)
        self.app_routine.main = self._main
        self.routines.append(self.app_routine)
        self.createAPI(
                       publicapi(self.createphysicalnetwork,
                                    criteria=lambda type: type == 'vxlan'),
                       publicapi(self.updatephysicalnetwork,
                                    criteria=lambda type: type == 'vxlan'),
                       publicapi(self.deletephysicalnetwork,
                                    criteria=lambda type: type == 'vxlan'),
                       publicapi(self.createphysicalport,
                                    criteria=lambda type: type == 'vxlan'),
                       publicapi(self.updatephysicalport,
                                    criteria=lambda type: type == 'vxlan'),
                       publicapi(self.deletephysicalport,
                                    criteria=lambda type: type == 'vxlan'),
                       publicapi(self.createlogicalnetwork,
                                    criteria=lambda type: type == 'vxlan'),
                       publicapi(self.updatelogicalnetwork,
                                    criteria=lambda type: type == 'vxlan'),
                       publicapi(self.deletelogicalnetwork,
                                    criteria=lambda type: type == "vxlan")
                       )

    async def _main(self):
        self._logger.info("network_vxlan_driver running ---")

    def createphysicalnetwork(self, type):
        # create an new physical network
        def create_physicalnetwork_processor(physicalnetwork, walk, write, *, parameters):
            if 'vnirange' not in parameters:
                raise ValueError('must specify vnirange with physical network type=vxlan')
            _check_vnirange(parameters['vnirange'])
            return default_processor(physicalnetwork, parameters=parameters, excluding=('id', 'type'))
        return createphysicalnetwork(type, create_processor=create_physicalnetwork_processor),\
                default_physicalnetwork_keys

    def updatephysicalnetwork(self, type):
        # update a physical network
        def update_physicalnetwork_keys(id_, parameters):
            if 'vnirange' in parameters:
                return (PhysicalNetworkMap.default_key(id_),)
            else:
                return ()
        def update_physicalnetwork_processor(physicalnetwork, walk, write, *, parameters):
            if 'vnirange' in parameters:
                _check_vnirange(parameters['vnirange'])
                try:
                    phymap = walk(PhysicalNetworkMap.default_key(physicalnetwork.id))
                except WalkKeyNotRetrieved:
                    pass
                else:
                    _check_vnirange_allocation(parameters['vnirange'], phymap.network_allocation)
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
            if 'vni' in parameters:
                # Allocate this vni
                vni = int(parameters['vni'])
                if _isavaliablevni(physicalnetwork.vnirange, physicalnetworkmap.network_allocation, vni):
                    physicalnetworkmap.network_allocation[str(vni)] = logicalnetwork.create_weakreference()
                    write(physicalnetworkmap.getkey(), physicalnetworkmap)
                else:
                    raise ValueError("Specified VNI " + str(vni) + " allocated or not in range")
            else:
                # Allocate a vni from range
                vni = _findavaliablevni(physicalnetwork.vnirange, physicalnetworkmap.network_allocation)
                if vni is None:
                    raise ValueError("no available VNI in physical network " + physicalnetwork.id)
                physicalnetworkmap.network_allocation[str(vni)] = logicalnetwork.create_weakreference()
                write(physicalnetworkmap.getkey(), physicalnetworkmap)
            logicalnetwork.vni = vni
            write(logicalnetwork.getkey(), logicalnetwork)
            return default_processor(logicalnetwork, parameters=parameters, excluding=('id', 'physicalnetwork', 'vni'))
        # Process logical networks with specified IDs first
        return createlogicalnetwork(create_processor=logicalnetwork_processor,
                                    reorder_dict=lambda x: sorted(x.items(), key=lambda y: 'vni' in y[1], reverse=True)),\
               default_logicalnetwork_keys

    def updatelogicalnetwork(self, type):
        # When updating VLAN ids, Must first deallocate all VLAN ids, then allocate all
        # Chaining two walkers for this
        def update_logicalnetwork_keys(id_, parameters):
            if 'vni' in parameters:
                return (PhysicalNetwork.default_key(id_),
                        PhysicalNetworkMap.default_key(id_))
            else:
                return ()
        def deallocate_processor(logicalnetwork, walk, write, *, parameters):
            if 'vni' in parameters:
                try:
                    phymap = walk(PhysicalNetworkMap._network.leftkey(logicalnetwork.physicalnetwork))
                except WalkKeyNotRetrieved:
                    pass
                else:
                    del phymap.network_allocation[str(logicalnetwork.vni)]
                    write(phymap.getkey(), phymap)
            return False
        deallocate_walker = updatelogicalnetwork(update_processor=deallocate_processor)
        def allocate_processor(logicalnetwork, walk, write, *, parameters):
            if 'vni' in parameters:
                try:
                    phynet = walk(logicalnetwork.physicalnetwork.getkey())
                    phymap = walk(PhysicalNetworkMap._network.leftkey(logicalnetwork.physicalnetwork))
                except WalkKeyNotRetrieved:
                    ensure_keys(PhysicalNetworkMap._network.leftkey(logicalnetwork.physicalnetwork))
                else:
                    vni = int(parameters['vni'])
                    if _isavaliablevni(phynet.vnirange, phymap.network_allocation, vni):
                        phymap.network_allocation[str(vni)] = logicalnetwork.create_weakreference()
                        write(phymap.getkey(), phymap)
                    else:
                        raise ValueError("Specified VNI " + str(vni) + " allocated or not in range")
                    logicalnetwork.vni = vni
                    write(logicalnetwork.getkey(), logicalnetwork)
            return default_processor(logicalnetwork, parameters=parameters, excluding=('id', 'vni'),
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
            del physicalnetworkmap.network_allocation[str(logicalnetwork.vni)]
            write(physicalnetworkmap.getkey(), physicalnetworkmap)
        return deletelogicalnetwork(check_processor=check_processor), default_logicalnetwork_keys


def _check_vnirange(vnirange):
    lastend = 0
    for start,end in vnirange:
        if start <= 0 or end > (1 << 24) - 1:
            raise ValueError('VNI out of range (1 - 4095)')
        if start > end or start <= lastend:
            raise ValueError('VNI sequences overlapped or disordered: [%r, %r]' % (start, end))
        lastend = end


def _check_vnirange_allocation(vnirange, allocation):
    allocated_ids = sorted(int(k) for k in allocation.keys())
    range_iter = iter(vnirange)
    current_range = None
    for id_ in allocated_ids:
        while current_range is None or current_range[1] < id_:
            try:
                current_range = next(range_iter)
            except StopIteration:
                raise ValueError("Allocated VNI " + str(id_) + " not in new VNI range")
        if current_range[0] > id_:
            raise ValueError("Allocated VNI " + str(id_) + " not in new VNI range")


def _findavaliablevni(vnirange,allocated):
    
    vni = None
    for vr in vnirange:
        find = False
        for v in range(vr[0],vr[1] + 1):
            if str(v) not in allocated:
                vni = v
                find = True
                break

        if find:
            break
    return vni


def _isavaliablevni(vnirange,allocated,vni):
    
    find = False
    for start,end in vnirange:
        if start <= int(vni) <= end:
            find = True
            break

    if find:
        if str(vni) not in allocated:
            find = True
        else:
            find = False
    else:
        find = False

    return find
