'''
Created on 2018/7/2

:author: hubo

Base walkers for network plugins
'''
from vlcp.utils.dataobject import create_new
from vlcp.utils.networkmodel import PhysicalNetwork, PhysicalNetworkMap,\
    PhysicalNetworkSet, PhysicalPort, PhysicalPortSet, LogicalNetworkSet,\
    LogicalNetwork, LogicalNetworkMap
from functools import partial


def default_processor(obj, *args, parameters, excluding=('id',), disabled=()):
    _updated = False
    for k,v in parameters.items():
        if k in disabled:
            raise ValueError(repr(k) + " cannot be updated")
        if k not in excluding:
            _updated = True
            setattr(obj, k, v)
    return _updated


def _false_processor(*args, **kwargs):
    return False


def default_physicalnetwork_keys(id_, parameters):
    return (PhysicalNetworkMap.default_key(id_),
            PhysicalNetworkSet.default_key())


def default_physicalnetwork_update_keys(id_, parameters):
    return (PhysicalNetworkMap.default_key(id_),)


def default_iterate_dict(parameter_dict):
    return parameter_dict.items()


def createphysicalnetwork(type, create_processor = partial(default_processor, excluding=('id', 'type')),
                                      reorder_dict = default_iterate_dict):
    """
    :param type: physical network type
    
    :param create_processor: create_processor(physicalnetwork, walk, write, *, parameters)
    """
    # create an new physical network
    def walker(walk, write, timestamp, parameters_dict):
        for key, parameters in reorder_dict(parameters_dict):
            try:
                value = walk(key)
            except KeyError:
                pass
            else:
                id_ = parameters['id']
                new_network = create_new(PhysicalNetwork, value, id_)
                new_network.type = type
                
                create_processor(new_network, walk, write, parameters=parameters)
                write(key, new_network)
                new_networkmap = PhysicalNetworkMap.create_instance(id_)
                new_networkmap.network = new_network.create_weakreference()
                write(new_networkmap.getkey(), new_networkmap)
                
                # Save into network set
                try:
                    physet = walk(PhysicalNetworkSet.default_key())
                except KeyError:
                    pass
                else:
                    physet.set.dataset().add(new_network.create_weakreference())
                    write(physet.getkey(), physet)
    return walker


def updatephysicalnetwork(update_processor = partial(default_processor, disabled=('type',)),
                                reorder_dict = default_iterate_dict):
    """
    :param update_processor: update_processor(physicalnetwork, walk, write, *, parameters)
    """
    def walker(walk, write, timestamp, parameters_dict):
        # do not need to check value is not None in a plugin; it is checked in the caller
        for key, parameters in reorder_dict(parameters_dict):
            try:
                value = walk(key)
            except KeyError:
                pass
            else:
                if update_processor(value, walk, write, parameters=parameters):
                    write(key, value)
    return walker


def default_physicalnetwork_delete_check(phynet, phymap, *args, parameters):
    if phymap.logicnetworks.dataset():
        raise ValueError("Must delete all logical networks of this physical network "+ parameters['id'])
    if phymap.ports.dataset():
        raise ValueError("Must delete all physical ports of this physical network " + parameters['id'])


def deletephysicalnetwork(check_processor = default_physicalnetwork_delete_check,
                                reorder_dict = default_iterate_dict):
    """
    :param check_processor: check_processor(physicalnetwork, physicalnetworkmap, walk, write, *, parameters)
    """
    def walker(walk, write, timestamp, parameters_dict):
        for key, parameters in reorder_dict(parameters_dict):
            try:
                value = walk(key)
            except KeyError:
                pass
            else:
                id_ = parameters['id']
                try:
                    phy_map = walk(PhysicalNetworkMap.default_key(id_))
                except KeyError:
                    pass
                else:
                    check_processor(value, phy_map, walk, write, parameters=parameters)
                    write(phy_map.getkey(), None)
                try:
                    phynetset = walk(PhysicalNetworkSet.default_key())
                except KeyError:
                    pass
                else:
                    phynetset.set.dataset().discard(value.create_weakreference())
                    write(phynetset.getkey(), phynetset)
                write(key, None)
    return walker


def default_physicalport_keys(phynet_id, parameters):
    return (PhysicalNetwork.default_key(phynet_id),
            PhysicalNetworkMap.default_key(phynet_id),
            PhysicalPortSet.default_key())


def createphysicalport(create_processor = partial(default_processor, excluding=('vhost', 'systemid',
                                                                                       'bridge', 'name',
                                                                                       'physicalnetwork')),
                       reorder_dict = default_iterate_dict):
    """
    :param create_processor: create_processor(physicalport, physicalnetwork, physicalnetworkmap, walk, write, *, parameters)
    """
    def walker(walk, write, timestamp, parameters_dict):
        for key, parameters in reorder_dict(parameters_dict):
            try:
                value = walk(key)
            except KeyError:
                pass
            else:
                p = create_new(PhysicalPort, value, parameters['vhost'], parameters['systemid'],
                                                    parameters['bridge'], parameters['name'])
                try:
                    physicalnetwork = walk(PhysicalNetwork.default_key(parameters['physicalnetwork']))
                except KeyError:
                    pass
                else:
                    # Should already been check from outside
                    p.physicalnetwork = physicalnetwork.create_reference()
                    try:
                        phymap = walk(PhysicalNetworkMap._network.leftkey(physicalnetwork))
                    except KeyError:
                        pass
                    else:
                        create_processor(p, physicalnetwork, phymap, walk, write, parameters=parameters)
                        phymap.ports.dataset().add(p.create_weakreference())
                        write(phymap.getkey(), phymap)
                try:
                    phyportset = walk(PhysicalPortSet.default_key())
                except KeyError:
                    pass
                else:
                    phyportset.set.dataset().add(p.create_weakreference())
                    write(phyportset.getkey(), phyportset)
                write(p.getkey(), p)
    return walker


def updatephysicalport(update_processor = partial(default_processor, excluding=('vhost', 'systemid',
                                                                                   'bridge', 'name'),
                                                                     disabled=('physicalnetwork',)),
                       reorder_dict = default_iterate_dict
                      ):
    """
    :param update_processor: update_processor(physcialport, walk, write, *, parameters)    
    """
    def walker(walk, write, timestamp, parameters_dict):
        for key, parameters in reorder_dict(parameters_dict):
            try:
                value = walk(key)
            except KeyError:
                pass
            else:
                if update_processor(value, walk, write, parameters=parameters):
                    write(key, value)
    return walker


def deletephysicalport(check_processor=_false_processor,
                       reorder_dict = default_iterate_dict):
    """
    :param check_processor: check_processor(physicalport, physicalnetwork, physicalnetworkmap,
                            walk, write *, parameters)
    """
    def walker(walk, write, timestamp, parameters_dict):
        for key, parameters in reorder_dict(parameters_dict):
            try:
                value = walk(key)
            except KeyError:
                pass
            else:
                try:
                    phynet = walk(value.physicalnetwork.getkey())
                except KeyError:
                    pass
                else:
                    try:
                        phymap = walk(PhysicalNetworkMap._network.leftkey(phynet))
                    except KeyError:
                        pass
                    else:
                        check_processor(value, phynet, phymap, walk, write, parameters=parameters)
                        phymap.ports.dataset().discard(value.create_weakreference())
                        write(phymap.getkey(), phymap)
                try:
                    physet = walk(PhysicalPortSet.default_key())
                except KeyError:
                    pass
                else:
                    physet.set.dataset().discard(value.create_weakreference())
                    write(physet.getkey(), physet)
                write(key, None)
    return walker


def default_logicalnetwork_keys(phynet_id, parameters):
    return (PhysicalNetwork.default_key(phynet_id),
            PhysicalNetworkMap.default_key(phynet_id),
            LogicalNetworkSet.default_key(),
            LogicalNetworkMap.default_key(parameters['id']))


def createlogicalnetwork(create_processor = partial(default_processor, excluding=('id', 'physicalnetwork')),
                         reorder_dict = default_iterate_dict):
    """
    :param create_processor: create_processor(logicalnetwork, logicalnetworkmap, physicalnetwork,
                             physicalnetworkmap, walk, write, *, parameters)
    """
    def walker(walk, write, timestamp, parameters_dict):
        for key, parameters in reorder_dict(parameters_dict):
            try:
                value = walk(key)
            except KeyError:
                pass
            else:
                id_ = parameters['id']
                lognet = create_new(LogicalNetwork, value, id_)
                logmap = LogicalNetworkMap.create_instance(id_)
                logmap.network = lognet.create_reference()
                try:
                    phynet = walk(PhysicalNetwork.default_key(parameters['physicalnetwork']))
                except KeyError:
                    pass
                else:
                    lognet.physicalnetwork = phynet.create_reference()
                    try:
                        phymap = walk(PhysicalNetworkMap._network.leftkey(phynet))
                    except KeyError:
                        pass
                    else:
                        create_processor(lognet, logmap, phynet, phymap, walk, write, parameters=parameters)
                        phymap.logicnetworks.dataset().add(lognet.create_weakreference())
                        write(phymap.getkey(), phymap)
                        write(lognet.getkey(), lognet)
                        write(logmap.getkey(), logmap)
                try:
                    logicalnetworkset = walk(LogicalNetworkSet.default_key())
                except KeyError:
                    pass
                else:
                    logicalnetworkset.set.dataset().add(lognet.create_weakreference())
                    write(logicalnetworkset.getkey(), logicalnetworkset)
    return walker


def updatelogicalnetwork(update_processor = partial(default_processor, excluding=('id',),
                                                                       disabled=('physicalnetwork',)),
                         reorder_dict = default_iterate_dict):
    """
    :param update_processor: update_processor(logicalnetwork, walk, write, *, parameters)
    """
    def walker(walk, write, timestamp, parameters_dict):
        for key, parameters in reorder_dict(parameters_dict):
            try:
                value = walk(key)
            except KeyError:
                pass
            else:
                if update_processor(value, walk, write, parameters=parameters):
                    write(key, value)
    return walker


def default_logicalnetwork_delete_check(logicalnetwork, logicalnetworkmap, *args, parameters):
    if logicalnetworkmap.subnets.dataset():
        raise ValueError("Must delete all subnets of this logical network "+ logicalnetwork.id)
    if logicalnetworkmap.ports.dataset():
        raise ValueError("Must delete all logical ports of this logical network " + logicalnetwork.id)


def deletelogicalnetwork(check_processor=default_logicalnetwork_delete_check,
                         reorder_dict = default_iterate_dict):
    """
    :param check_processor: check_processor(logicalnetwork, logicalnetworkmap,
                                            physicalnetwork, physicalnetworkmap,
                                            walk, write, *, parameters)
    """
    def walker(walk, write, timestamp, parameters_dict):
        for key, parameters in reorder_dict(parameters_dict):
            try:
                value = walk(key)
            except KeyError:
                pass
            else:
                try:
                    logmap = walk(LogicalNetworkMap._network.leftkey(key))
                except KeyError:
                    pass
                else:
                    try:
                        phynet = walk(value.physicalnetwork.getkey())
                    except KeyError:
                        pass
                    else:
                        try:
                            phymap = walk(PhysicalNetworkMap._network.leftkey(phynet))
                        except KeyError:
                            pass
                        else:
                            check_processor(value, logmap, phynet, phymap, walk, write, parameters=parameters)
                            phymap.logicnetworks.dataset().discard(value.create_weakreference())
                            write(phymap.getkey(), phymap)
                            write(key, None)
                            write(logmap.getkey(), None)
                try:
                    logicalnetworkset = walk(LogicalNetworkSet.default_key())
                except KeyError:
                    pass
                else:
                    logicalnetworkset.set.dataset().discard(value.create_weakreference())
                    write(logicalnetworkset.getkey(), logicalnetworkset)
    return walker
