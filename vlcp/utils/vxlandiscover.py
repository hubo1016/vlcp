'''
Created on 2016/12/15

:author: hubo

Refactoring from vxlancast module, to share the logic with hardware-vtep
'''
from vlcp.utils.networkmodel import VXLANEndpointSet, LogicalNetworkMap,\
    LogicalPort, LogicalPortVXLANInfo
import itertools
from vlcp.server.module import callAPI
from vlcp.utils.ethernet import ip4_addr


def lognet_vxlan_walker(prepush = True):
    """
    Return a walker function to retrieve necessary information from ObjectDB
    """
    def _walk_lognet(key, value, walk, save):
        save(key)
        if value is None:
            return
        try:
            phynet = walk(value.physicalnetwork.getkey())
        except KeyError:
            pass
        else:
            if phynet is not None and getattr(phynet, 'type') == 'vxlan':
                try:
                    vxlan_endpoint_key = VXLANEndpointSet.default_key(value.id)
                    walk(vxlan_endpoint_key)
                except KeyError:
                    pass
                else:
                    save(vxlan_endpoint_key)
                if prepush:
                    # Acquire all logical ports
                    try:
                        netmap = walk(LogicalNetworkMap.default_key(value.id))
                    except KeyError:
                        pass
                    else:
                        save(netmap.getkey())
                        for logport in netmap.ports.dataset():
                            try:
                                _ = walk(logport.getkey())
                            except KeyError:
                                pass
                            else:
                                save(logport.getkey())
                            try:
                                _, (portid,) = LogicalPort._getIndices(logport.getkey())
                                portinfokey = LogicalPortVXLANInfo.default_key(portid)
                                _ = walk(portinfokey)
                            except KeyError:
                                pass
                            else:
                                save(portinfokey)                            
    return _walk_lognet

def update_vxlaninfo(container, network_ip_dict, created_ports, removed_ports,
                ovsdb_vhost, system_id, bridge,
                allowedmigrationtime, refreshinterval):
    '''
    Do an ObjectDB transact to update all VXLAN informations
    
    :param container: Routine container
    
    :param network_ip_dict: a {logicalnetwork_id: tunnel_ip} dictionary
    
    :param created_ports: logical ports to be added, a {logicalport_id: tunnel_ip} dictionary
    
    :param removed_ports: logical ports to be removed, a {logicalport_id: tunnel_ip} dictionary
    
    :param ovsdb_vhost: identifier for the bridge, vhost name
    
    :param system_id: identifier for the bridge, OVSDB systemid
    
    :param bridge: identifier for the bridge, bridge name
    
    :param allowedmigrationtime: time allowed for port migration, secondary endpoint info will be removed
                                 after this time
    
    :param refreshinterval: refreshinterval * 2 will be the timeout for network endpoint
    '''
    network_list = list(network_ip_dict.keys())
    vxlanendpoint_list = [VXLANEndpointSet.default_key(n) for n in network_list]
    all_tun_ports2 = list(set(created_ports.keys()).union(set(removed_ports.keys())))
    def update_vxlanendpoints(keys, values, timestamp):
        # values = List[VXLANEndpointSet]
        # endpointlist is [src_ip, vhost, systemid, bridge, expire]
        for v,n in zip(values[0:len(network_list)], network_list):
            if v is not None:
                v.endpointlist = [ep for ep in v.endpointlist
                                  if (ep[1], ep[2], ep[3]) != (ovsdb_vhost, system_id, bridge)
                                  and ep[4] >= timestamp]
                ip_address = network_ip_dict[n]
                if ip_address is not None:
                    v.endpointlist.append([ip_address,
                              ovsdb_vhost,
                              system_id,
                              bridge,
                              None if refreshinterval is None else
                                    refreshinterval * 1000000 * 2 + timestamp
                              ])
        written_values = {}
        if all_tun_ports2:
            for k,v,vxkey,vxinfo in zip(keys[len(network_list):len(network_list) + len(all_tun_ports2)],
                           values[len(network_list):len(network_list) + len(all_tun_ports2)],
                           keys[len(network_list) + len(all_tun_ports2):len(network_list) + 2 * len(all_tun_ports2)],
                           values[len(network_list) + len(all_tun_ports2):len(network_list) + 2 * len(all_tun_ports2)]):
                if v is None:
                    if vxinfo is not None:
                        # The port is deleted? Then we should also delete the vxinfo
                        written_values[vxkey] = None
                else:
                    if v.id in created_ports:
                        if vxinfo is None:
                            vxinfo = LogicalPortVXLANInfo.create_from_key(vxkey)
                        # There maybe more than one endpoint at the same time (on migrating)
                        # so we keep all possible endpoints, but move our endpoint to the first place
                        myendpoint = {'vhost': ovsdb_vhost,
                                      'systemid': system_id,
                                      'bridge': bridge,
                                      'tunnel_dst': created_ports[v.id],
                                      'updated_time': timestamp}
                        vxinfo.endpoints = [ep for ep in vxinfo.endpoints
                                            if ep['updated_time'] + allowedmigrationtime * 1000000 >= timestamp
                                            and (ep['vhost'], ep['systemid'], ep['bridge']) != (ovsdb_vhost, system_id, bridge)]
                        vxinfo.endpoints = [myendpoint] + vxinfo.endpoints
                        written_values[vxkey] = vxinfo
                    elif v.id in removed_ports:
                        if vxinfo is not None:
                            # Remove endpoint
                            vxinfo.endpoints = [ep for ep in vxinfo.endpoints
                                                if (ep['vhost'], ep['systemid'], ep['bridge']) != (ovsdb_vhost, system_id, bridge)]
                            if not vxinfo.endpoints:
                                written_values[vxkey] = None
                            else:
                                written_values[vxkey] = vxinfo
        written_values_list = tuple(written_values.items())
        return (tuple(itertools.chain(keys[:len(network_list)], (k for k,_ in written_values_list))),
                tuple(itertools.chain(values[:len(network_list)], (v for _,v in written_values_list))))
    for m in callAPI(container, 'objectdb', 'transact', {'keys': tuple(vxlanendpoint_list + [LogicalPort.default_key(p) for p in all_tun_ports2] +
                                                                  [LogicalPortVXLANInfo.default_key(p) for p in all_tun_ports2]),
                                                    'updater': update_vxlanendpoints,
                                                    'withtime': True
                                                    }):
        yield m

def _get_ip(ip):
    try:
        return ip4_addr(ip)
    except Exception:
        return None

def get_broadcast_ips(vxlan_endpointset, local_ip, ovsdb_vhost, system_id, bridge):
    '''
    Get all IP addresses that are not local
    
    :param vxlan_endpointset: a VXLANEndpointSet object
    
    :param local_ips: list of local IP address to exclude with
    
    :param ovsdb_vhost: identifier, vhost
    
    :param system_id: identifier, system-id
    
    :param bridge: identifier, bridge name
    
    :return: `[(ip, ipnum)]` list where IPs are the original string of the IP address, and ipnum
             are 32-bit numeric IPv4 address.
    '''
    localip_addr = _get_ip(local_ip)
    allips = [(ip, ipnum) for ip, ipnum in ((ep[0], _get_ip(ep[0])) for ep in vxlan_endpointset.endpointlist
              if (ep[1], ep[2], ep[3]) != (ovsdb_vhost, system_id, bridge))
              if ipnum is not None and ipnum != localip_addr]
    return allips
