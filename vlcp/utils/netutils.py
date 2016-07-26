'''
Created on 2016/7/26

:author: think
'''
from vlcp.utils.ethernet import ip4_addr

def check_ip_pool(gateway, start, end, allocated, cidr):

    nstart = parse_ip4_address(start)
    nend = parse_ip4_address(end)
    ncidr,prefix = parse_ip4_network(cidr)
    if gateway:
        ngateway = parse_ip4_address(gateway)
        assert ip_in_network(ngateway,ncidr,prefix)
        assert ip_in_network(nstart,ncidr,prefix)
        assert ip_in_network(nend,ncidr,prefix)
        assert nstart < nend
        assert ngateway < nstart or ngateway > nend

        for ip in allocated:
            nip = parse_ip4_address(ip)
            assert nstart < nip < nend
    else:
        assert ip_in_network(nstart,ncidr,prefix)
        assert ip_in_network(nend,ncidr,prefix)
        assert nstart < nend

        for ip in allocated:
            nip = parse_ip4_address(ip)
            assert nstart < nip < nend

def parse_ip4_network( network ):

    ip,f,prefix = network.rpartition('/')
    if not f:
        raise ValueError('invalid cidr ' + prefix)
    if not 0 <= int(prefix) <= 32:
        raise ValueError("invalid prefix " + prefix)

    netmask = get_netmask(prefix)

    value = ip4_addr(ip)
    return value & netmask,int(prefix)

def get_netmask(prefix):
    return (0xffffffff) >> (32 - int(prefix)) << (32 - int(prefix))

def get_network(ip, prefix):
    return ip & get_netmask(prefix)

def get_broadcast(network, prefix):
    return network | get_netmask(prefix)

def parse_ip4_address(address):
    return ip4_addr(address)

def ip_in_network(ip,network,prefix):
    shift = 32 - prefix
    return (ip >> shift) == (network >> shift)

def network_first(network,prefix):
    return network + 1

def network_last(network,prefix):
    hostmask = (1 << (32 - prefix)) - 1
    # calc cidr last avaliable ip , so inc 1
    return (network | hostmask) - 1
