'''
Created on 2016/7/26

:author: think
'''
from vlcp.utils.ethernet import ip4_addr, mac_addr, ip_protocol

ip_protocols = {
    'ip': ip_protocol.IPPROTO_IP,
    'icmp': ip_protocol.IPPROTO_ICMP,
    'tcp': ip_protocol.IPPROTO_TCP,
    'udp': ip_protocol.IPPROTO_UDP
}

def parse_ip4_network( network ):

    ip,f,prefix = network.partition('/')
    if not f or not prefix:
        raise ValueError('invalid cidr ' + network)
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
    return network | ((1 << (32 - prefix)) - 1)

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

def format_network_cidr(cidr, strict=False):
    ip,f,prefix = cidr.partition('/')

    try:
        ip = check_ip_address(ip)
    except Exception:
        raise ValueError("Invalid CIDR " + cidr)
    if f and prefix:
        try:
            prefix = int(prefix)
            assert 0 <= prefix<= 32
        except Exception:
            raise ValueError("Invalid CIDR " + cidr)
        else:
            netmask = get_netmask(prefix)
            ip = ip & netmask
            return ip4_addr.formatter(ip) + "/" + str(prefix)
    else:
        if strict:
            raise ValueError("Invalid CIDR " + cidr)
        return ip4_addr.formatter(ip) + "/32"

def format_mac_mask(mac_str):
    """
    format mac/mask
    :param mac_str:
    :return:str
    """
    v, p, m = mac_str.partition('/')
    try:
        check_mac_address(v)
    except Exception:
        raise ValueError("Invalid MAC" + mac_str)
    if p and m:
        return mac_str
    else:
        return v + "/" + "ff:ff:ff:ff:ff:ff"

def check_ip_address(ipaddress):
    try:
        ip = ip4_addr(ipaddress)
    except Exception:
        raise ValueError("Invalid IP address " + ipaddress)
    return ip

def check_mac_address(macaddress):
    """
    check mac address valid
    :param macaddress:
    :return:uint8
    """
    try:
        mac = mac_addr(macaddress)
    except Exception:
        raise ValueError("Invalid MAC address " + mac)
    return mac

def format_ip_address(ipaddress):
    return ip4_addr.formatter(check_ip_address(ipaddress))

def mac_parser(mac_str):
    """
    parse MAC in ACL

    :param mac_str: input string
    :return: (value, mask) tuple
    """
    v, p, m = mac_str.partition('/')
    if p:
        value = mac_addr(v)
        mask = mac_addr(m)
    else:
        value = mac_addr(v)
        mask = None
    return value, mask

def ip4_parser(ip4):
    """
    parse MAC in ACL
    :param ip4:
    :return:(value, mask) tuple
    """
    ip,f,prefix = ip4.partition('/')
    value = ip4_addr(ip)
    if not f:
        net_mask = None
    else:
        mask = int(prefix)
        if not 0 <= mask <= 32:
            raise ValueError("invalid prefix " + prefix)
        net_mask = get_netmask(mask)

    return value, net_mask

def protocol_parser(protocol):
    """
    parse Protocol in ACL
    :param protocol:
    :return:(value,mask) tuple
    """
    if isinstance(protocol, int):
        return protocol, None
    else:
        return ip_protocols[protocol], None

def protocol_sport_parser(p_type):
    """
    parse sport in ACL
    :param p_type:
    :return:int
    """
    if p_type == ip_protocol.IPPROTO_TCP:
        return 'OXM_OF_TCP_SRC'
    elif p_type == ip_protocol.IPPROTO_UDP:
        return 'OXM_OF_UDP_SRC'
def protocol_dport_parser(p_type):
    """
        parse dport in ACL
        :param p_type:
        :return:int
        """
    if p_type == ip_protocol.IPPROTO_TCP:
        return 'OXM_OF_TCP_DST'
    elif p_type == ip_protocol.IPPROTO_UDP:
        return 'OXM_OF_UDP_DST'

def icmp_parser(value):
    """
    :param value:
    :return:(value,None) tuple
    """
    return value, None


