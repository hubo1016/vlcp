'''
Created on 2018/7/13

:author: hubo
'''
from pychecktype import extra, TypeMismatchException
from functools import partial, wraps
from vlcp.utils.netutils import format_ip_address, format_network_cidr, format_mac_mask, protocol_parser
from vlcp.utils.ethernet import mac_addr, icmp_type, ip_protocol


def _type_assert(convert, type_, info = None):
    @wraps(convert)
    def _convert(value):
        try:
            return convert(value)
        except Exception as e:
            if info is None:
                raise TypeMismatchException(value, type_, str(e))
            else:
                raise TypeMismatchException(value, type_, info)
    return _convert


ip_address_type = extra()
ip_address_type.bind(str, convert=_type_assert(format_ip_address, ip_address_type))

cidr_type = extra()
cidr_type.bind(str, convert=_type_assert(partial(format_network_cidr, strict=True), cidr_type))

cidr_nonstrict_type = extra()
cidr_nonstrict_type.bind(str, convert=_type_assert(format_network_cidr, cidr_nonstrict_type))

mac_address_type = extra()
mac_address_type.bind(str, convert=_type_assert(lambda x: mac_addr.formatter(mac_addr(x)),
                                                mac_address_type,
                                                "Invalid MAC address"))

mac_mask_type = extra()
mac_mask_type.bind(str, convert=_type_assert(format_mac_mask, mac_mask_type))

# Auto convert to number when calling from Web API
def _convert_to_int(s):
    try:
        return int(s)
    except ValueError:
        raise TypeMismatchException(s, autoint, "not an integer")

str_int = extra()
str_int.bind(str, convert=_type_assert(int, str_int, "not an integer"))

autoint = (int, str_int)

ip_keys=["sport", "dport", "icmp_code", "icmp_type"]
def _check_acl(data):
    """
    check acl element relay
    :param data:
    :return:bool
    """
    if 'protocol' in data:
        protocol, _ = protocol_parser(data["protocol"])
        if "sport" in data:
            if protocol not in [ip_protocol.IPPROTO_TCP, ip_protocol.IPPROTO_UDP]:
                return False
        if "dport" in data:
            if protocol not in [ip_protocol.IPPROTO_TCP, ip_protocol.IPPROTO_UDP]:
                return False
        if "icmp_code" in data:
            if protocol != ip_protocol.IPPROTO_ICMP:
                return False
        if "icmp_type" in data:
            if protocol != ip_protocol.IPPROTO_ICMP:
                return False
    elif len(set(ip_keys).intersection(set(data))):
        return False
    return True

acl_type = extra()
acl_type.bind({"?priority": int,
            "?src_mac": mac_mask_type,
            "?dst_mac": mac_mask_type,
            "?src_ip": cidr_nonstrict_type,
            "?dst_ip": cidr_nonstrict_type,
            "?protocol": (int, extra(str, lambda x: x in ["tcp", "udp", "icmp"])),
            "?sport": extra(int, lambda x: 1 <= x <= 65535),
            "?dport": extra(int, lambda x: 1 <= x <= 65535),
            "?icmp_code": int,
            "?icmp_type": extra(int, lambda x: x in [icmp_type.ICMP_ECHOREPLY, icmp_type.ICMP_DEST_UNREACH, icmp_type.ICMP_ECHO]),
            "accept": bool},
            check=_check_acl)
acl_list_type = [acl_type]
