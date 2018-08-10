'''
Created on 2018/7/13

:author: hubo
'''
from pychecktype import extra, TypeMismatchException
from functools import partial, wraps
from vlcp.utils.netutils import format_ip_address, format_network_cidr
from vlcp.utils.ethernet import mac_addr


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

# Auto convert to number when calling from Web API
def _convert_to_int(s):
    try:
        return int(s)
    except ValueError:
        raise TypeMismatchException(s, autoint, "not an integer")

str_int = extra()
str_int.bind(str, convert=_type_assert(int, str_int, "not an integer"))

autoint = (int, str_int)
