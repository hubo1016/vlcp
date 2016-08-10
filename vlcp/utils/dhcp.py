'''
Created on 2016/6/22

:author: hubo
'''


from namedstruct import *
from vlcp.utils.ethernet import ip4_addr, ip4_addr_bytes, mac_addr_bytes
from vlcp.utils.netutils import parse_ip4_network

boot_op = enum('boot_op', globals(), uint8,
               BOOTREQUEST = 1,
               BOOTREPLY = 2)

BOOTP_MAGIC_COOKIE = 0x63825363

dhcp_tag = enum('dhcp_tag', globals(), uint8,
                OPTION_PAD = 0,
                OPTION_END = 255,
                OPTION_NETMASK = 1,
                OPTION_TIME_OFFSET = 2,
                OPTION_ROUTER = 3,
                OPTION_DNSSERVER = 6,
                OPTION_RLSERVER = 11,
                OPTION_HOSTNAME = 12,
                # Ignore many other options...
                OPTION_DOMAINNAME = 15,
                OPTION_MTU = 26,
                OPTION_BROADCAST = 28,
                OPTION_NTPSERVER = 42,
                OPTION_REQUESTED_IP = 50,
                OPTION_LEASE_TIME = 51,
                OPTION_OVERLOAD = 52,
                OPTION_MESSAGE_TYPE = 53,
                OPTION_SERVER_IDENTIFIER = 54,
                OPTION_REQUESTED_OPTIONS = 55,
                OPTION_MESSAGE = 56,
                OPTION_MAX_MESSAGE_SIZE = 57,
                OPTION_T1 = 58,
                OPTION_T2 = 59,
                OPTION_CLIENT_IDENTIFIER = 61,
                OPTION_CLASSLESSROUTE = 121
                )

dhcp_message_type = enum('dhcp_message_type', globals(), uint8,
                        DHCPDISCOVER           = 1,
                        DHCPOFFER              = 2,
                        DHCPREQUEST            = 3,
                        DHCPDECLINE            = 4,
                        DHCPACK                = 5,
                        DHCPNAK                = 6,
                        DHCPRELEASE            = 7,
                        DHCPINFORM             = 8
                        )

dhcp_overload = enum('dhcp_overload', globals(), uint8, True,
                     OVERLOAD_FILE = 1,
                     OVERLOAD_SNAME = 2)

class _EmptyOptionException(Exception):
    pass

def _no_empty(l):
    if not l:
        raise _EmptyOptionException()
    return l

def _prepack_dhcp_option(x):
    if x.tag != OPTION_PAD and x.tag != OPTION_END:
        x.length = 0
        x.length = x._realsize() - 2
        if x.length > 255:
            x.length = 255
    else:
        if hasattr(x, 'length'):
            delattr(x, 'length')

dhcp_option = nstruct((dhcp_tag, 'tag'),
                      (optional(uint8, 'length', lambda x: x.tag != OPTION_PAD and x.tag != OPTION_END),),
                      name = 'dhcp_option',
                      size = lambda x: getattr(x, 'length', -1) + 2,
                      prepack = _prepack_dhcp_option,
                      padding = 1
                      )

dhcp_option_single_byte = nstruct(name = 'dhcp_option_single_byte',
                                  base = dhcp_option,
                                  criteria = lambda x: x.tag in (OPTION_PAD, OPTION_END),
                                  init = packvalue(OPTION_PAD, 'tag'))

dhcp_option_address = nstruct((ip4_addr, 'value'),
                                 name = 'dhcp_option_address',
                                 base = dhcp_option,
                                 criteria = lambda x: x.tag in (OPTION_NETMASK, OPTION_REQUESTED_IP, OPTION_SERVER_IDENTIFIER, OPTION_BROADCAST),
                                 init = packvalue(OPTION_NETMASK, 'tag'))

dhcp_option_address._parse_from_value = lambda x: ip4_addr(x)

dhcp_time_value = enum('dhcp_time_value', globals(), uint32,
                       DHCPTIME_INFINITE = 0xffffffff
                  )

dhcp_option_time = nstruct((dhcp_time_value, 'value'),
                             name = 'dhcp_option_time',
                             base = dhcp_option,
                             criteria = lambda x: x.tag in (OPTION_TIME_OFFSET, OPTION_LEASE_TIME, OPTION_T1, OPTION_T2),
                             init = packvalue(OPTION_LEASE_TIME, 'tag')
                             )

dhcp_option_time._parse_from_value = lambda x: int(x) if x is not None and x != 'infinity' else DHCPTIME_INFINITE

dhcp_option_servers = nstruct((ip4_addr[0], 'value'),
                              name = 'dhcp_option_servers',
                              base = dhcp_option,
                              criteria = lambda x: OPTION_ROUTER <= x.tag <= OPTION_RLSERVER or x.tag in (OPTION_NTPSERVER,),
                              init = packvalue(OPTION_ROUTER, 'tag')
                              )

try:
    unicode
except:
    unicode = str

dhcp_option_servers._parse_from_value = lambda x: _no_empty([ip4_addr(x)] if isinstance(x, (str, unicode)) else [ip4_addr(v) for v in x])

dhcp_option_data = nstruct((raw, 'value'),
                           name = 'dhcp_option_name',
                           base = dhcp_option,
                           criteria = lambda x: x.tag in (OPTION_HOSTNAME, OPTION_DOMAINNAME, OPTION_MESSAGE, OPTION_CLIENT_IDENTIFIER),
                           init = packvalue(OPTION_HOSTNAME, 'tag'))


def _tobytes(s):
    if isinstance(s, bytes):
        return s
    elif isinstance(s, str):
        return s.decode('ascii')
    elif hasattr(s, '__iter__'):
        return create_binary(s, len(s))
    else:
        raise ValueError('Unsupported type: ')


dhcp_option_data._parse_from_value = _tobytes

dhcp_option_message_type = nstruct((dhcp_message_type, 'value'),
                                   name = 'dhcp_option_message_type',
                                   base = dhcp_option,
                                   criteria = lambda x: x.tag in (OPTION_MESSAGE_TYPE,),
                                   init = packvalue(OPTION_MESSAGE_TYPE, 'tag'))

dhcp_option_uint16 = nstruct((uint16, 'value'),
                          name = 'dhcp_option_uint16',
                          base = dhcp_option,
                          criteria = lambda x: x.tag in (OPTION_MTU, OPTION_MAX_MESSAGE_SIZE),
                          init = packvalue(OPTION_MTU, 'tag'))

dhcp_option_uint16._parse_from_value = lambda x: int(x)

dhcp_option_requested_options = nstruct((dhcp_tag[0], 'value'),
                                        name = 'dhcp_option_requested_options',
                                        base = dhcp_option,
                                        criteria = lambda x: x.tag == OPTION_REQUESTED_OPTIONS,
                                        init = packvalue(OPTION_REQUESTED_OPTIONS, 'tag'))

dhcp_option_overload = nstruct((dhcp_overload, 'value'),
                               name = 'dhcp_option_overload',
                               base = dhcp_option,
                               criteria = lambda x: x.tag == OPTION_OVERLOAD,
                               init = packvalue(OPTION_OVERLOAD, 'tag'))

def _prepack_destination(x):
    x.subnet = x.subnet[:(x.mask + 7) // 8]

def _destination_formatter(x):
    x['_cidr'] = ip4_addr_bytes.formatter(x.subnet) + '/' + str(x.mask)
    return x

_route_destination = nstruct((uint8, 'mask'),
                            (raw, 'subnet'),
                            name = '_route_destination',
                            padding = 1,
                            size = lambda x: (x.mask + 7) // 8 + 1,
                            prepack = _prepack_destination,
                            formatter = _destination_formatter
                            )

dhcp_route = nstruct((_route_destination,),
                     (ip4_addr, 'router'),
                     name = 'dhcp_route',
                     padding = 1
                     )

dhcp_classless_routes = nstruct((dhcp_route[0], 'value'),
                                name = 'dhcp_classless_routes',
                                base = dhcp_option,
                                criteria = lambda x: x.tag == OPTION_CLASSLESSROUTE,
                                init = packvalue(OPTION_CLASSLESSROUTE, 'tag'))

def _create_dhcp_route(cidr, router):
    network, mask = parse_ip4_network(cidr)
    return dhcp_route(subnet = ip4_addr.tobytes(network), mask = mask, router = ip4_addr(router))

dhcp_classless_routes._parse_from_value = lambda x: _no_empty([_create_dhcp_route(cidr, router) for cidr, router in x])

# According to RFC, options may be split into multiple parts
dhcp_option_partial = nstruct((dhcp_tag, 'tag'),
                      (optional(uint8, 'length', lambda x: x.tag != OPTION_PAD and x.tag != OPTION_END),),
                      (raw, 'data'),
                      name = 'dhcp_option_partial',
                      size = lambda x: getattr(x, 'length', -1) + 2,
                      prepack = _prepack_dhcp_option,
                      padding = 1
                      )

dhcp_flags = enum('dhcp_flags', globals(), uint16, True,
                  DHCPFLAG_BROADCAST = 1)

dhcp_payload = nstruct((boot_op, 'op'),
                       (uint8, 'htype'),
                       (uint8, 'hlen'),
                       (uint8, 'hops'),
                       (uint32, 'xid'),
                       (uint16, 'secs'),
                       (dhcp_flags, 'flags'),
                       (ip4_addr, 'ciaddr'),
                       (ip4_addr, 'yiaddr'),
                       (ip4_addr, 'siaddr'),
                       (ip4_addr, 'giaddr'),
                       (char[16], 'chaddr'),
                       (char[64], 'sname'),
                       (char[128], 'file'),
                       (uint32, 'magic_cookie'),    # 0x63825363
                       (dhcp_option_partial[0], 'options'),
                       name = 'dhcp_payload',
                       padding = 1,
                       extend = {'chaddr': mac_addr_bytes}
                       )

def reassemble_options(payload):
    '''
    Reassemble partial options to options, returns a list of dhcp_option
    '''
    options = []
    option_indices = {}
    def process_option_list(partials):
        for p in partials:
            if p.tag == OPTION_END:
                break
            if p.tag == OPTION_PAD:
                continue
            if p.tag in option_indices:
                # Reassemble the data
                options[option_indices[p.tag]][1].append(p.data)
            else:
                options.append((p.tag, [p.data]))
                option_indices[p.tag] = len(options) - 1
    # First process options field
    process_option_list(payload.options)
    if OPTION_OVERLOAD in option_indices:
        # There is an overload option
        data = b''.join(options[option_indices[OPTION_OVERLOAD]][1])
        overload_option = dhcp_overload.create(data)
        if overload_option & OVERLOAD_FILE:
            process_option_list(dhcp_option_partial[0].create(payload.file))
        if overload_option & OVERLOAD_SNAME:
            process_option_list(dhcp_option_partial[0].create(payload.sname))
    def _create_dhcp_option(tag, data):
        opt = dhcp_option(tag = tag)
        opt._setextra(data)
        opt._autosubclass()
        return opt
    return [_create_dhcp_option(tag, b''.join(data)) for tag,data in options]

def build_options(payload, options, maxsize = 576, overload = OVERLOAD_FILE | OVERLOAD_SNAME):
    '''
    Split a list of options 
    '''
    if maxsize < 576:
        maxsize = 576
    max_options_size = maxsize - 240
    options = [o for o in options if o.tag not in (OPTION_PAD, OPTION_END)]
    option_data = [(o.tag, o._tobytes()[2:]) for o in options]
    def split_options(option_data, limits):
        partial_options = []
        buffers = [0]
        if not options:
            return ([], True)
        def create_result():
            # Remove any unfinished partial options
            while partial_options and partial_options[-1][1]:
                partial_options.pop()
            buffers.append(len(partial_options))
            r = [[po for po,_ in partial_options[buffers[i]:buffers[i+1]]] for i in range(0, len(buffers) - 1)]
            while r and not r[-1]:
                r.pop()
            return r
        current_size = 0
        limit_iter = iter(limits)
        try:
            next_limit = next(limit_iter)
        except StopIteration:
            return ([], False)
        except GeneratorExit:
            return ([], False)
        for tag, data in option_data:
            data_size = 0
            nosplit = (len(data) <= 32)
            while True:
                next_size = min(next_limit - current_size - 2, 255, len(data) - data_size)
                if next_size < 0 or (next_size == 0 and data_size < len(data)) \
                        or (next_size < len(data) - data_size and nosplit):
                    try:
                        next_limit = next(limit_iter)
                    except StopIteration:
                        return (create_result(), False)
                    except GeneratorExit:
                        return (create_result(), False)
                    buffers.append(len(partial_options))
                    current_size = 0
                else:
                    partial_options.append((dhcp_option_partial(tag = tag, data = data[data_size : data_size + next_size]),
                                            (next_size < len(data) - data_size)))
                    data_size += next_size
                    current_size += next_size + 2
                    if data_size >= len(data):
                        break
        return (create_result(), True)
    # First try to fit all options in options field
    result, finished = split_options(option_data, [max_options_size - 1])
    if not finished:
        if overload & (OVERLOAD_FILE | OVERLOAD_SNAME):
            limits = [max_options_size - 4]
            if overload & OVERLOAD_FILE:
                limits.append(127)
            if overload & OVERLOAD_SNAME:
                limits.append(63)
            result, finished = split_options(option_data, limits)
    if not result:
        return
    elif len(result) <= 1:
        payload.options = result[0] + [dhcp_option_partial(tag = OPTION_END)]
    else:
        overload_option = 0
        if len(result) >= 2 and result[1]:
            overload_option |= OVERLOAD_FILE
            payload.file = dhcp_option_partial[0].tobytes(result[1] + [dhcp_option_partial(tag = OPTION_END)])
        if len(result) >= 3 and result[2]:
            overload_option |= OVERLOAD_SNAME
            payload.sname = dhcp_option_partial[0].tobytes(result[2] + [dhcp_option_partial(tag = OPTION_END)])
        payload.options = [dhcp_option_partial(tag = OPTION_OVERLOAD, data = dhcp_overload.tobytes(overload_option))] \
                        + result[0] + [dhcp_option_partial(tag = OPTION_END)]


def create_option_from_value(tag, value):
    dhcp_option.parser()
    fake_opt = dhcp_option(tag = tag)
    for c in dhcp_option.subclasses:
        if c.criteria(fake_opt):
            if hasattr(c, '_parse_from_value'):
                return c(tag = tag, value = c._parse_from_value(value))
            else:
                raise ValueError('Cannot set this DHCP option')
    else:
        fake_opt._setextra(_tobytes(value))
        return fake_opt

def create_dhcp_options(input_dict, ignoreError = False, generateNone = False):
    retdict = {}
    for k,v in dict(input_dict).items():
        try:
            if generateNone and v is None:
                retdict[k] = None
            else:
                try:
                    retdict[k] = create_option_from_value(k, v)
                except _EmptyOptionException:
                    if generateNone:
                        retdict[k] = None
        except Exception:
            if ignoreError:
                continue
            else:
                raise
    return retdict

