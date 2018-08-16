'''
Created on 2016/5/17

:author: think
'''
import unittest
from vlcp.utils.ethernet import *
import vlcp.utils.ethernet as ethernet

class Test(unittest.TestCase):
    exclude = [ip4_icmp_fragment_payload, ip4_icmp_partial_payload, ip4_icmp_payload,
               ip4_partial_payload_fragment, ip4_payload_fragment, ip4_tcp_fragment_payload,
               ip4_tcp_partial_payload, ip4_udp_fragment_payload, tcp_bestparse_partial]
    def testTcpPayload(self):
        mypacket = ip4_tcp_payload(ip_src = ip4_addr('192.168.5.12'),
                                      ip_dst = ip4_addr('192.168.6.11'),
                                      tos = IPTOS_ECN_ECT1 | IPTOS_DSCP_AF21,
                                      identifier = 0x1234,
                                      ttl = 128,
                                      options = b'\x01\x01\x01\x01\x00\x00\x00\x00',
                                      sport = 32188,
                                      dport = 80,
                                      seq = 0x19237812,
                                      ack = 0x09a40178,
                                      tcp_flags = TH_FIN | TH_ACK,
                                      tcp_win = 65535,
                                      data = b'GET / HTTP/1.0\r\nHost: 192.168.6.11\r\n\r\n')
        mypacket_bytes = mypacket._tobytes()
        self.assertEqual(mypacket_bytes, b'GI\x00V\x124\x00\x00\x80\x06\x97\xbb\xc0\xa8\x05\x0c\xc0\xa8\x06\x0b\x01\x01\x01\x01\x00\x00\x00\x00}\xbc\x00P\x19#x\x12\t\xa4\x01xP\x11\xff\xff\xf3\x0e\x00\x00GET / HTTP/1.0\r\nHost: 192.168.6.11\r\n\r\n')
        mypacket2 = ip4_payload.create(mypacket_bytes)
        self.assertEqual(dump(mypacket), dump(mypacket2))
        mypacket3 = ip4_partial_payload.create(mypacket_bytes)
        self.assertEqual(dump(mypacket, typeinfo = DUMPTYPE_NONE), dump(mypacket3, typeinfo = DUMPTYPE_NONE))
        mypacket4 = ip4_partial_payload.create(mypacket_bytes[:50])
        self.assertEqual(dict((k,v) for k,v in dump(mypacket, typeinfo = DUMPTYPE_NONE).items() if k != 'data'),
                         dict((k,v) for k,v in dump(mypacket4, typeinfo = DUMPTYPE_NONE).items() if k != 'data'))
        mypacket5 = ip4_partial_payload.create(mypacket_bytes[:40])
        self.assertEqual(mypacket5.sport, 32188)
        self.assertEqual(mypacket5.dport, 80)
        self.assertEqual(checksum(mypacket2._get_embedded(ip4_header)._tobytes(True)) or 0xffff, 0xffff)
        self.assertEqual(tp4_checksum(mypacket2._get_embedded(tcp_payload), mypacket2.ip_src, mypacket2.ip_dst, IPPROTO_TCP) or 0xffff, 0xffff)
    def testUdpPayload(self):
        mypacket = ip4_udp_payload(ip_src = ip4_addr('192.168.5.12'),
                                  ip_dst = ip4_addr('192.168.6.11'),
                                  tos = IPTOS_DSCP_AF21,
                                  identifier = 0x1234,
                                  ttl = 128,
                                  options = b'\x01\x01\x01\x00',
                                  sport = 32189,
                                  dport = 99,
                                  data = b'Test UDP Datagram')
        mypacket_bytes = mypacket._tobytes()
        self.assertEqual(mypacket_bytes, b'FH\x001\x124\x00\x00\x80\x11\x98\xd7\xc0\xa8\x05\x0c\xc0\xa8\x06\x0b\x01\x01\x01\x00}\xbd\x00c\x00\x19\x063Test UDP Datagram')
        mypacket2 = ip4_payload.create(mypacket_bytes)
        self.assertEqual(dump(mypacket), dump(mypacket2))
        mypacket3 = ip4_partial_payload.create(mypacket_bytes)
        self.assertEqual(dump(mypacket, typeinfo = DUMPTYPE_NONE), dump(mypacket3, typeinfo = DUMPTYPE_NONE))
        mypacket4 = ip4_partial_payload.create(mypacket_bytes[:40])
        self.assertEqual(dict((k,v) for k,v in dump(mypacket, typeinfo = DUMPTYPE_NONE).items() if k != 'data'),
                         dict((k,v) for k,v in dump(mypacket4, typeinfo = DUMPTYPE_NONE).items() if k != 'data'))
        self.assertEqual(checksum(mypacket2._get_embedded(ip4_header)._tobytes(True)) or 0xffff, 0xffff)
        self.assertEqual(tp4_checksum(mypacket2._get_embedded(udp_payload), mypacket2.ip_src, mypacket2.ip_dst, IPPROTO_UDP) or 0xffff, 0xffff)        
    def testIcmpPayload(self):
        mypacket = ip4_icmp_payload((icmp_bestparse, icmp_echo),
                                  ip_src = ip4_addr('192.168.5.12'),
                                  ip_dst = ip4_addr('192.168.6.11'),
                                  tos = IPTOS_DSCP_AF21,
                                  identifier = 0x1234,
                                  ttl = 128,
                                  options = b'\x01\x01\x01\x00',
                                  icmp_id = 0x4000,
                                  icmp_seq = 1,
                                  data = b'Test ICMP Package')
        mypacket_bytes = mypacket._tobytes()
        self.assertEqual(mypacket_bytes, b'FH\x001\x124\x00\x00\x80\x01\x98\xe7\xc0\xa8\x05\x0c\xc0\xa8\x06\x0b\x01\x01\x01\x00\x08\x00\xc29@\x00\x00\x01Test ICMP Package')
        mypacket2 = ip4_payload.create(mypacket_bytes)
        self.assertEqual(dump(mypacket), dump(mypacket2))
        mypacket3 = ip4_partial_payload.create(mypacket_bytes)
        self.assertEqual(dump(mypacket, typeinfo = DUMPTYPE_NONE), dump(mypacket3, typeinfo = DUMPTYPE_NONE))
        mypacket4 = ip4_partial_payload.create(mypacket_bytes[:40])
        self.assertEqual(dict((k,v) for k,v in dump(mypacket, typeinfo = DUMPTYPE_NONE).items() if k != 'data'),
                         dict((k,v) for k,v in dump(mypacket4, typeinfo = DUMPTYPE_NONE).items() if k != 'data'))
        mypacket5 = ip4_partial_payload.create(mypacket_bytes[:32])
        self.assertEqual(mypacket5.icmp_type, ICMP_ECHO)
        self.assertEqual(mypacket5.icmp_code, 0)
        self.assertEqual(checksum(mypacket2._get_embedded(ip4_header)._tobytes(True)) or 0xffff, 0xffff)
        self.assertEqual(checksum(mypacket2._get_embedded(icmp_bestparse)._tobytes(True)) or 0xffff, 0xffff)        
    def testEthernet(self):
        mypacket = ip4_packet_l7((ip4_payload, ip4_tcp_payload),
                                  dl_src = mac_addr('02:00:11:38:0a:19'),
                                  dl_dst = mac_addr('06:00:99:ff:01:07'),
                                  ip_src = ip4_addr('192.168.5.12'),
                                  ip_dst = ip4_addr('192.168.6.11'),
                                  tos = IPTOS_ECN_ECT1 | IPTOS_DSCP_AF21,
                                  identifier = 0x1234,
                                  ttl = 128,
                                  options = b'\x01\x01\x01\x01\x00\x00\x00\x00',
                                  sport = 32188,
                                  dport = 80,
                                  seq = 0x19237812,
                                  ack = 0x09a40178,
                                  tcp_flags = TH_FIN | TH_ACK,
                                  tcp_win = 65535,
                                  data = b'GET / HTTP/1.0\r\nHost: 192.168.6.11\r\n\r\n')
        mypacket_bytes = mypacket._tobytes()
        self.assertEqual(mypacket_bytes, b'\x06\x00\x99\xff\x01\x07\x02\x00\x118\n\x19\x08\x00GI\x00V\x124\x00\x00\x80\x06\x97\xbb\xc0\xa8\x05\x0c\xc0\xa8\x06\x0b\x01\x01\x01\x01\x00\x00\x00\x00}\xbc\x00P\x19#x\x12\t\xa4\x01xP\x11\xff\xff\xf3\x0e\x00\x00GET / HTTP/1.0\r\nHost: 192.168.6.11\r\n\r\n')
        mypacket2 = ethernet_l7.create(mypacket_bytes)
        self.assertEqual(dump(mypacket), dump(mypacket2))
        mypacket3 = ethernet_l4.create(mypacket_bytes)
        self.assertEqual(dump(mypacket, typeinfo = DUMPTYPE_NONE), dump(mypacket3, typeinfo = DUMPTYPE_NONE))
        mypacket4 = ethernet_l4.create(mypacket_bytes[:64])
        self.assertEqual(dict((k,v) for k,v in dump(mypacket, typeinfo = DUMPTYPE_NONE).items() if k != 'data'),
                         dict((k,v) for k,v in dump(mypacket4, typeinfo = DUMPTYPE_NONE).items() if k != 'data'))
        mypacket5 = ethernet_l4.create(mypacket_bytes[:54])
        self.assertEqual(mypacket5.sport, 32188)
        self.assertEqual(mypacket5.dport, 80)
        mypacket_payload = mypacket._get_embedded(ip4_payload)._tobytes()
        self.assertEqual(mypacket_payload, b'GI\x00V\x124\x00\x00\x80\x06\x97\xbb\xc0\xa8\x05\x0c\xc0\xa8\x06\x0b\x01\x01\x01\x01\x00\x00\x00\x00}\xbc\x00P\x19#x\x12\t\xa4\x01xP\x11\xff\xff\xf3\x0e\x00\x00GET / HTTP/1.0\r\nHost: 192.168.6.11\r\n\r\n')
        mypacket6 = ethernet_l3.create(mypacket_bytes[:54])
        self.assertEqual(mypacket6.ip_src, ip4_addr('192.168.5.12'))
        self.assertEqual(mypacket6.ip_dst, ip4_addr('192.168.6.11'))
        mypacket7 = ethernet_l2.create(mypacket_bytes)
        self.assertEqual(mypacket7.dl_src, mac_addr('02:00:11:38:0a:19'))
        self.assertEqual(mypacket7.dl_dst, mac_addr('06:00:99:ff:01:07'))
        mypacket8 = ip4_payload.create(mypacket7._getextra())
        self.assertEqual(mypacket8.ip_src, ip4_addr('192.168.5.12'))
        self.assertEqual(mypacket8.ip_dst, ip4_addr('192.168.6.11'))
    def testEthernet8021q(self):
        mypacket = create_packet(ip4_packet_l7, 101, 0, None, 0,
                                 (ip4_payload, ip4_tcp_payload),
                                  dl_src = mac_addr('02:00:11:38:0a:19'),
                                  dl_dst = mac_addr('06:00:99:ff:01:07'),
                                  ip_src = ip4_addr('192.168.5.12'),
                                  ip_dst = ip4_addr('192.168.6.11'),
                                  tos = IPTOS_ECN_ECT1 | IPTOS_DSCP_AF21,
                                  identifier = 0x1234,
                                  ttl = 128,
                                  options = b'\x01\x01\x01\x01\x00\x00\x00\x00',
                                  sport = 32188,
                                  dport = 80,
                                  seq = 0x19237812,
                                  ack = 0x09a40178,
                                  tcp_flags = TH_FIN | TH_ACK,
                                  tcp_win = 65535,
                                  data = b'GET / HTTP/1.0\r\nHost: 192.168.6.11\r\n\r\n')
        mypacket_bytes = mypacket._tobytes()
        self.assertEqual(mypacket_bytes, b'\x06\x00\x99\xff\x01\x07\x02\x00\x118\n\x19\x81\x00\x10\x65\x08\x00GI\x00V\x124\x00\x00\x80\x06\x97\xbb\xc0\xa8\x05\x0c\xc0\xa8\x06\x0b\x01\x01\x01\x01\x00\x00\x00\x00}\xbc\x00P\x19#x\x12\t\xa4\x01xP\x11\xff\xff\xf3\x0e\x00\x00GET / HTTP/1.0\r\nHost: 192.168.6.11\r\n\r\n')
        mypacket2 = ethernet_l7.create(mypacket_bytes)
        self.assertEqual(dump(mypacket), dump(mypacket2))
        mypacket3 = ethernet_l4.create(mypacket_bytes)
        self.assertEqual(dump(mypacket, typeinfo = DUMPTYPE_NONE), dump(mypacket3, typeinfo = DUMPTYPE_NONE))
        mypacket4 = ethernet_l4.create(mypacket_bytes[:68])
        self.assertEqual(dict((k,v) for k,v in dump(mypacket, typeinfo = DUMPTYPE_NONE).items() if k != 'data'),
                         dict((k,v) for k,v in dump(mypacket4, typeinfo = DUMPTYPE_NONE).items() if k != 'data'))
        mypacket5 = ethernet_l4.create(mypacket_bytes[:58])
        self.assertEqual(mypacket5.sport, 32188)
        self.assertEqual(mypacket5.dport, 80)
        mypacket_payload = mypacket._get_embedded(ip4_payload)._tobytes()
        self.assertEqual(mypacket_payload, b'GI\x00V\x124\x00\x00\x80\x06\x97\xbb\xc0\xa8\x05\x0c\xc0\xa8\x06\x0b\x01\x01\x01\x01\x00\x00\x00\x00}\xbc\x00P\x19#x\x12\t\xa4\x01xP\x11\xff\xff\xf3\x0e\x00\x00GET / HTTP/1.0\r\nHost: 192.168.6.11\r\n\r\n')
        mypacket6 = ethernet_l3.create(mypacket_bytes[:58])
        self.assertEqual(mypacket6.ip_src, ip4_addr('192.168.5.12'))
        self.assertEqual(mypacket6.ip_dst, ip4_addr('192.168.6.11'))
        mypacket7 = ethernet_l2.create(mypacket_bytes)
        self.assertEqual(mypacket7.dl_src, mac_addr('02:00:11:38:0a:19'))
        self.assertEqual(mypacket7.dl_dst, mac_addr('06:00:99:ff:01:07'))
    def testEthernet8021ad(self):
        mypacket = create_packet(ip4_packet_l7, 101, 0, 200, 1,
                                 (ip4_payload, ip4_tcp_payload),
                                  dl_src = mac_addr('02:00:11:38:0a:19'),
                                  dl_dst = mac_addr('06:00:99:ff:01:07'),
                                  ip_src = ip4_addr('192.168.5.12'),
                                  ip_dst = ip4_addr('192.168.6.11'),
                                  tos = IPTOS_ECN_ECT1 | IPTOS_DSCP_AF21,
                                  identifier = 0x1234,
                                  ttl = 128,
                                  options = b'\x01\x01\x01\x01\x00\x00\x00\x00',
                                  sport = 32188,
                                  dport = 80,
                                  seq = 0x19237812,
                                  ack = 0x09a40178,
                                  tcp_flags = TH_FIN | TH_ACK,
                                  tcp_win = 65535,
                                  data = b'GET / HTTP/1.0\r\nHost: 192.168.6.11\r\n\r\n')
        mypacket_bytes = mypacket._tobytes()
        self.assertEqual(mypacket_bytes, b'\x06\x00\x99\xff\x01\x07\x02\x00\x118\n\x19\x88\xa8\x30\xc8\x81\x00\x10\x65\x08\x00GI\x00V\x124\x00\x00\x80\x06\x97\xbb\xc0\xa8\x05\x0c\xc0\xa8\x06\x0b\x01\x01\x01\x01\x00\x00\x00\x00}\xbc\x00P\x19#x\x12\t\xa4\x01xP\x11\xff\xff\xf3\x0e\x00\x00GET / HTTP/1.0\r\nHost: 192.168.6.11\r\n\r\n')
        mypacket2 = ethernet_l7.create(mypacket_bytes)
        self.assertEqual(dump(mypacket), dump(mypacket2))
        mypacket3 = ethernet_l4.create(mypacket_bytes)
        self.assertEqual(dump(mypacket, typeinfo = DUMPTYPE_NONE), dump(mypacket3, typeinfo = DUMPTYPE_NONE))
        mypacket4 = ethernet_l4.create(mypacket_bytes[:72])
        self.assertEqual(dict((k,v) for k,v in dump(mypacket, typeinfo = DUMPTYPE_NONE).items() if k != 'data'),
                         dict((k,v) for k,v in dump(mypacket4, typeinfo = DUMPTYPE_NONE).items() if k != 'data'))
        mypacket5 = ethernet_l4.create(mypacket_bytes[:62])
        self.assertEqual(mypacket5.sport, 32188)
        self.assertEqual(mypacket5.dport, 80)
        mypacket_payload = mypacket._get_embedded(ip4_payload)._tobytes()
        self.assertEqual(mypacket_payload, b'GI\x00V\x124\x00\x00\x80\x06\x97\xbb\xc0\xa8\x05\x0c\xc0\xa8\x06\x0b\x01\x01\x01\x01\x00\x00\x00\x00}\xbc\x00P\x19#x\x12\t\xa4\x01xP\x11\xff\xff\xf3\x0e\x00\x00GET / HTTP/1.0\r\nHost: 192.168.6.11\r\n\r\n')
        mypacket6 = ethernet_l3.create(mypacket_bytes[:62])
        self.assertEqual(mypacket6.ip_src, ip4_addr('192.168.5.12'))
        self.assertEqual(mypacket6.ip_dst, ip4_addr('192.168.6.11'))
        mypacket7 = ethernet_l2.create(mypacket_bytes)
        self.assertEqual(mypacket7.dl_src, mac_addr('02:00:11:38:0a:19'))
        self.assertEqual(mypacket7.dl_dst, mac_addr('06:00:99:ff:01:07'))
    def testEthernetARP(self):
        mypacket = arp_packet_l7(dl_src = mac_addr('02:00:11:38:0a:19'),
                                 dl_dst = mac_addr('ff:ff:ff:ff:ff:ff'),
                                 arp_sha = mac_addr('02:00:11:38:0a:19'),
                                 arp_spa = ip4_addr('192.168.10.51'),
                                 arp_tha = mac_addr('ff:ff:ff:ff:ff:ff'),
                                 arp_tpa = ip4_addr('192.168.10.1'))
        mypacket_bytes = mypacket._tobytes()
        self.assertEqual(mypacket_bytes, b'\xff\xff\xff\xff\xff\xff\x02\x00\x118\n\x19\x08\x06\x00\x01\x08\x00\x06\x04\x00\x00\x02\x00\x118\n\x19\xc0\xa8\n3\xff\xff\xff\xff\xff\xff\xc0\xa8\n\x01')
        mypacket2 = ethernet_l7.create(mypacket_bytes)
        self.assertEqual(dump(mypacket), dump(mypacket2))
        mypacket3 = ethernet_l4.create(mypacket_bytes)
        self.assertEqual(dump(mypacket, typeinfo=DUMPTYPE_NONE), dump(mypacket3, typeinfo=DUMPTYPE_NONE))
        mypacket4 = ethernet_l3.create(mypacket_bytes)
        self.assertEqual(dump(mypacket, typeinfo=DUMPTYPE_NONE), dump(mypacket4, typeinfo=DUMPTYPE_NONE))
        mypacket5 = ethernet_l2.create(mypacket_bytes)
        self.assertEqual(mypacket5.dl_src, mac_addr('02:00:11:38:0a:19'))
        self.assertEqual(mypacket5.dl_dst, mac_addr('ff:ff:ff:ff:ff:ff'))
    def testDefs(self):
        for k in dir(ethernet):
            attr = getattr(ethernet, k)
            if isinstance(attr, nstruct) and not attr in self.exclude and not k.startswith('_'):
                if not attr.subclasses:
                    self.assertEqual(k, repr(attr), k + ' has different name: ' + repr(attr))
                    print(k, repr(attr))
                    obj = attr.new()
                    s = obj._tobytes()
                    r = attr.create(s)
                    self.assertTrue(r is not None, repr(attr) + ' failed to parse')
                    self.assertEqual(dump(obj), dump(r), repr(attr) + ' changed after parsing')
    def testFragments(self):
        mypayload = udp_payload(sport = 39211,
                                dport = 99,
                                data = b'Very large data ' * 200,
                                )
        mypayload.udp_sum = 0
        mypayload.udp_sum = tp4_checksum(mypayload._tobytes(), ip4_addr('192.168.5.12'), ip4_addr('192.168.6.11'), IPPROTO_UDP)
        fragments = create_fragments_ip4(mypayload, ip_src = ip4_addr('192.168.5.12'),
                                                      ip_dst = ip4_addr('192.168.6.11'),
                                                      tos = IPTOS_DSCP_AF21,
                                                      proto = IPPROTO_UDP,
                                                      identifier = 0x1234,
                                                      ttl = 128,
                                                      options = b'\x01\x01\x01\x00',
                                                      fragment_options = b'')
        self.assertEqual(len(fragments), 3)
        self.assertListEqual([f._tobytes() for f in fragments], [
                                b'FH\x05\xd8\x124 \x00\x80\x11s0\xc0\xa8\x05\x0c\xc0\xa8\x06\x0b\x01\x01\x01\x00\x99+\x00c\x0c\x88C[' \
                                        + 91 * b'Very large data ' + b'Very lar',
                                b'EH\x05\xdc\x124 \xb8\x80\x11uu\xc0\xa8\x05\x0c\xc0\xa8\x06\x0b' \
                                        + b'ge data ' + 92 * b'Very large data ',
                                b'EH\x01\x14\x124\x01q\x80\x11\x99\x84\xc0\xa8\x05\x0c\xc0\xa8\x06\x0b' \
                                        + b'Very large data ' * 16
                                                                 ])
        first, second, third = [ip4_payload.create(f._tobytes()) for f in fragments]
        self.assertEqual(first.ip_src, ip4_addr('192.168.5.12'))
        self.assertEqual(first.ip_dst, ip4_addr('192.168.6.11'))
        self.assertEqual(second.ip_src, ip4_addr('192.168.5.12'))
        self.assertEqual(second.ip_dst, ip4_addr('192.168.6.11'))
        self.assertEqual(third.ip_src, ip4_addr('192.168.5.12'))
        self.assertEqual(third.ip_dst, ip4_addr('192.168.6.11'))
        self.assertEqual(first.sport, 39211)
        self.assertEqual(first.dport, 99)
        self.assertEqual(first.data + second.payload + third.payload, b'Very large data ' * 200)
        first2, second2, third2 = [ip4_partial_payload.create(f._tobytes()[:128]) for f in fragments]
        self.assertEqual(first2.ip_src, ip4_addr('192.168.5.12'))
        self.assertEqual(first2.ip_dst, ip4_addr('192.168.6.11'))
        self.assertEqual(second2.ip_src, ip4_addr('192.168.5.12'))
        self.assertEqual(second2.ip_dst, ip4_addr('192.168.6.11'))
        self.assertEqual(third2.ip_src, ip4_addr('192.168.5.12'))
        self.assertEqual(third2.ip_dst, ip4_addr('192.168.6.11'))
        self.assertEqual(first2.sport, 39211)
        self.assertEqual(first2.dport, 99)
    def testEthernetFragments(self):
        mypayload = udp_payload(sport = 39211,
                                dport = 99,
                                data = b'Very large data ' * 200,
                                )
        mypayload.udp_sum = 0
        mypayload.udp_sum = tp4_checksum(mypayload._tobytes(), ip4_addr('192.168.5.12'), ip4_addr('192.168.6.11'), IPPROTO_UDP)
        fragments = create_fragments_ip4_packet(mypayload, dl_src = mac_addr('02:00:11:38:0a:19'),
                                                      dl_dst = mac_addr('06:00:99:ff:01:07'),
                                                      vid = 101,
                                                      ip_src = ip4_addr('192.168.5.12'),
                                                      ip_dst = ip4_addr('192.168.6.11'),
                                                      proto = IPPROTO_UDP,
                                                      tos = IPTOS_DSCP_AF21,
                                                      identifier = 0x1234,
                                                      ttl = 128,
                                                      options = b'\x01\x01\x01\x00',
                                                      fragment_options = b'')
        self.assertEqual(len(fragments), 3)
        self.assertListEqual([f._tobytes() for f in fragments], [
                                b'\x06\x00\x99\xff\x01\x07\x02\x00\x118\n\x19\x81\x00\x10e\x08\x00FH\x05\xd8\x124 \x00\x80\x11s0\xc0\xa8\x05\x0c\xc0\xa8\x06\x0b\x01\x01\x01\x00\x99+\x00c\x0c\x88C[' \
                                        + 91 * b'Very large data ' + b'Very lar',
                                b'\x06\x00\x99\xff\x01\x07\x02\x00\x118\n\x19\x81\x00\x10e\x08\x00EH\x05\xdc\x124 \xb8\x80\x11uu\xc0\xa8\x05\x0c\xc0\xa8\x06\x0b' \
                                        + b'ge data ' + 92 * b'Very large data ',
                                b'\x06\x00\x99\xff\x01\x07\x02\x00\x118\n\x19\x81\x00\x10e\x08\x00EH\x01\x14\x124\x01q\x80\x11\x99\x84\xc0\xa8\x05\x0c\xc0\xa8\x06\x0b' \
                                        + b'Very large data ' * 16
                                                                 ])
        first, second, third = [ethernet_l7.create(f._tobytes()) for f in fragments]
        self.assertEqual(first.ip_src, ip4_addr('192.168.5.12'))
        self.assertEqual(first.ip_dst, ip4_addr('192.168.6.11'))
        self.assertEqual(second.ip_src, ip4_addr('192.168.5.12'))
        self.assertEqual(second.ip_dst, ip4_addr('192.168.6.11'))
        self.assertEqual(third.ip_src, ip4_addr('192.168.5.12'))
        self.assertEqual(third.ip_dst, ip4_addr('192.168.6.11'))
        self.assertEqual(first.sport, 39211)
        self.assertEqual(first.dport, 99)
        self.assertEqual(first.data + second.payload + third.payload, b'Very large data ' * 200)
        first2, second2, third2 = [ethernet_l4.create(f._tobytes()[:128]) for f in fragments]
        self.assertEqual(first2.ip_src, ip4_addr('192.168.5.12'))
        self.assertEqual(first2.ip_dst, ip4_addr('192.168.6.11'))
        self.assertEqual(second2.ip_src, ip4_addr('192.168.5.12'))
        self.assertEqual(second2.ip_dst, ip4_addr('192.168.6.11'))
        self.assertEqual(third2.ip_src, ip4_addr('192.168.5.12'))
        self.assertEqual(third2.ip_dst, ip4_addr('192.168.6.11'))
        self.assertEqual(first2.sport, 39211)
        self.assertEqual(first2.dport, 99)


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()