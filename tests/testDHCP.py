'''
Created on 2016/6/23

:author: hubo
'''
import unittest
from vlcp.utils.dhcp import *
from namedstruct import dump
class Test(unittest.TestCase):

    def testOptions(self):
        p1 = dhcp_payload()
        build_options(p1, [dhcp_option_message_type(value = DHCPOFFER),
                           dhcp_option_address(tag = OPTION_SERVER_IDENTIFIER, value = ip4_addr('10.101.1.1')),
                           dhcp_option_data(tag = OPTION_MESSAGE, value = b'01234' * 20)])
        p2 = dhcp_payload.create(p1._tobytes())
        self.assertLessEqual(len(p1), 576)
        self.assertEqual([(o.tag, o.value) for o in reassemble_options(p2)],
                         [(o.tag, o.value) for o in
                          [dhcp_option_message_type(value = DHCPOFFER),
                           dhcp_option_address(tag = OPTION_SERVER_IDENTIFIER, value = ip4_addr('10.101.1.1')),
                           dhcp_option_data(tag = OPTION_MESSAGE, value = b'01234' * 20)]])
        p1 = dhcp_payload()
        build_options(p1, [dhcp_option_message_type(value = DHCPOFFER),
                           dhcp_option_address(tag = OPTION_SERVER_IDENTIFIER, value = ip4_addr('10.101.1.1')),
                           dhcp_option_data(tag = OPTION_MESSAGE, value = b'01234' * 60)])
        p2 = dhcp_payload.create(p1._tobytes())
        self.assertLessEqual(len(p1), 576)
        self.assertEqual([(o.tag, o.value) for o in reassemble_options(p2)],
                         [(o.tag, o.value) for o in
                          [dhcp_option_message_type(value = DHCPOFFER),
                           dhcp_option_address(tag = OPTION_SERVER_IDENTIFIER, value = ip4_addr('10.101.1.1')),
                           dhcp_option_data(tag = OPTION_MESSAGE, value = b'01234' * 60)]])
        p1 = dhcp_payload()
        build_options(p1, [dhcp_option_message_type(value = DHCPOFFER),
                           dhcp_option_address(tag = OPTION_SERVER_IDENTIFIER, value = ip4_addr('10.101.1.1')),
                           dhcp_option_data(tag = OPTION_MESSAGE, value = b'01234' * 80)])
        p2 = dhcp_payload.create(p1._tobytes())
        self.assertEqual(len(p1), 576)
        self.assertEqual([(o.tag, o.value) for o in reassemble_options(p2)],
                         [(o.tag, o.value) for o in
                          [dhcp_option_overload(value = OVERLOAD_FILE),
                           dhcp_option_message_type(value = DHCPOFFER),
                           dhcp_option_address(tag = OPTION_SERVER_IDENTIFIER, value = ip4_addr('10.101.1.1')),
                           dhcp_option_data(tag = OPTION_MESSAGE, value = b'01234' * 80)]])
        p1 = dhcp_payload()
        build_options(p1, [dhcp_option_message_type(value = DHCPOFFER),
                           dhcp_option_address(tag = OPTION_SERVER_IDENTIFIER, value = ip4_addr('10.101.1.1')),
                           dhcp_option_data(tag = OPTION_MESSAGE, value = b'01234' * 100)])
        p2 = dhcp_payload.create(p1._tobytes())
        self.assertEqual(len(p1), 576)
        self.assertEqual([(o.tag, o.value) for o in reassemble_options(p2)],
                         [(o.tag, o.value) for o in
                          [dhcp_option_overload(value = OVERLOAD_FILE | OVERLOAD_SNAME),
                           dhcp_option_message_type(value = DHCPOFFER),
                           dhcp_option_address(tag = OPTION_SERVER_IDENTIFIER, value = ip4_addr('10.101.1.1')),
                           dhcp_option_data(tag = OPTION_MESSAGE, value = b'01234' * 100)]])
        p1 = dhcp_payload()
        build_options(p1, [dhcp_option_message_type(value = DHCPOFFER),
                           dhcp_option_address(tag = OPTION_SERVER_IDENTIFIER, value = ip4_addr('10.101.1.1')),
                           dhcp_option_data(tag = OPTION_MESSAGE, value = b'01234' * 120)])
        p2 = dhcp_payload.create(p1._tobytes())
        self.assertLessEqual(len(p1), 576)
        self.assertEqual([(o.tag, o.value) for o in reassemble_options(p2)],
                         [(o.tag, o.value) for o in
                          [dhcp_option_message_type(value = DHCPOFFER),
                           dhcp_option_address(tag = OPTION_SERVER_IDENTIFIER, value = ip4_addr('10.101.1.1'))]])
        p1 = dhcp_payload()
        build_options(p1, [dhcp_option_message_type(value = DHCPOFFER),
                           dhcp_option_address(tag = OPTION_SERVER_IDENTIFIER, value = ip4_addr('10.101.1.1')),
                           dhcp_option_data(tag = OPTION_MESSAGE, value = b'01234' * 63),
                           dhcp_option_servers(tag = OPTION_ROUTER, value = [ip4_addr('10.101.1.1'), ip4_addr('10.101.1.2')])])
        p2 = dhcp_payload.create(p1._tobytes())
        self.assertEqual(len(p1), 572)
        self.assertEqual([(o.tag, o.value) for o in reassemble_options(p2)],
                         [(o.tag, o.value) for o in
                          [dhcp_option_overload(value = OVERLOAD_FILE),
                           dhcp_option_message_type(value = DHCPOFFER),
                           dhcp_option_address(tag = OPTION_SERVER_IDENTIFIER, value = ip4_addr('10.101.1.1')),
                           dhcp_option_data(tag = OPTION_MESSAGE, value = b'01234' * 63),
                           dhcp_option_servers(tag = OPTION_ROUTER, value = [ip4_addr('10.101.1.1'), ip4_addr('10.101.1.2')])]])

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()