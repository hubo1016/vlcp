'''
Created on 2015/7/20/

@author: hubo
'''
from __future__ import print_function
import unittest
from vlcp.protocol.openflow import common, openflow10, openflow13
from vlcp.utils.namedstruct import nstruct, dump
from pprint import pprint

class Test(unittest.TestCase):
    exclude = [common.ofp_error_experimenter_msg, openflow13.ofp_group_desc_stats, openflow13.ofp_oxm_mask, openflow13.ofp_oxm_nomask, openflow13._ofp_oxm_mask_value,
               openflow13.ofp_action_set_field, openflow10.nx_flow_mod_spec, openflow13.nx_flow_mod_spec, openflow10.nx_matches, openflow13.nx_matches]
    def testDefs10(self):
        for k in dir(openflow10):
            attr = getattr(openflow10, k)
            if isinstance(attr, nstruct) and not attr in self.exclude and not k.startswith('nxm_'):
                if not attr.subclasses:
                    self.assertEqual(k, repr(attr), k + ' has different name: ' + repr(attr))
                    print(k, repr(attr))
                    obj = attr.new()
                    s = obj._tobytes()
                    r = attr.parse(s)
                    self.assertTrue(r is not None, repr(attr) + ' failed to parse')
                    obj2, size = r
                    self.assertEqual(size, len(s), repr(attr) + ' failed to parse')
                    self.assertEqual(dump(obj), dump(obj2), repr(attr) + ' changed after parsing')
    def testDefs13(self):
        for k in dir(openflow13):
            attr = getattr(openflow13, k)
            if isinstance(attr, nstruct) and not attr in self.exclude and not k.startswith('ofp_oxm_') and not k.startswith('nxm_'):
                if not attr.subclasses:
                    self.assertEqual(k, repr(attr), k + ' has different name: ' + repr(attr))
                    print(k, repr(attr))
                    obj = attr.new()
                    s = obj._tobytes()
                    r = attr.parse(s)
                    self.assertTrue(r is not None, repr(attr) + ' failed to parse')
                    obj2, size = r
                    self.assertEqual(size, len(s), repr(attr) + ' failed to parse')
                    self.assertEqual(dump(obj), dump(obj2), repr(attr) + ' changed after parsing')
    def testOxm(self):
        fm = openflow13.ofp_flow_mod.new(priority = openflow13.OFP_DEFAULT_PRIORITY, command = openflow13.OFPFC_ADD, buffer_id = openflow13.OFP_NO_BUFFER)
        fm.cookie = 0x67843512
        fm.match = openflow13.ofp_match_oxm.new()
        fm.match.oxm_fields.append(openflow13.create_oxm(openflow13.OXM_OF_ETH_DST, b'\x06\x00\x0c\x15\x45\x99'))
        fm.match.oxm_fields.append(openflow13.create_oxm(openflow13.OXM_OF_ETH_TYPE, common.ETHERTYPE_IP))
        fm.match.oxm_fields.append(openflow13.create_oxm(openflow13.OXM_OF_IP_PROTO, 6))
        fm.match.oxm_fields.append(openflow13.create_oxm(openflow13.OXM_OF_IPV4_SRC_W, [192,168,1,0], [255,255,255,0]))
        apply = openflow13.ofp_instruction_actions.new(type = openflow13.OFPIT_APPLY_ACTIONS)
        apply.actions.append(openflow13.ofp_action_set_field.new(field = openflow13.create_oxm(openflow13.OXM_OF_IPV4_SRC, [202, 102, 0, 37])))
        apply.actions.append(openflow13.ofp_action_set_queue.new(queue_id = 1))
        fm.instructions.append(apply)
        write = openflow13.ofp_instruction_actions.new(type = openflow13.OFPIT_WRITE_ACTIONS)
        write.actions.append(openflow13.ofp_action_output.new(port = 7))
        fm.instructions.append(write)
        goto = openflow13.ofp_instruction_goto_table.new(table_id = 1)
        fm.instructions.append(goto)
        s = fm._tobytes()
        r = common.ofp_msg.parse(s)
        self.assertTrue(r is not None, 'Cannot parse message')
        obj2, size = r
        self.assertEqual(size, len(s), 'Cannot parse message')
        pprint(dump(fm))
        pprint(dump(obj2))
        self.assertEqual(dump(fm), dump(obj2), 'message changed after parsing')

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testDefs']
    unittest.main()