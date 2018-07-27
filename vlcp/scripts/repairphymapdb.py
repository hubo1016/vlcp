from vlcp.scripts.script import ScriptModule
from vlcp.server.module import depend, call_api
from vlcp.service.kvdb import objectdb
from vlcp.utils.networkmodel import PhysicalNetworkMap, PhysicalNetworkSet
from vlcp.utils.exceptions import WalkKeyNotRetrieved
from contextlib import suppress


@depend(objectdb.ObjectDB)
class RepairPhyMapDB(ScriptModule):
    """
     before version 317b31130794650a9392eb15634a2aefaba35c28
     have problem about physicalmap don't remove weakref(logicalnetwork)
     this script repair this problem in DB
    """
    async def run(self):

        saved_lgnet_keys = []
        saved_phymap_keys = []
        saved_physicalnetworkmap_to_logicalnetwork = {}

        def walk_phynetset(key,value,walk,save):
            del saved_lgnet_keys[:]
            del saved_phymap_keys[:]
            saved_physicalnetworkmap_to_logicalnetwork.clear()
            for weak_phynet in value.set.dataset():
                with suppress(WalkKeyNotRetrieved):
                    phynetobj = walk(weak_phynet.getkey())
                    phynetmapkey = PhysicalNetworkMap.default_key(phynetobj.id)
                    phynetmapobj = walk(phynetmapkey)
                    save(phynetmapobj.getkey())
                    saved_phymap_keys.append(phynetmapobj.getkey())
                    saved_physicalnetworkmap_to_logicalnetwork[phynetmapobj.getkey()] = []
                    for lgnet in phynetmapobj.logicnetworks.dataset():
                        with suppress(WalkKeyNotRetrieved):
                            lgnetobj = walk(lgnet.getkey())
                            if not lgnetobj:
                                save(lgnet.getkey())
                                saved_lgnet_keys.append(lgnet.getkey())
                                saved_physicalnetworkmap_to_logicalnetwork[phynetmapobj.getkey()]\
                                    .append(lgnet.getkey())

        await call_api(self.apiroutine,"objectdb","walk",
                    {"keys":[PhysicalNetworkSet.default_key()],
                    "walkerdict":{PhysicalNetworkSet.default_key():walk_phynetset},
                    "requestid":1})

        saved_lgnet_keys = list(set(saved_lgnet_keys))
        saved_phymap_keys = list(set(saved_phymap_keys))

        print("we have find all physicalnetworkmap %s" % saved_phymap_keys)

        for k in saved_phymap_keys:
            print("# # begin to repair physicalnetwork %s" % k)
            print("# # # # remove invaild lgnet %s" % saved_physicalnetworkmap_to_logicalnetwork[k])

        def updater(keys,values):

            start = 0
            for i in range(len(saved_phymap_keys)):

                lgnet_keys = keys[1+start:start+1+len(saved_physicalnetworkmap_to_logicalnetwork[keys[start]])]
                lgnet = values[1+start:start+1+len(saved_physicalnetworkmap_to_logicalnetwork[keys[start]])]

                lgnet_dict = dict(zip(lgnet_keys,lgnet))

                for n in list(values[start].logicnetworks.dataset()):
                    if n.getkey() in lgnet_dict and lgnet_dict[n.getkey()] is None:
                        print("remove %s from %s" % (n.getkey(),keys[start]))
                        values[start].logicnetworks.dataset().discard(n)

                start = len(saved_physicalnetworkmap_to_logicalnetwork[keys[start]]) + 1

            return keys,values

        transact_keys = []

        for k in saved_phymap_keys:
            transact_keys.append(k)
            for key in saved_physicalnetworkmap_to_logicalnetwork[k]:
                transact_keys.append(key)

        if transact_keys:
            await call_api(self.apiroutine,"objectdb","transact",{"keys":transact_keys,
                                                                "updater":updater})

        print("# # # # # # repair success !")

if __name__ == '__main__':
    RepairPhyMapDB.main()
