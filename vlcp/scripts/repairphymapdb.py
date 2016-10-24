
from vlcp.scripts.script import ScriptModule
from vlcp.server.module import depend, callAPI
from vlcp.service.kvdb import objectdb
from vlcp.utils.networkmodel import LogicalNetworkSet, LogicalNetwork, PhysicalNetworkMap


@depend(objectdb.ObjectDB)
class RepairPhyMapDB(ScriptModule):
    """
     before version 317b31130794650a9392eb15634a2aefaba35c28
     have problem about physicalmap don't remove weakref(logicalnetwork)
     this script repair this problem in DB
    """
    def run(self):

        def walk_lgnet(key,value,walk,save):
            save(key)

            for weak_lgnet in value.set.dataset():
                try:
                    obj = walk(weak_lgnet)
                except KeyError:
                    pass
                else:
                    save(obj.getkey())

                    try:
                        phynetobj = walk(obj.physicalnetwork.getkey())
                    except KeyError:
                        pass
                    else:
                        save(phynetobj.getkey())
                        phymapkey = PhysicalNetworkMap.default_key(phynetobj.id)

                        try:
                            phymapobj = walk(phymapkey)
                        except KeyError:
                            pass
                        else:
                            save(phymapobj.getkey())

        for m in callAPI(self.apiroutine,"objectdb","walk",{"keys":[LogicalNetworkSet.default_key()],
                                                            "walkerdict":{LogicalNetworkSet.default_key():walk_lgnet},
                                                            "requestid":1}):
            yield m

        logicalnetwork = [v for k,v in zip(self.apiroutine.retvalue[0],self.apiroutine.retvalue[1])
                          if v.isinstance(LogicalNetwork)]

        physicalnetworkkeys = list(set([k for k,v in zip(self.apiroutine.retvalue[0],self.apiroutine.retvalue[1])
                               if v.isinstance(PhysicalNetworkMap)]))

        physicalnetworkmap_to_logicalnetwork = {}

        for key in physicalnetworkkeys:
            physicalnetworkmap_to_logicalnetwork[key] = []
            for lgnet in logicalnetwork:
                if PhysicalNetworkMap.default_key(lgnet.physicalnetwork.id) == key:
                    physicalnetworkmap_to_logicalnetwork[key].append(lgnet.getkey())

        update_physical_network_map = {}

        def walk_phynetmap(key,value,walk,save):
            save(key)

            for lgnet in value.logicnetworks.dataset():
                try:
                    lgnet_obj = walk(lgnet.getkey())
                except KeyError:
                    pass
                else:
                    if lgnet_obj:
                        save(lgnet_obj.getkey())

                    if not lgnet_obj and lgnet.getkey() not in physicalnetworkmap_to_logicalnetwork[key]:
                        #print(" we have found invaild lgnet key %s in phymap %s" % (lgnet.getkey(),key))
                        update_physical_network_map.setdefault(key,[]).append(lgnet.getkey())

        for m in callAPI(self.apiroutine,"objectdb","walk",{"keys":physicalnetworkkeys,
                                                            "walkerdict":dict((key,walk_phynetmap) for key in physicalnetworkkeys),
                                                            "requestid":2}):
            yield m

        print("we have find all physicalnetworkmap %s" % list(update_physical_network_map.keys()))

        for k,v in update_physical_network_map.items():
            print("# # begin to repair physicalnetwork %s" % k)
            print("# # # # remove invaild lgnet %s" % v)

        def updater(keys,values):
            for i in range(len(keys)):
                if keys[i] in update_physical_network_map:
                    removed_key = update_physical_network_map[keys[i]]

                    for weak_obj in list(values[i].logicnetworks.dataset()):
                        if weak_obj.getkey() in removed_key:
                            print(" discard %s from %s" % (weak_obj.getkey(),keys[i]))
                            values[i].logicnetworks.dataset().discard(weak_obj)


            return keys,values

        if update_physical_network_map.keys():
            for m in callAPI(self.apiroutine,"objectdb","transact",{"keys":update_physical_network_map.keys(),
                                                                "updater":updater}):
                yield m

        print("# # # # # # repair success !")

if __name__ == '__main__':
    RepairPhyMapDB.main()