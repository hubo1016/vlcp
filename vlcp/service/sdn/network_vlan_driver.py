#! /usr/bin/python
#! --*-- utf-8 --*--
import logging
from uuid import uuid1

from vlcp.server.module import Module,api,publicapi
from vlcp.event.runnable import RoutineContainer
from vlcp.utils.dataobject import updater,set_new
from vlcp.utils.networkmodel import *

logger = logging.getLogger('network_vlan_driver')

class network_vlan_driver(Module):
    def __init__(self,server):
        super(network_vlan_driver,self).__init__(server)
        self.app_routine = RoutineContainer(self.scheduler)
        self.app_routine.main = self._main
        self.routines.append(self.app_routine)
        self.createAPI(publicapi(self.createphysicalnetwork,
                                    criteria=lambda type,id,args:type == 'vlan'),
                       publicapi(self.createphysicalnetworks,
                                    criteria=lambda networks,type:type == 'vlan'),
                       publicapi(self.updatephysicalnetwork,
                                    criteria=lambda type,id,args:type == 'vlan'),
                       publicapi(self.deletephysicalnetwork,
                                    criteria=lambda type,id:type == 'vlan'))

    def _main(self):

        logger.info("network_vlan_driver running ---")
        if None:
            yield
    def createphysicalnetwork(self,type,id,args = {}):
        
        new_network,new_networkmap = self._createphysicalnetwork(type,id,**args)

        #
        # this func will be to update DB transaction, args will be the 
        # old value that effect 
        #
        @updater
        def createphynetwork(physet,phynet,phymap):
            phynet = set_new(phynet,new_network)
            phymap = set_new(phymap,new_networkmap)
            
            physet.set.dataset().add(phynet.create_weakreference())
            
            return [physet,phynet,phymap]

        return createphynetwork
    
    def createphysicalnetworks(self,networks,type):
        
        new_networks = [ self._createphysicalnetwork(**n) for n in networks]
        
        def createphynetworks(keys,values):
            for i in range(0,len(new_networks)): 
                values[i + 1] = set_new(values[i + 1],new_networks[i][0])
                values[i + 1 + len(new_networks)] = set_new(values[i + 1 + len(new_networks)],
                        new_networks[i][1])
                values[0].set.dataset().add(new_networks[i][0].create_weakreference())

            return keys,values

        return createphynetworks
    
    def _createphysicalnetwork(self,type,id,**args):

        if 'vlanrange' not in args:
            raise ValueError('must specify vlanrange with network type vlan')

        
        #
        # vlanrange [(1,100),(200,500)]
        #
        try:
            lastend = 0
            for start,end in args.get('vlanrange'):
                if start > end or start <= lastend:
                    raise ValueError('vlan sequences overlapped or disorder')
                if end > 4095:
                    raise ValueError('vlan out of range (0 -- 4095)')  
                lastend = end
        except:
            raise ValueError('vlanrange format error,[(1,100),(200,500)]')


        # create an new physical network
        new_network = PhysicalNetwork.create_instance(id)
        new_network.type = type

        for k,v in args.items():
            setattr(new_network,k,v)
        
        # create 1 : 1 physical network map
        new_networkmap = PhysicalNetworkMap.create_instance(id)
        new_networkmap.network = new_network.create_reference()
        
        return new_network,new_networkmap


    def updatephysicalnetwork(self,type,id,args = {}):
        
        # we also should check 'vlanrange'
        # vlanrange must be total [], we can not 
        # update a segment ()

        if 'vlanrange' in args:
            try:
                lastend = 0
                for start,end in args.get('vlanrange'):
                    if start > end or start <=lastend:
                        raise ValueError('vlan sequences overlapped or disorder')
                    if end > 4095:
                        raise ValueError('vlan out of range (0 -- 4095)') 
                    lastend = end
            except:
                raise ValueError('vlanrange format error,[(1,100),(200,500)]')
        
        @updater
        def updatephynetwork(phynet,phymap):
            for k,v in args.items():
                # update vlanrange, we should check range with allocation
                if k == 'vlanrange':
                    findflag = False
                    for k,_ in phymap.network_allocation.items():
                        find = False
                        for start,end in v:
                            if k >= start and k <= end:
                                find = True
                                break
                        if find == False:
                            findflag = False
                            break
                        else:
                            findflag = True
                    
                    if len(phymap.network_allocation) == 0:
                        findflag = True
                    
                    # new vlanrange do not 
                    if findflag == False:
                        raise ValueError('new vlan range do not match with allocation')
                setattr(phynet,k,v)
            # only change phynet , so return only phynet
            return [phynet]
        
        return updatephynetwork

    def deletephysicalnetwork(self,type,id):
        @updater
        def deletephynetwork(physet,phynet,phymap):
            # if there is logicnetwork on the phynet
            # delete will fail
            if len(phymap.network_allocation) > 0:
                raise ValueError('delete all logicnetwork on this phynet before delete')
            
            
            for weakobj in physet.set.dataset().copy():
                if weakobj.getkey() == phynet.getkey():
                    physet.set.dataset().remove(weakobj)
            return [physet,None,None]

        return deletephynetwork
