#! /usr/bin/python
#! --*-- utf-8 --*--
import logging
from uuid import uuid1

from vlcp.server.module import Module,api,publicapi
from vlcp.event.runnable import RoutineContainer
from vlcp.utils.dataobject import updater,set_new,ReferenceObject,dump
from vlcp.utils.networkmodel import *
from vlcp.utils.ethernet import ETHERTYPE_8021Q 
from vlcp.config.config import defaultconfig

logger = logging.getLogger('NetworkVlanDriver')

@defaultconfig
class NetworkVlanDriver(Module):
    """
    Network driver for VXLAN networks. When creating a VXLAN type physical network,
    you must specify an extra option ``vlanrange``.
    """
    def __init__(self,server):
        super(NetworkVlanDriver,self).__init__(server)
        self.app_routine = RoutineContainer(self.scheduler)
        self.app_routine.main = self._main
        self.routines.append(self.app_routine)
        self.createAPI(
                       publicapi(self.createphysicalnetworks,
                                    criteria=lambda networks,type:type == 'vlan'),
                       publicapi(self.updatephysicalnetworks,
                                    criteria=lambda type,networks:type == 'vlan'),
                       publicapi(self.deletephysicalnetworks,
                                    criteria=lambda type,networks:type == 'vlan'),
                       publicapi(self.createphysicalports,
                                    criteria=lambda type,ports:type == 'vlan'),
                       publicapi(self.updatephysicalports,
                                    criteria=lambda phynettype,
                                    ports:phynettype == 'vlan'),
                       publicapi(self.deletephysicalports,
                                    criteria=lambda phynettype,
                                    ports:phynettype == 'vlan'),
                       publicapi(self.createlogicalnetworks,
                                    criteria=lambda phynettype,
                                    networks:phynettype == 'vlan'),
                       publicapi(self.updatelogicalnetworks,
                                    criteria=lambda phynettype,
                                    networks:phynettype == 'vlan'),
                       publicapi(self.deletelogicalnetworks,
                                    criteria=lambda phynettype,networks:phynettype == "vlan"),
                       #used in IOprocessing module
                       publicapi(self.createioflowparts,
                                    criteria=lambda connection,logicalnetwork,
                                    physicalport,logicalnetworkid,physicalportid:
                                    logicalnetwork.physicalnetwork.type == "vlan")

                       )

    def _main(self):

        logger.info("network_vlan_driver running ---")
        if None:
            yield

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
        new_networkmap.network = new_network.create_weakreference()
        
        return new_network,new_networkmap

    
    def updatephysicalnetworks(self,type,networks):
        def updatephynetworks(keys,values):
            phykeys = keys[0:len(keys)//2]
            phyvalues = values[0:len(values)//2]
            
            phymapkeys = keys[len(keys)//2:]
            phymapvalues = values[len(keys)//2:]

            phynetdict = dict(zip(phykeys,zip(phyvalues,phymapvalues)))

            for network in networks:
                phynet,phymap = phynetdict.get(PhysicalNetwork.default_key(network.get('id')))
                if not phynet or not phymap:
                    raise ValueError("key object not existed "+\
                            PhysicalNetwork.default_key(network['id']))
                
                if 'vlanrange' in network:
                    findflag = False
                    for k,_ in phymap.network_allocation.items():
                        find = False
                        for start,end in network.get('vlanrange'):
                            if int(k) >= start and int(k) <= end:
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
                for k,v in network.items():   
                    setattr(phynet,k,v)
            return phykeys,phyvalues

        return updatephynetworks
    
    def deletephysicalnetworks(self,type,networks):

        def deletephynetwork(keys,values):
            
            phynetlen = (len(keys) - 1)//2
            phynetkeys = keys[1:1+phynetlen]
            phynetvalues = values[1:1+phynetlen]

            phynetmapkeys = keys[1+phynetlen:]
            phynetmapvalues = values[1+phynetlen:]

            phynetdict = dict(zip(phynetkeys,zip(phynetvalues,phynetmapvalues)))
            phynetset = values[0].set.dataset()
            for network in networks:
                phynet,phynetmap = phynetdict.get(PhysicalNetwork.default_key(network.get('id')))
                # if there is phynetworkport on the phynet 
                # delete will fail
                if phynetmap and phynetmap.ports.dataset():
                    raise ValueError("delete all phynetworkport on this phynet before delete")
                # if there is logicnetwork on the phynet
                # delete will fail
                if phynetmap and phynetmap.network_allocation:
                    raise ValueError('delete all logicnetwork on this phynet before delete')
                
                phynetset.discard(phynet.create_weakreference())
           
            return keys,[values[0]]+[None]*(len(keys)-1)
        
        return deletephynetwork
    def createphysicalports(self,type,ports):
        portobjs = [self._createphysicalport(**n) for n in ports]
        
        def createpyports(keys,values):
            phynetlen = (len(keys) - 1 - len(portobjs))//2
            
            phynetkeys = keys[1+len(portobjs):1+len(portobjs)+phynetlen]
            phynetvalues = values[1+len(portobjs):1+len(portobjs)+phynetlen]
            
            phynetmapkeys = keys[1+len(portobjs)+phynetlen:]
            phynetmapvalues = values[1+len(portobjs)+phynetlen:]
            
            phynetdict = dict(zip(phynetkeys,zip(phynetvalues,phynetmapvalues)))

            for i in range(0,len(portobjs)):
                values[i + 1] = set_new(values[i + 1],portobjs[i])
                
                key = portobjs[i].physicalnetwork.getkey()
                phynet,phymap = phynetdict.get(key)
                if not phynet or not phymap:
                    raise ValueError("key object not existed "+ key) 

                phymap.ports.dataset().add(portobjs[i].create_weakreference())
                values[0].set.dataset().add(portobjs[i].create_weakreference())

            return keys[0:1+len(ports)] + phynetmapkeys,values[0:1+len(ports)] + phynetmapvalues
        return createpyports
    def _createphysicalport(self,physicalnetwork,name,vhost,systemid,bridge,**args):

        p = PhysicalPort.create_instance(vhost,systemid,bridge,name)
        p.physicalnetwork = ReferenceObject(PhysicalNetwork.default_key(physicalnetwork))
        
        for k,v in args.items():
            setattr(p,k,v)

        return p
    
    def updatephysicalports(self,phynettype,ports):

        def updatephyports(keys,values):
            
            portdict = dict(zip(keys,values))
            for port in ports:
                
                key = PhysicalPort.default_key(port['vhost'],port['systemid'],
                            port['bridge'],port['name'])
                phyport = portdict[key]
                if phyport:
                    for k,v in port.items():
                        setattr(phyport,k,v)
                else:
                    raise ValueError("key object not existed "+ key) 
            
            return keys,values

        return updatephyports
   
    def deletephysicalports(self,phynettype,ports):

        def deletephyports(keys,values):
            portkeys = keys[1:1+len(ports)]
            portvalues = values[1:1+len(ports)]
            
            phynetmapkeys = keys[1+len(ports):]
            phynetmapvalues = values[1+len(ports):]
            
            phynetmapdict = dict(zip(phynetmapkeys,phynetmapvalues))
            portdict = dict(zip(portkeys,portvalues))

            for port in ports:
                phymap = phynetmapdict.get(PhysicalNetworkMap.default_key(port.get("phynetid")))
                
                key = PhysicalPort.default_key(port['vhost'],port['systemid'],port['bridge'],port['name'])
                portobj = portdict.get(key)
                
                if not portobj or not phymap:
                    raise ValueError("key object not existed "+ key) 

                phymap.ports.dataset().discard(portobj.create_weakreference())
               
                values[0].set.dataset().discard(portobj.create_weakreference())
           
            return keys,[values[0]]+[None]*len(ports) + phynetmapvalues
        return deletephyports
    def createlogicalnetworks(self,phynettype,networks):
        
        networkmap = [self._createlogicalnetwork(**n) for n in networks]

        def createlgnetworks(keys,values):
            
            phynetlen = (len(keys) - len(networkmap)*2 - 1)//2
            phynetkeys = keys[1 + len(networkmap)*2 : 1 + len(networkmap)*2 + phynetlen]
            phynetvalues = values[1 + len(networkmap)*2 : 1 + len(networkmap)*2 + phynetlen]
            
            phynetmapkeys = keys[1+len(networkmap)*2 + phynetlen:]
            phynetmapvalues = values[1+len(networkmap)*2 + phynetlen:]
            
            phynetmapdict = dict(zip(phynetkeys,zip(phynetvalues,phynetmapvalues)))
            
            #
            # have an problem ,  when [{...},{... vlanid is least or last+1}]
            # first one will allocate least one vlanid,second will conflict  
            #
            # so set new lgnet that have user defind 'vlanid' first
            for i in range(0,len(networks)):
                if hasattr(networkmap[i][0],'vlanid'):

                    vlanid = int(networkmap[i][0].vlanid)
                    networkmap[i][0].vlanid = vlanid
                    phynet,phymap = phynetmapdict.get(networkmap[i][0].physicalnetwork.getkey())

                    if not phynet or not phymap:
                        raise ValueError("physicalnetwork key object not existed "+\
                                networkmap[i][0].physicalnetwork.getkey()) 

                    if _isavaliablevlanid(phynet.vlanrange,phymap.network_allocation.keys(),vlanid):
                        phymap.network_allocation[str(vlanid)] = networkmap[i][0].create_weakreference()
                    else:
                        raise ValueError("user defind vlan id has been used or out of range!")
                    
                    # set lgnetwork
                    values[1+i] = set_new(values[i + 1],networkmap[i][0])
                    # set lgnetworkmap
                    values[1+i+len(networks)] = set_new(values[i + 1 + len(networks)],networkmap[i][1])
                    # set phynetmap
                  
                    #_,phymap = phynetmapdict.get(networkmap[i][0].physicalnetwork.getkey())
                    phymap.logicnetworks.dataset().add(networkmap[i][0].create_weakreference())

                    values[0].set.dataset().add(networkmap[i][0].create_weakreference())


            for i in range(0,len(networks)):
                if not getattr(networkmap[i][0],'vlanid',None):
                    # there is no 'vlanid' in lgnetwork
                    # allocated one from vlanrange

                    phynet,phymap = phynetmapdict.get(networkmap[i][0].physicalnetwork.getkey())
                    
                    if not phynet or not phymap:
                        raise ValueError("physicalnetwork key object not existed "+\
                                networkmap[i][0].physicalnetwork.getkey()) 

                    vlanid = _findavaliablevlanid(phynet.vlanrange,phymap.network_allocation.keys())
                    if not vlanid:
                        raise ValueError("there is no avaliable vlan id")
                    setattr(networkmap[i][0],'vlanid',vlanid)
                    phymap.network_allocation[str(vlanid)] = networkmap[i][0].create_weakreference()
                    
                    # set lgnetwork
                    values[1+i] = set_new(values[i + 1],networkmap[i][0])
                    # set lgnetworkmap
                    values[1+i+len(networks)] = set_new(values[i + 1 + len(networks)],networkmap[i][1])
                    # set phynetmap
                  
                    #_,phymap = phynetmapdict.get(networkmap[i][0].physicalnetwork.getkey())
                    phymap.logicnetworks.dataset().add(networkmap[i][0].create_weakreference())

                    values[0].set.dataset().add(networkmap[i][0].create_weakreference())
            return keys[0:1] + keys[1:1+len(networks)+len(networks)]+phynetmapkeys,\
                    values[0:1]+values[1:1+len(networks)+len(networks)] + phynetmapvalues
        return createlgnetworks
        
    def _createlogicalnetwork(self,physicalnetwork,id,**args):

        logicalnetwork = LogicalNetwork.create_instance(id)
        logicalnetworkmap = LogicalNetworkMap.create_instance(id)
        
        for k,v in args.items():
            setattr(logicalnetwork,k,v)

        logicalnetworkmap.network = logicalnetwork.create_reference()
        logicalnetwork.physicalnetwork = ReferenceObject(PhysicalNetwork.default_key(physicalnetwork))

        return (logicalnetwork,logicalnetworkmap)
    
   
    def updatelogicalnetworks(self,phynettype,networks):

        def updatelgnetworks(keys,values):
            
            phynetlen = (len(keys) - len(networks))//2
            
            lgnetkeys = keys[0:len(networks)]
            lgnetvalues = values[0:len(networks)]
            phynetkeys = keys[len(networks):len(keys)+phynetlen]
            phynetvalues = values[len(networks):len(keys)+phynetlen]
            
            phynetmapkeys = keys[len(networks)+phynetlen:]
            phynetmapvalues = values[len(networks)+phynetlen:]

            phynetdict = dict(zip(phynetkeys,zip(phynetvalues,phynetmapvalues)))
            lgnetdict = dict(zip(lgnetkeys,lgnetvalues))
            
            for network in networks:
                # 
                # to update vlanid, we should delete it first ,
                #
                if "vlanid" in network:
                    phynet,phynetmap = phynetdict.get(PhysicalNetwork.default_key(network.get("phynetid")))
                    
                    if not phynet or not phynetmap:
                        raise ValueError("physicalnetwork key object not existed "+\
                            PhysicalNetwork.default_key(network["phynetid"])) 
                    
                    lgnet = lgnetdict.get(LogicalNetwork.default_key(network["id"]))
                    
                    del phynetmap.network_allocation[str(lgnet.vlanid)]

            for network in networks:
                phynet,phynetmap = phynetdict.get(PhysicalNetwork.default_key(network.get("phynetid")))
                if not phynet or not phynetmap:
                    raise ValueError("physicalnetwork key object not existed "+\
                        PhysicalNetwork.default_key(network["phynetid"])) 
                
                lgnet = lgnetdict.get(LogicalNetwork.default_key(network["id"]))
               
                if "vlanid" in network:
                    vlanid = int(network["vlanid"])
                    
                    if vlanid == lgnet.vlanid:
                        continue

                    if _isavaliablevlanid(phynet.vlanrange,phynetmap.network_allocation.keys(),vlanid):
                        phynetmap.network_allocation[str(vlanid)] = lgnet.create_weakreference()
                    else:
                        raise ValueError("new vlanid is not avaliable")
                    
                    setattr(lgnet,'vlanid',vlanid)
                
                for k,v in network.items():
                    # this phynetid is fack attr for find phynet phymap
                    if k != 'phynetid' and k != 'vlanid':
                        setattr(lgnet,k,str(v))
            return keys[0:len(networks)] + phynetmapkeys ,values[0:len(networks)] + phynetmapvalues

        return updatelgnetworks

    def deletelogicalnetworks(self,phynettype,networks):

        def deletelgnetworks(keys,values):
            phynetlen = (len(keys) - 1 - len(networks))//2

            lgnetkeys = keys[1:1+len(networks)]
            lgnetvalues = values[1:1+len(networks)]
            
            lgnetmapkeys = keys[1+len(networks):1+len(networks)+len(networks)]
            lgnetmapvalues = values[1+len(networks):1+len(networks)+len(networks)]
            
            phynetmapkeys = keys[1+len(networks)+len(networks):]
            phynetmapvalues = values[1+len(networks)+len(networks):]

            lgnetdict = dict(zip(lgnetkeys,zip(lgnetvalues,lgnetmapvalues)))
            phynetmapdict = dict(zip(phynetmapkeys,phynetmapvalues))
            
            for network in networks:
                lgnet,lgnetmap = lgnetdict.get(LogicalNetwork.default_key(network.get("id")))

                phymap = phynetmapdict.get(PhysicalNetworkMap.default_key(network.get("phynetid")))
                
                if not phymap:
                    raise ValueError("physicalnetwork map key object not existed " + network["phynetid"]) 

                values[0].set.dataset().discard(lgnet.create_weakreference())
                phymap.logicnetworks.dataset().discard(lgnet.create_weakreference())
                del phymap.network_allocation[str(lgnet.vlanid)]
            
            return keys,[values[0]]+[None]*len(networks)*2+phynetmapvalues
        return deletelgnetworks

    def createioflowparts(self,connection,logicalnetwork,physicalport,logicalnetworkid,physicalportid):

        #
        #  1. used in IOProcessing , when physicalport add to logicalnetwork 
        #     return : input flow match vlan oxm, input flow vlan parts actions
        #              output flow vlan parts actions, output group bucket
        #
        
        input_match_oxm = [
                    connection.openflowdef.create_oxm(
                        connection.openflowdef.OXM_OF_VLAN_VID,
                        logicalnetwork.vlanid|connection.openflowdef.OFPVID_PRESENT)
                ]

        input_action = [
                   connection.openflowdef.ofp_action(type = 
                        connection.openflowdef.OFPAT_POP_VLAN)    
              ]

        output_action = [
                    connection.openflowdef.ofp_action_push(ethertype=ETHERTYPE_8021Q),
                    connection.openflowdef.ofp_action_set_field(
                            field = connection.openflowdef.create_oxm(
                                    connection.openflowdef.OXM_OF_VLAN_VID,
                                    logicalnetwork.vlanid |
                                    connection.openflowdef.OFPVID_PRESENT
                                )
                        ),
                    connection.openflowdef.ofp_action_output(
                            port = physicalportid 
                        )
                ]
        output_action2 = [
                    connection.openflowdef.ofp_action_push(ethertype=ETHERTYPE_8021Q),
                    connection.openflowdef.ofp_action_set_field(
                            field = connection.openflowdef.create_oxm(
                                    connection.openflowdef.OXM_OF_VLAN_VID,
                                    logicalnetwork.vlanid |
                                    connection.openflowdef.OFPVID_PRESENT
                                )
                        ),
                    connection.openflowdef.ofp_action_output(
                            port = connection.openflowdef.OFPP_IN_PORT
                        )
                ]
        
        # this action is same as ouput_action  on type vlan
        output_group_bucket_action = [
                    connection.openflowdef.ofp_action_push(ethertype=ETHERTYPE_8021Q),
                    connection.openflowdef.ofp_action_set_field(
                            field = connection.openflowdef.create_oxm(
                                    connection.openflowdef.OXM_OF_VLAN_VID,
                                    logicalnetwork.vlanid |
                                    connection.openflowdef.OFPVID_PRESENT
                                )
                        ),
                    connection.openflowdef.ofp_action_output(
                            port = physicalportid
                        )
                ]

        return input_match_oxm,input_action,output_action,output_group_bucket_action,output_action2
#
# utils function
#

def _findavaliablevlanid(vlanrange,allocated):
    
    vlanid = None
    for vr in vlanrange:
        find = False
        for v in range(vr[0],vr[1]):
            if str(v) not in allocated:
                vlanid = v
                find = True
                break

        if find:
            break
    return vlanid

def _isavaliablevlanid(vlanrange,allocated,vlanid):
    
    find = False
    for start,end in vlanrange:
        if start <= int(vlanid) <= end:
            find = True
            break

    if find:
        if str(vlanid) not in allocated:
            find = True
        else:
            find = False
    else:
        find = False

    return find
