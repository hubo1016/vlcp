import logging
import uuid
import time

from vlcp.server.module import Module,depend,callAPI
from vlcp.server import main
from vlcp.event.runnable import RoutineContainer
from vlcp.service.sdn import viperflow

logger = logging.getLogger("testviperflow")
logger.setLevel(logging.DEBUG)

@depend(viperflow.ViperFlow)
class MainModule(Module):
    def __init__(self,server):
        super(MainModule,self).__init__(server)
        
        self.app_routine = RoutineContainer(self.scheduler)
        self.app_routine.main = self._main
        self.routines.append(self.app_routine)

    def _main(self):
        
        logger.info(" test viperflow running ----")
        # test create physicalnetwork
        # case 1 : must special vlanrange
        try:
            for m in callAPI(self.app_routine,'viperflow',"createphysicalnetwork",{},timeout = 1):
                yield m
        except ValueError as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 1 createphysicalnetwork success \033[0m")
        else:
            logger.info("\033[1;31;40m test 1 createphysicalnetwork failed \033[0m")
        
        # case 2 : special vlanrange [[100,200]]
        try:
            for m in callAPI(self.app_routine,'viperflow','createphysicalnetwork',{'vlanrange':[(100,200)]},timeout = 1):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 2 special vlanrange [[100,200]] createphysicalnetwork failed \033[0m")
        else:
            result = self.app_routine.retvalue
            try:
                assert len(result) == 1
                assert result[0].get('vlanrange') == [[100,200]]
            except:
                logger.info("\033[1;31;40m test 2 special vlanrange [[100,200]] createphysicalnetwork failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 2 special vlanrange [[100,200]] createphysicalnetwork success \033[0m")
        
        # case 3 : special vlanrange [[100,200],[200,201]]
        try:
            for m in callAPI(self.app_routine,'viperflow','createphysicalnetwork',{'vlanrange':[(100,200),(200,201)]},timeout = 1):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 3 special vlanrange [[100,200],[200,201]] createphysicalnetwork failed \033[0m")
        else:
            result = self.app_routine.retvalue
            try:
                assert len(result) == 1
                assert result[0].get('vlanrange') == [[100,200],[200,201]]
            except:
                logger.info("\033[1;31;40m test 3 special vlanrange [[100,200],[200,201]] createphysicalnetwork failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 3 special vlanrange [[100,200],[200,201]] createphysicalnetwork success \033[0m")

        # case 4 : special vlanrange [[100,200],[201,4094]]
        try:
            for m in callAPI(self.app_routine,'viperflow','createphysicalnetwork',{'vlanrange':[(100,200),(201,4094)]},timeout = 1):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 4 special vlanrange [[100,200],[201,4094]] createphysicalnetwork failed \033[0m")
        else:
            result = self.app_routine.retvalue
            try:
                assert len(result) == 1
                assert result[0].get('vlanrange') == [[100,200],[201,4094]]
            except:
                logger.info("\033[1;31;40m test 4 special vlanrange [[100,200],[201,4094]] createphysicalnetwork failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 4 special vlanrange [[100,200],[201,4094]] createphysicalnetwork success \033[0m")

        # case 5 : special vlanrange [[100,200],[201,4096]]
        try:
            for m in callAPI(self.app_routine,'viperflow','createphysicalnetwork',{'vlanrange':[(100,200),(201,4096)]},timeout = 1):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 5 special vlanrange [[100,200],[201,4096]] createphysicalnetwork failed \033[0m")
        else:
            result = self.app_routine.retvalue
            try:
                assert len(result) == 1
                assert result[0].get('vlanrange') == [[100,200],[201,4096]]
            except:
                logger.info("\033[1;31;40m test 5 special vlanrange [[100,200],[201,4096]] createphysicalnetwork failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 5 special vlanrange [[100,200],[201,4096]] createphysicalnetwork success \033[0m")
        
        # case 6 : special vlanrange [[100,200],[201,4096]], create 1000 phynetwork

        # first produce 1000 different id
        ids = [str(uuid.uuid1()) for i in range(0,1000)]
        networks = [{'vlanrange':[(100,200)],'type':'vlan','id':id} for id in ids]

        begintime = time.time()
        try:
            for m in callAPI(self.app_routine,'viperflow','createphysicalnetworks',{'networks':networks}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 6 createphysicalnetworks 1000 failed \033[0m")
        else:
            results = self.app_routine.retvalue
            try:
                assert len(results) == 1000
                 
                for result in results:
                    assert result.get('vlanrange') == [[100,200]]
                    assert result.get('id') in ids
            except:
                endtime = time.time()
                logger.info("\033[1;31;40m test 6 createphysicalnetworks 1000 failed used %r\033[0m",endtime - begintime)
                raise SystemExit
            else:
                endtime = time.time()
                logger.info("\033[1;31;40m test 6 createphysicalnetworks 1000 success used %r \033[0m",endtime - begintime)

        #case 7 : list all phynetwork
        try:
            for m in callAPI(self.app_routine,'viperflow','listphysicalnetworks',{}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 7 listphysicalnetworks failed \033[0m")
        else:
            results = self.app_routine.retvalue 
            try:
                assert len(results) > 1000
                for result in results:
                    assert result.get('vlanrange')
                    assert result.get('id')
            except:
                logger.info("\033[1;31;40m test 7 listphysicalnetworks failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 7 listphysicalnetworks success \033[0m")
        
        #case 8 : list one phynetwork 
        try:
            for m in callAPI(self.app_routine,'viperflow','listphysicalnetworks',{"id":ids[999]}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 8 listphysicalnetwork one failed \033[0m")
        else:
            results = self.app_routine.retvalue 
            try:
                assert len(results) == 1
                for result in results:
                    assert result.get('vlanrange')
                    assert result.get('id') == ids[999]
            except:
                logger.info("\033[1;31;40m test 8 listphysicalnetwork one failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 8 listphysicalnetwork one success \033[0m")

        #case 9 : update one phynetwork 
        try:
            for m in callAPI(self.app_routine,'viperflow','updatephysicalnetwork',{"id":ids[999],'name':"one"}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 9 updatephysicalnetwork one failed \033[0m")
        else:
            results = self.app_routine.retvalue 
            try:
                assert len(results) == 1
                for result in results:
                    assert result.get('vlanrange')
                    assert result.get('id') == ids[999]
                    assert result.get('name') == "one"
            except:
                logger.info("\033[1;31;40m test 9 updatephysicalnetwork one failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 9 updatephysicalnetwork one success \033[0m")
        
        #case 10 : update one not existed phynetwork 
        try:
            for m in callAPI(self.app_routine,'viperflow','updatephysicalnetwork',{"id":"123",'name':"one"}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 10 updatephysicalnetwork not existed one success \033[0m")
        else:
            logger.info("\033[1;31;40m test 10 updatephysicalnetwork one failed \033[0m")

        #case 11 : delete one phynetwork
        try:
            for m in callAPI(self.app_routine,'viperflow','deletephysicalnetwork',{"id":ids[999]}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 11 deletephysicalnetwork one failed \033[0m")
        else:
            result = self.app_routine.retvalue
            try:
                assert result.get('status') == 'OK'
            except:
                logger.info("\033[1;31;40m test 11 deletephysicalnetwork one failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 11 deletephysicalnetwork one success \033[0m")
        
        #case 12 : list one not existed phynetwork 
        try:
            for m in callAPI(self.app_routine,'viperflow','listphysicalnetworks',{"id":ids[999]}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 12 listphysicalnetwork one not existed failed \033[0m")
        else:
            results = self.app_routine.retvalue
            try:
                assert len(results) == 0
            except:
                logger.info("\033[1;31;40m test 12 listphysicalnetwork one not existed failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 12 listphysicalnetwork one not existed success \033[0m")

        #case 13 : update 1000 phynetwork, but this one not existed

        n = [{"id":i,"name":"one"} for i in ids]
        try:
            for m in callAPI(self.app_routine,'viperflow','updatephysicalnetworks',{"networks":n}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 13 updatephysicalnetwork 1000 but one not existed success \033[0m")
        else:
            logger.info("\033[1;31;40m test 13 updatephysicalnetwork 1000 but one not existed failed  \033[0m")
        
        # case 14: update 999 phynetwork, 
        n = [{"id":i,"name":"one"} for i in ids[0:-1]]
        begintime = time.time()
        try:
            for m in callAPI(self.app_routine,'viperflow','updatephysicalnetworks',{"networks":n}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 14 updatephysicalnetwork 999 failed \033[0m")
        else:
            results = self.app_routine.retvalue
            endtime = time.time()
            try:
                assert len(results) == 999
                for result in results:
                    assert result.get('vlanrange')
                    assert result.get('id') in ids
                    assert result.get('name') == "one"
            except:
                logger.info("\033[1;31;40m test 14 updatephysicalnetwork 999 failed used %r \033[0m",endtime - begintime)
            else:
                logger.info("\033[1;31;40m test 14 updatephysicalnetwork 999 success used %r \033[0m",endtime - begintime)
        
        #case 15 : list all phynetwork where name = 'one'
        try:
            for m in callAPI(self.app_routine,'viperflow','listphysicalnetworks',{"name":"one"}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 15 listphysicalnetworks name = 'one' failed \033[0m")
        else:
            results = self.app_routine.retvalue 
            try:
                assert len(results) == 999
                for result in results:
                    assert result.get('vlanrange')
                    assert result.get('id')
                    assert result.get('name') == 'one'
            except:
                logger.info("\033[1;31;40m test 15 listphysicalnetworks name = 'one' failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 15 listphysicalnetworks name = 'one' success \033[0m")
        
        #case 16 : delete 999 phynetwork
        n = [{"id":i} for i in ids[0:-1]]
        begintime = time.time()
        try:
            for m in callAPI(self.app_routine,'viperflow','deletephysicalnetworks',{"networks":n}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 16 deletephysicalnetworks 999 failed \033[0m")
        else:
            result = self.app_routine.retvalue
            endtime = time.time()
            try:
                assert result.get('status') == 'OK'
            except:
                logger.info("\033[1;31;40m test 16 deletephysicalnetworks 999 failed used %r\033[0m",endtime-begintime)
            else:
                logger.info("\033[1;31;40m test 16 deletephysicalnetworks 999 success used %r\033[0m",endtime-begintime)

        logger.info("--------- end test physicalnetwork resource ----------")

        physicalnetworkids = []
        try:
            for m in callAPI(self.app_routine,'viperflow','listphysicalnetworks',{}):
                yield m
        except Exception as e:
            logger.info(e)
        else:
            results = self.app_routine.retvalue

            for result in results:
                physicalnetworkids.append([result.get("id"),result.get("vlanrange")])
        
        logger.info("physcialnetwork id %r used to next test",physicalnetworkids)
        
        #case 17: test create one phynetworkport

        try:
            for m in callAPI(self.app_routine,'viperflow','createphysicalport',
                    {"physicalnetwork":physicalnetworkids[0][0],"name":"eth0"}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 17 createphysicalport one  failed \033[0m")
        else:
            result = self.app_routine.retvalue
            try:
                assert len(result) == 1
                assert result[0].get("name") == "eth0"
                assert result[0].get("physicalnetwork").get("id") == physicalnetworkids[0][0]
            except:
                logger.info("\033[1;31;40m test 17 createphysicalport one failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 17 createphysicalport one success \033[0m")
        
        #case 18: test create one existed phynetworkport

        try:
            for m in callAPI(self.app_routine,'viperflow','createphysicalport',
                    {"physicalnetwork":physicalnetworkids[0][0],"name":"eth0"}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 18 createphysicalport one existed success \033[0m")
        else:
            logger.info("\033[1;31;40m test 18 createphysicalport one existed failed \033[0m")

        #case 19: test create one phynetworkport

        try:
            for m in callAPI(self.app_routine,'viperflow','createphysicalport',
                    {"physicalnetwork":physicalnetworkids[0][0],"name":"eth0","systemid":"1234","bridge":"br0"}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 19 createphysicalport one full name failed \033[0m")
        else:
            result = self.app_routine.retvalue
            try:
                assert len(result) == 1
                assert result[0].get("name") == "eth0"
                assert result[0].get("systemid") == "1234"
                assert result[0].get("bridge") == "br0"
                assert result[0].get("physicalnetwork").get("id") == physicalnetworkids[0][0]
            except:
                logger.info("\033[1;31;40m test 19 createphysicalport one full name failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 19 createphysicalport one full name success \033[0m")

        
        #case 20 : test list all phynetworkport
        try:
            for m in callAPI(self.app_routine,'viperflow','listphysicalports',{}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 20 listphysicalport failed \033[0m")
        else:
            results = self.app_routine.retvalue
            try:
                assert len(results) == 2
                for result in results:
                    assert result.get("name") == "eth0"
                    assert result.get("systemid") == "1234" or result.get("systemid") =="%"
                    assert result.get("bridge") == "br0" or result.get("bridge") == "%"
                    assert result.get("physicalnetwork").get("id") == physicalnetworkids[0][0]
            except:
                logger.info("\033[1;31;40m test 20 listphysicalport failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 20 listphysicalport success \033[0m")

        #case 21 : test list one phynetworkport
        try:
            for m in callAPI(self.app_routine,'viperflow','listphysicalports',{"name":"eth0"}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 21 listphysicalport one failed \033[0m")
        else:
            results = self.app_routine.retvalue
            try:
                assert len(results) == 1
                for result in results:
                    assert result.get("name") == "eth0"
                    assert result.get("systemid") =="%"
                    assert result.get("bridge") == "%"
                    assert result.get("physicalnetwork").get("id") == physicalnetworkids[0][0]
            except:
                logger.info("\033[1;31;40m test 21 listphysicalport one failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 21 listphysicalport one success \033[0m")
        
        #case 22 : test list one full phynetworkport
        try:
            for m in callAPI(self.app_routine,'viperflow','listphysicalports',
                    {"name":"eth0","systemid":"1234","bridge":"br0"}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 22 listphysicalport one full failed \033[0m")
        else:
            results = self.app_routine.retvalue
            try:
                assert len(results) == 1
                for result in results:
                    assert result.get("name") == "eth0"
                    assert result.get("systemid") =="1234"
                    assert result.get("bridge") == "br0"
                    assert result.get("physicalnetwork").get("id") == physicalnetworkids[0][0]
            except:
                logger.info("\033[1;31;40m test 22 listphysicalport one full failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 22 listphysicalport one full success \033[0m")
        
        #case 23 : test update one phynetworkport
        try:
            for m in callAPI(self.app_routine,'viperflow','updatephysicalport',
                    {"name":"eth0","systemid":"1234","bridge":"br0","mac":"0123456789"}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 23 updatephysicalport one failed \033[0m")
        else:
            results = self.app_routine.retvalue
            try:
                assert len(results) == 1
                for result in results:
                    assert result.get("name") == "eth0"
                    assert result.get("systemid") =="1234"
                    assert result.get("bridge") == "br0"
                    assert result.get("mac") == "0123456789"
                    assert result.get("physicalnetwork").get("id") == physicalnetworkids[0][0]
            except:
                logger.info("\033[1;31;40m test 23 updatephysicalport one failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 23 updatephysicalport one success \033[0m")
        
        #case 24 : test list one where phynetworkport
        try:
            for m in callAPI(self.app_routine,'viperflow','listphysicalports',
                    {"name":"eth0","mac":"0123456789"}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 24 listphysicalport one where failed \033[0m")
        else:
            results = self.app_routine.retvalue
            try:
                assert len(results) == 0
            except:
                logger.info("\033[1;31;40m test 24 listphysicalport one where failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 24 listphysicalport one where success \033[0m")    
        
        #case 25: test delete one phynetworkport
        try:
            for m in callAPI(self.app_routine,'viperflow','deletephysicalport',
                    {"name":"eth0"}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 25 deletephysicalport one failed \033[0m")
        else:
            result = self.app_routine.retvalue
            try:
                assert result.get("status") == "OK"
            except:
                logger.info("\033[1;31;40m test 25 deletephysicalport one failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 25 deletephysicalport one success \033[0m")    

        #case 26: test create 1000 phynetworkport

        p1 = [{"name":"eth"+str(i),"physicalnetwork":physicalnetworkids[0][0]} for i in range(0,500)]
        p2 = [{"name":"eth"+str(i),"physicalnetwork":physicalnetworkids[1][0]} for i in range(500,1000)]
        begintime = time.time()
        try:
            for m in callAPI(self.app_routine,'viperflow','createphysicalports',
                    {"ports":p1 + p2}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 26 createphysicalport 1000 failed \033[0m")
            raise SystemExit
        else:
            results = self.app_routine.retvalue
            endtime = time.time()
            try:
                assert len(results) == 1000
                for result in results:
                    assert result.get("physicalnetwork").get("id") == physicalnetworkids[0][0] or \
                           result.get("physicalnetwork").get("id") == physicalnetworkids[1][0]
            except:
                logger.info("\033[1;31;40m test 26 createphysicalport 1000 failed used %r \033[0m",endtime-begintime)
                raise SystemExit
            else:
                logger.info("\033[1;31;40m test 26 createphysicalport 1000 success used %r \033[0m",endtime-begintime)
        
        # case 27: test update 500 phynetworkport
        p1 = [{"name":"eth"+str(i),"mac":str(i)} for i in range(0,500)]
        begintime = time.time()
        try:
            for m in callAPI(self.app_routine,'viperflow','updatephysicalports',
                    {"ports":p1}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 27 updatephysicalport 500 failed \033[0m")
        else:
            results = self.app_routine.retvalue
            endtime = time.time()
            try:
                assert len(results) == 500
                for result in results:
                    assert 0 <= int(result.get("mac")) <= 500
                    assert result.get("physicalnetwork").get("id") == physicalnetworkids[0][0]
            except:
                logger.info("\033[1;31;40m test 27 updatephysicalport 500 failed used %r \033[0m",endtime-begintime)
            else:
                logger.info("\033[1;31;40m test 27 updatephysicalport 500 success used %r \033[0m",endtime-begintime)
        #case 28: test list phynetwork == xxx
        try:
            for m in callAPI(self.app_routine,'viperflow','listphysicalports',
                    {"physicalnetwork":physicalnetworkids[1][0]}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 28 listphysicalport phynetwork = xxx failed \033[0m")
        else:
            results = self.app_routine.retvalue
            try:
                assert len(results) == 500
                for result in results:
                    assert result.get("physicalnetwork").get("id") == physicalnetworkids[1][0]
            except:
                logger.info("\033[1;31;40m test 28 listphysicalport phynetwork = xxx failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 28 listphysicalport phynetwork = xxx success \033[0m")    
        
        #case 29: test delete 1000 phynetworkport
        p1 = [{"name":"eth"+str(i)} for i in range(0,1000)]
        begintime = time.time()
        try:
            for m in callAPI(self.app_routine,'viperflow','deletephysicalports',
                    {"ports":p1}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 29 deletephysicalports 1000 failed \033[0m")
        else:
            endtime = time.time()
            result = self.app_routine.retvalue
            try:
                assert result.get("status") == "OK"
            except:
                logger.info("\033[1;31;40m test 29 deletephysicalports 1000 failed used %r \033[0m",endtime - begintime)
            else:
                logger.info("\033[1;31;40m test 29 deletephysicalports 1000 success used %r \033[0m",endtime - begintime)    
        
        #case 30: test delete phynetwork where have physicalnetworkport 
        try:
            for m in callAPI(self.app_routine,'viperflow','deletephysicalnetwork',{"id":physicalnetworkids[0][0]}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 30 deletephysicalnetwork where have physicalnetworkport success \033[0m")
        else:
            logger.info("\033[1;31;40m test 30 deletephysicalnetwork where have physicalnetworkport failed \033[0m")

        logger.info("--------- end test physicalnetworkport resource ----------")

        #case 31: test createlogicalnetwork one
        try:
            for m in callAPI(self.app_routine,'viperflow','createlogicalnetwork',
                    {"physicalnetwork":physicalnetworkids[0][0]}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 31 createlogicalnetwork one failed \033[0m")
        else:
            result = self.app_routine.retvalue
            try:
                assert len(result) == 1
                assert result[0].get("id")
                assert result[0].get("vlanid") == 100
                assert result[0].get("physicalnetwork").get("id") == physicalnetworkids[0][0]
            except:
                logger.info("\033[1;31;40m test 31 createlogicalnetwork one failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 31 createlogicalnetwork one success \033[0m")

        #case 32：test createlogcialnetwork that vlanid have been used
        try:
            for m in callAPI(self.app_routine,'viperflow','createlogicalnetwork',
                    {"physicalnetwork":physicalnetworkids[0][0],"vlanid":"100"}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 32 createlogicalnetwork that vlanid have been used success \033[0m")
        else:
            logger.info("\033[1;31;40m test 32 createlogicalnetwork that vlanid have been used failed \033[0m")
        
        
        #case 33: test createlogicalnetworks 1000
        for phynetid in physicalnetworkids:
            if phynetid[1] == [[100, 200], [201, 4094]]:
                pyid = phynetid[0]

        n = [{"physicalnetwork":pyid,"id":str(uuid.uuid1())} for i in range(0,1000)]
        begintime = time.time()
        try:
            for m in callAPI(self.app_routine,'viperflow','createlogicalnetworks',{"networks":n}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 33 createlogicalnetwork 100 failed \033[0m")
            raise SystemExit
        else:
            endtime = time.time()
            results = self.app_routine.retvalue
            try:
                assert len(results) == 1000
                for result in results:
                    assert result.get("id")
                    assert result.get("physicalnetwork").get("id") == pyid
            except:
                logger.info("\033[1;31;40m test 33 createlogicalnetwork 1000 failed used %r \033[0m",endtime - begintime)
            else:
                logger.info("\033[1;31;40m test 33 createlogicalnetwork 1000 success %r \033[0m",endtime - begintime)   

        #case 34: test updatelogicalnetworks 1000
        pn = [{"id":ne.get("id"),"name":"two"} for ne in n]
        begintime = time.time()
        try:
            for m in callAPI(self.app_routine,'viperflow','updatelogicalnetworks',{"networks":pn}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 34 updatelogicalnetwork 1000 failed \033[0m")
        else:
            endtime = time.time()
            results = self.app_routine.retvalue
            try:
                assert len(results) == 1000
                for result in results:
                    assert result.get("id")
                    assert result.get("name") == "two"
                    assert result.get("physicalnetwork").get("id") == pyid
            except:
                logger.info("\033[1;31;40m test 34 updatelogicalnetwork 1000 failed used %r \033[0m",endtime - begintime)
            else:
                logger.info("\033[1;31;40m test 34 updatelogicalnetwork 1000 success %r \033[0m",endtime - begintime)   
        
        #case 35: test listlogicalnetwork all
        pnid = [{"id":ne.get("id")} for ne in n]
        try:
            for m in callAPI(self.app_routine,'viperflow','listlogicalnetworks',{}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 35 listlogicalnetwork all failed \033[0m")
        else:
            endtime = time.time()
            results = self.app_routine.retvalue
            try:
                assert len(results) >= 1000
            except:
                logger.info("\033[1;31;40m test 35 listlogicalnetwork all failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 35 listlogicalnetwork all success\033[0m")        

        #case 36: test listlogicalnetwork name = "two"
        try:
            for m in callAPI(self.app_routine,'viperflow','listlogicalnetworks',{"name":"two"}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 36 listlogicalnetwork name = 'two' failed \033[0m")
        else:
            endtime = time.time()
            results = self.app_routine.retvalue
            try:
                assert len(results) == 1000
                for result in results:
                    assert result.get("name") == "two"
            except:
                logger.info("\033[1;31;40m test 36 listlogicalnetwork name = 'two' failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 36 listlogicalnetwork name = 'two' success \033[0m")   
        
        #case 37：test listlogicalnetwork phynetwork == xxx
        try:
            for m in callAPI(self.app_routine,'viperflow','listlogicalnetworks',{"physicalnetwork":pyid}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 37 listlogicalnetwork name = 'two' failed \033[0m")
        else:
            endtime = time.time()
            results = self.app_routine.retvalue
            try:
                assert len(results) >= 1000 
                for result in results:
                    assert result.get("physicalnetwork").get("id") == pyid
            except:
                logger.info("\033[1;31;40m test 37 listlogicalnetwork phynetwork == xxx failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 37 listlogicalnetwork phynetwork == xxx success \033[0m")  
        
        #case 38：test listlogicalnetwork phynetwork == xxx, name = "two"
        try:
            for m in callAPI(self.app_routine,'viperflow','listlogicalnetworks',
                    {"physicalnetwork":pyid,"name":"two"}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 38 listlogicalnetwork name = 'two' failed \033[0m")
        else:
            endtime = time.time()
            results = self.app_routine.retvalue
            try:
                assert len(results) == 1000 
                for result in results:
                    assert result.get("name") == "two"
                    assert result.get("physicalnetwork").get("id") == pyid
            except:
                logger.info("\033[1;31;40m test 38 listlogicalnetwork phynetwork == xxx name = two failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 38 listlogicalnetwork phynetwork == xxx name = two success \033[0m")  
        
        #case 39：test deletelogicalnetwork 1000 
        pnid = [{"id":ne.get("id")} for ne in n]
        begintime = time.time()
        try:
            for m in callAPI(self.app_routine,'viperflow','deletelogicalnetworks',
                    {"networks":pnid}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 39 deletelogicalnetworks failed \033[0m")
        else:
            endtime = time.time()
            result = self.app_routine.retvalue
            try:
                assert result.get("status") == "OK"
            except:
                logger.info("\033[1;31;40m test 39 deletelogicalnetworks 1000 failed used %r\033[0m",endtime - begintime)
            else:
                logger.info("\033[1;31;40m test 39 deletelogicalnetworks 1000 success used %r\033[0m",endtime - begintime)  
        
        #case 40: test create logicalnetwork allocate vlanid
        for phynetid in physicalnetworkids:
            if phynetid[1] == [[100, 200]]:
                pyid = phynetid[0]

        lgid = [str(uuid.uuid1()),str(uuid.uuid1()),str(uuid.uuid1())]
        n = [{"physicalnetwork":pyid,"id":lgid[0]},{"physicalnetwork":pyid,"vlanid":"101","id":lgid[1]},{"physicalnetwork":pyid,"id":lgid[2]}]
        try:
            for m in callAPI(self.app_routine,'viperflow','createlogicalnetworks',
                    {"networks":n}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 40 createlogicalnetworks allocate vlanid one failed \033[0m")
        else:
            result = self.app_routine.retvalue
            try:
                assert len(result) == 3
                assert result[0].get("id") == lgid[0]
                assert result[0].get("vlanid") == 102
                assert result[0].get("physicalnetwork").get("id") == pyid

                assert result[1].get("id") == lgid[1]
                assert result[1].get("vlanid") == 101
                assert result[1].get("physicalnetwork").get("id") == pyid

                assert result[2].get("id") == lgid[2]
                assert result[2].get("vlanid") == 103
                assert result[2].get("physicalnetwork").get("id") == pyid


            except:
                logger.info("\033[1;31;40m test 40 createlogicalnetworks allocate vlanid failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 40 createlogicalnetworks allocate vlanid success \033[0m")

        #case 41: test exchange vlanid  
        n = [{"id":lgid[0],"vlanid":"103"},{"id":lgid[1],"vlanid":"102"},{"vlanid":"101","id":lgid[2]}]
        try:
            for m in callAPI(self.app_routine,'viperflow','updatelogicalnetworks',{"networks":n}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 41 updatelogicalnetwork exchange vlanid failed \033[0m")
        else:
            endtime = time.time()
            results = self.app_routine.retvalue

            try:
                assert len(results) == 3
                assert results[0].get("id") == lgid[0]
                assert results[0].get("vlanid") == 103

                assert results[1].get("id") == lgid[1]
                assert results[1].get("vlanid") == 102
                
                assert results[2].get("id") == lgid[2]
                assert results[2].get("vlanid") == 101

            except:
                logger.info("\033[1;31;40m test 41 updatelogicalnetwork exchange vlanid falied \033[0m")
            else:
                logger.info("\033[1;31;40m test 41 updatelogicalnetwork exchange vlanid success \033[0m")   
        
    
        #case 42: test delete one logicalnetwork
        try:
            for m in callAPI(self.app_routine,'viperflow','deletelogicalnetwork',
                    {"id":lgid[2]}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 42 deletelogicalnetwork one failed \033[0m")
        else:
            endtime = time.time()
            result = self.app_routine.retvalue
            try:
                assert result.get("status") == "OK"
            except:
                logger.info("\033[1;31;40m test 42 deletelogicalnetwork one failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 42 deletelogicalnetwork one success \033[0m")  
 
        logger.info("--------- end test logicalnetwork resource ----------")

        #case 43: test create logicalnetworkport one
        try:
            for m in callAPI(self.app_routine,'viperflow','createlogicalport',{"logicalnetwork":lgid[0]}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 43 createlogicalport one failed \033[0m")
        else:
            result = self.app_routine.retvalue
            try:
                assert len(result) == 1
                assert result[0].get("id")
                assert result[0].get("network").get("id") == lgid[0]

            except:
                logger.info("\033[1;31;40m test 43 createlogicalport one failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 43 createlogicalport one success \033[0m")   
        
        #case 44: test create logicalnetworkport one id
        pid = str(uuid.uuid1())
        try:
            for m in callAPI(self.app_routine,'viperflow','createlogicalport',{"logicalnetwork":lgid[0],"id":pid}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 44 createlogicalport one id failed \033[0m")
        else:
            result = self.app_routine.retvalue
            try:
                assert len(result) == 1
                assert result[0].get("id") == pid
                assert result[0].get("network").get("id") == lgid[0]

            except:
                logger.info("\033[1;31;40m test 44 createlogicalport one id failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 44 createlogicalport one id success \033[0m")   
        
        #case 45: test update logicalnetworkport one id
        try:
            for m in callAPI(self.app_routine,'viperflow','updatelogicalport',{"name":"three","id":pid}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 45 updatelogicalport one id failed \033[0m")
        else:
            result = self.app_routine.retvalue
            try:
                assert len(result) == 1
                assert result[0].get("id") == pid
                assert result[0].get("name") == "three"
                assert result[0].get("network").get("id") == lgid[0]

            except:
                logger.info("\033[1;31;40m test 45 updatelogicalport one id failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 45 updatelogicalport one id success \033[0m")   
        
        #case 46: test list logicalnetworkport all
        try:
            for m in callAPI(self.app_routine,'viperflow','listlogicalports',{}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 46 listlogicalport all failed \033[0m")
        else:
            result = self.app_routine.retvalue
            try:
                assert len(result) == 2
                assert result[0].get("network").get("id") == lgid[0]

            except:
                logger.info("\033[1;31;40m test 46 listlogicalport all failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 46 listlogicalport all success \033[0m")   
        
        #case 47: test list logicalnetworkport logicalnetwork == xxx
        try:
            for m in callAPI(self.app_routine,'viperflow','listlogicalports',{"logicalnetwork":lgid[0]}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 47 listlogicalport logicalnetwork = xxx failed \033[0m")
        else:
            result = self.app_routine.retvalue
            try:
                assert len(result) == 2
                assert result[0].get("network").get("id") == lgid[0]

            except:
                logger.info("\033[1;31;40m test 47 listlogicalport logicalnetwork = xxx failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 47 listlogicalport logcialnetwork = xxx success \033[0m")   
        
        #case 48: test delete logicalnetworkport 
        try:
            for m in callAPI(self.app_routine,'viperflow','deletelogicalport',{"id":pid}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 48 deletelogicalport failed \033[0m")
        else:
            result = self.app_routine.retvalue
            try:
                assert result.get("status") == "OK" 

            except:
                logger.info("\033[1;31;40m test 48 deletelogicalport failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 48 deletelogicalport success \033[0m")   
        
        #case 49: test createnetworkports 1000

        p = [{"id":str(uuid.uuid1()),"logicalnetwork":lgid[1]} for i in range(0,1000)]
        begintime = time.time() 
        try:
            for m in callAPI(self.app_routine,'viperflow','createlogicalports',{"ports":p}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 49 createlogicalports failed \033[0m")
        else:
            results = self.app_routine.retvalue
            endtime = time.time()
            try:
                assert len(results) == 1000
                for result in results:
                    assert result.get("network").get("id") == lgid[1]
            except:
                logger.info("\033[1;31;40m test 49 createlogicalports failed used %r \033[0m",endtime - begintime)
            else:
                logger.info("\033[1;31;40m test 49 createlogicalports success used %r \033[0m",endtime - begintime)   
        
        #case 50: test updatenetworkports 1000
        
        up = [{"name":"ppp","id":port.get("id")} for port in p]
        begintime = time.time() 
        try:
            for m in callAPI(self.app_routine,'viperflow','updatelogicalports',{"ports":up}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 50 updatelogicalports failed \033[0m")
        else:
            results = self.app_routine.retvalue
            endtime = time.time()
            try:
                assert len(results) == 1000
                for result in results:
                    assert result.get("network").get("id") == lgid[1]
                    assert result.get("name") == "ppp"
            except:
                logger.info("\033[1;31;40m test 50 updatelogicalports failed used %r \033[0m",endtime - begintime)
            else:
                logger.info("\033[1;31;40m test 50 updatelogicalports success used %r \033[0m",endtime - begintime)  

        #case 51: test delete one logicalnetwork that have logicalport
        try:
            for m in callAPI(self.app_routine,'viperflow','deletelogicalnetwork',
                    {"id":lgid[1]}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 51 deletelogicalnetwork that have logicalport success \033[0m")
        else:
            logger.info("\033[1;31;40m test 51 deletelogicalnetwork that have logcialport failed \033[0m")

        #case 52: test delete logicalport 1000
        begintime = time.time() 
        try:
            for m in callAPI(self.app_routine,'viperflow','deletelogicalports',{"ports":up}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 52 deletelogicalports 1000 failed \033[0m")
        else:
            result = self.app_routine.retvalue
            endtime = time.time()
            try:
                assert result.get("status") == "OK"
            except:
                logger.info("\033[1;31;40m test 52 deletelogicalports 1000 failed used %r \033[0m",endtime - begintime)
            else:
                logger.info("\033[1;31;40m test 52 deletelogicalports 1000 success used %r \033[0m",endtime - begintime) 
        logger.info(" ---- test vxlan driver ----")

        physicalnetworkid1 = str(uuid.uuid1())
        physicalnetworkid2 = str(uuid.uuid1())
        physicalnetworkid3 = str(uuid.uuid1())
        logger.info(" ---- test physicalnetworkid %r , %r , %r----",physicalnetworkid1,physicalnetworkid2,physicalnetworkid3)

        physicalnetwork = [{"type":"vxlan","id":physicalnetworkid1,"vnirange":[(1000,2000)]},
                           {"type":"vxlan","id":physicalnetworkid2,"vnirange":[(3000,4000)]},
                           {"type":"vlan","id":physicalnetworkid3,"vlanrange":[(100,200)]}]
        
        # case 52 : test cretephysicalnetwork 
        try:
            for m in callAPI(self.app_routine,'viperflow','createphysicalnetworks',{'networks':physicalnetwork},timeout = 1):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 52 vxlan vlan type createphysicalnetworks failed \033[0m")
        else:
            results = self.app_routine.retvalue
            try:
                assert len(results) == 3

                for result in results:
                    if result.get('id') == physicalnetworkid1:
                        assert result.get('vnirange') == [[1000,2000]]
                    if result.get('id') == physicalnetworkid2:
                        assert result.get('vnirange') == [[3000,4000]]
                    if result.get('id') == physicalnetworkid3:
                        assert result.get('vlanrange') == [[100,200]]

            except:
                logger.info("\033[1;31;40m test 52 vxlan vlan createphysicalnetwork failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 52 vxlan vlan createphysicalnetwork success \033[0m")

        updatephysicalnetwork = [{"id":physicalnetworkid1,"name":"A"},
                           {"id":physicalnetworkid2,"name":"B"},
                           {"id":physicalnetworkid3,"name":"C"}]
       
        # case 53: test updatephysicalnetwork
        try:
            for m in callAPI(self.app_routine,'viperflow','updatephysicalnetworks',{'networks':updatephysicalnetwork},timeout = 1):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 52 vxlan vlan type updatephysicalnetworks failed \033[0m")
        else:
            results = self.app_routine.retvalue
            try:
                assert len(results) == 3
                for result in results:

                    if result.get('id') == physicalnetworkid1:
                        assert result.get("name") == "A"
                    if result.get('id') == physicalnetworkid2:
                        assert result.get("name") == "B"
                    if result.get('id') == physicalnetworkid3:
                        assert result.get("name") == "C"

            except:
                logger.info("\033[1;31;40m test 53 vxlan vlan updatephysicalnetwork failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 53 vxlan vlan updatephysicalnetwork success \033[0m")

        #case 54: test deletephysicalnetwork
        deletephysicalnetwork = [
                           {"id":physicalnetworkid2},
                           {"id":physicalnetworkid3}]
 
        try:
            for m in callAPI(self.app_routine,'viperflow','deletephysicalnetworks',{'networks':deletephysicalnetwork},timeout = 1):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 54 vxlan vlan type deletephysicalnetworks failed \033[0m")
        else:
            result = self.app_routine.retvalue
            try:
                assert result.get('status') == "OK"
            except:
                logger.info("\033[1;31;40m test 54 vxlan vlan deletephysicalnetwork failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 54 vxlan vlan deletephysicalnetwork success \033[0m")
        
        #case 55: test createlogicalnetwork 

        logicalnetworkid1 = uuid.uuid1()
        logicalnetworkid2 = uuid.uuid1()

        logicalnetworks = [{"physicalnetwork":physicalnetworkid1,"id":logicalnetworkid1},
                {"physicalnetwork":physicalnetworkid1,"id":logicalnetworkid2,"vni":1000}]
        try:
            for m in callAPI(self.app_routine,'viperflow','createlogicalnetworks',
                    {"networks":logicalnetworks}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 55 createlogicalnetworks failed \033[0m")
        else:
            result = self.app_routine.retvalue
            try:
                assert len(result) == 2
                assert result[0].get("id")
                assert result[0].get("vni") == 1001
                assert result[0].get("physicalnetwork").get("id") == physicalnetworkid1

                assert result[1].get("id")
                assert result[1].get("vni") == 1000
                assert result[1].get("physicalnetwork").get("id") == physicalnetworkid1

            except:
                logger.info("\033[1;31;40m test 55 createlogicalnetworks failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 55 createlogicalnetworks success \033[0m")

        # 56: test updatelogicalnetwork
        updatelogicalnetworks = [{"id":logicalnetworkid1,"vni":1000},
                {"id":logicalnetworkid2,"vni":1001}]
        try:
            for m in callAPI(self.app_routine,'viperflow','updatelogicalnetworks',
                    {"networks":updatelogicalnetworks}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 56 updatelogicalnetworks failed \033[0m")
        else:
            result = self.app_routine.retvalue
            try:
                assert len(result) == 2
                assert result[0].get("id")
                assert result[0].get("vni") == 1000
                assert result[0].get("physicalnetwork").get("id") == physicalnetworkid1

                assert result[1].get("id")
                assert result[1].get("vni") == 1001
                assert result[1].get("physicalnetwork").get("id") == physicalnetworkid1

            except:
                logger.info("\033[1;31;40m test 56 updatelogicalnetworks failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 56 updatelogicalnetworks success \033[0m")
        
        logger.info("-----  test type native network ----")
        #57: test native network create   
        nativePhysicalNetworkid = str(uuid.uuid1())
        physicalnetwork = [{"type":"native","id":nativePhysicalNetworkid}]

        try:
            for m in callAPI(self.app_routine,'viperflow','createphysicalnetworks',{'networks':physicalnetwork},timeout = 1):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 57 native type createphysicalnetworks failed \033[0m")
        else:
            results = self.app_routine.retvalue
            try:
                assert len(results) == 1
                
                assert results[0].get("id") == nativePhysicalNetworkid
                assert results[0].get("type") == 'native'
            except:
                logger.info("\033[1;31;40m test 57 native createphysicalnetwork failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 57 native vlan createphysicalnetwork success \033[0m")
        
        #58:test create two logicalnetwork on native physicalnetwork

        logicalnetworkid1 = uuid.uuid1()
        logicalnetworkid2 = uuid.uuid1()

        logicalnetworks = [{"physicalnetwork":nativePhysicalNetworkid,"id":logicalnetworkid1},
                {"physicalnetwork":nativePhysicalNetworkid,"id":logicalnetworkid2}]
        try:
            for m in callAPI(self.app_routine,'viperflow','createlogicalnetworks',
                    {"networks":logicalnetworks}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 58 createlogicalnetworks success \033[0m")
        else:
            logger.info("\033[1;31;40m test 58 createlogicalnetworks failed \033[0m")
       
        #59: test create logicalnetwork
        logicalnetworkid1 = uuid.uuid1()

        logicalnetworks = [{"physicalnetwork":nativePhysicalNetworkid,"id":logicalnetworkid1}]
        try:
            for m in callAPI(self.app_routine,'viperflow','createlogicalnetworks',
                    {"networks":logicalnetworks}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 59 createlogicalnetworks failed \033[0m")
        else:
            result = self.app_routine.retvalue
            try:
                assert len(result) == 1
                assert result[0].get("id")
                assert result[0].get("physicalnetwork").get("id") == nativePhysicalNetworkid


            except:
                logger.info("\033[1;31;40m test 59 createlogicalnetworks failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 59 createlogicalnetworks success \033[0m")
        
        #case 60: test deletephysicalnetwork there logicalnetwork
        deletephysicalnetwork = [
                           {"id":nativePhysicalNetworkid},
                           ]
 
        try:
            for m in callAPI(self.app_routine,'viperflow','deletephysicalnetworks',{'networks':deletephysicalnetwork},timeout = 1):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 60 deletephysicalnetworks have logicalnetwork success \033[0m")
        else:
            logger.info("\033[1;31;40m test 60 deletephysicalnetworks have logcialnetwork failed \033[0m")
        
        # 61: test updatelogicalnetwork
        updatelogicalnetworks = [{"id":logicalnetworkid1,"name":"AAA"}]
        try:
            for m in callAPI(self.app_routine,'viperflow','updatelogicalnetworks',
                    {"networks":updatelogicalnetworks}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 61 updatelogicalnetworks failed \033[0m")
        else:
            result = self.app_routine.retvalue
            try:
                assert len(result) == 1
                assert result[0].get("id")
                assert result[0].get("name") == "AAA"
                assert result[0].get("physicalnetwork").get("id") == nativePhysicalNetworkid
            except:
                logger.info("\033[1;31;40m test 61 updatelogicalnetworks failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 61 updatelogicalnetworks success \033[0m")
        
        # 62: test deletelogicalnetwork
        try:
            for m in callAPI(self.app_routine,'viperflow','deletelogicalnetwork',
                    {"id":logicalnetworkid1}):
                yield m
        except Exception as e:
            logger.info(e)
            logger.info("\033[1;31;40m test 62 deletelogicalnetwork failed \033[0m")
        else:
            result = self.app_routine.retvalue
            try:
                assert result.get('status') == "OK"
            except:
                logger.info("\033[1;31;40m test 62 deletelogicalnetwork failed \033[0m")
            else:
                logger.info("\033[1;31;40m test 62 deletelogicalnetwork success \033[0m")

if __name__ == '__main__':
    
    # here will auto search this file Module
    main("/root/software/vlcp/vlcp.conf",("__main__.MainModule",
                                          "vlcp.service.sdn.plugins.networkvlandriver.NetworkVlanDriver",
                                          "vlcp.service.sdn.plugins.networknativedriver.NetworkVlanDriver",
                                          "vlcp.service.sdn.plugins.networkvxlandriver.NetworkVlanDriver",
                                          'vlcp.service.manage.webapi.WebAPI',
                                          'vlcp.service.manage.modulemanager.Manager',
                                          ))
