from optparse import OptionParser
import logging
import uuid

from vlcp.server import main
from vlcp.server.module import Module,depend
from vlcp.service.connection import httpserver
from vlcp.service.web import static
from vlcp.utils.http import HttpHandler
from vlcp.event.core import Event,withIndices

# define new kind event message
# @withIndices("id","message")  this "id" "message" will be matcher index
@withIndices()
class ChatNewMessage(Event):
    pass

class messageBuffer:
    def __init__(self):
        self.cache = []
        self.cache_size = 100
    def newmessage(self,message):
        self.cache.append(message)

        if len(self.cache) > self.cache_size:
            self.cache = self.cache[-self.cache_size:]

    def dumpMessage(self,index):

        if index == None:
            return self.cache

        i = 1
        for msg in self.cache:
            if msg["id"] == index:
                break
            i += 1

        return self.cache[i:]


buffer = messageBuffer()

# we define our module inherit from "Module"
# and this module will depend "HttpServer", "Static" module
@depend(httpserver.HttpServer,static.Static)
class MainModule(Module):
    def __init__(self,server):
        super(MainModule,self).__init__(server)
        self.routines.append(Handler(self.scheduler))

# http handler class
class Handler(HttpHandler):

    logger = logging.getLogger("IM")
    # route /newmessage2 POST to this handler
    @HttpHandler.route(b'/newmessage2',method=[b'POST'])
    def newmessageHandler(self,env):
        #self.logger.info('newmessage')
        #self.logger.info(env)

        # while until env parseform complete
        for m in env.parseform():
            yield m

        # after it we can get the field message
        #self.logger.info(env.form)
        env.outputjson({"result":env.form["message"]})

    # another method route POST /newmessage to this handler
    # handler argument will get the form message
    @HttpHandler.routeargs(b'/newmessage')
    def newmessageHandler2(self,env,message):

        cacheData = {"id":str(uuid.uuid4()),"message":message}
        #self.logger.info(" new message cache data add" + message)

        # store this id message to cache
        buffer.newmessage(cacheData)

        # send to client for response
        env.outputjson(cacheData)

        # perpare message event to send , argument will be the event take message
        newmessageevent = ChatNewMessage(id=str(uuid.uuid4()),message=message)

        # while until send event complete
        for m in self.waitForSend(newmessageevent):
            yield m

    @HttpHandler.route(b'/updatemessage',method=[b'POST'])
    def updateMessageHandler(self,env):
        for m in env.parseform():
            yield m

        #self.logger.info(env.form)

        # if client send None index ,send all cache data to it
        # else send after index cache to it
        if "index" not in env.form:
            cache = buffer.dumpMessage(None)
        else:
            cache = buffer.dumpMessage(env.form["index"])

        #self.logger.info(cache)

        # if get None cache , this client will wait newmessage event
        if (len(cache) != 0):
            env.outputjson(dict(messages=cache))
        else:

            # create a new message matcher to wait
            newmessageeventmatcher = ChatNewMessage.createMatcher()
            #self.logger.info("wait With timeout")

            for m in self.waitWithTimeout(30,newmessageeventmatcher):
                yield m

            #self.logger.info("newmessageevent received!")
            #self.logger.info(newmessageeventmatcher)
            #self.logger.info(self.timeout)

            # here will call test the event cause
            if(self.timeout):
                    env.outputjson(dict(messages=[]))
            else:
                    # the event will store self.event
                    # we can get the message form it

                    #self.logger.info(self.event)
                    #self.logger.info(self.event.id)
                    #self.logger.info(self.event.message)

                    array = []
                    data = {"id":self.event.id,"message":self.event.message}
                    array.append(data)
                    env.outputjson(dict(messages=array))


if __name__ == '__main__':

    conf = "./app.conf"

    parsers = OptionParser()
    parsers.add_option('-d',dest='daemon',help='run as daemon',action='store_true',metavar='Daemon')
    (options,args) = parsers.parse_args()

    main(conf,(),options.daemon)


