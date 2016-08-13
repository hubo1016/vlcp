from __future__ import print_function
from namedstruct import *

message_type = enum('message_type', globals(), uint8,
                ERROR = 0,
                ECHO_REQUEST = 1,
                ECHO_REPLY = 2,
                SUM_REQUEST = 3,
                SUM_REPLY = 4)

message_version = enum('message_version', globals(), uint8,
                        MESSAGE_VERSION_10 = 1)
                
message = nstruct((message_type, 'type'),
                  (message_version, 'version'),
                  (uint16, 'length'),
                  (uint32, 'xid'),
                  name = 'message',
                  size = lambda x: x.length,
                  prepack = packrealsize('length'),
                  padding = 1)
                  
message10 = nstruct(name = 'message10',
                    base = message,
                    criteria = lambda x: x.version == MESSAGE_VERSION_10,
                    init = packvalue(MESSAGE_VERSION_10, 'version')
                    )

err_type = enum('err_type', globals(), uint16,
                UNSUPPORTED_REQUEST = 0,
                PARAMETER_ERROR = 1,
                RUNTIME_ERROR = 2)

                
message_error = nstruct((err_type, 'err_type'),
                        (char[0], 'details'),
                        name = 'message_error',
                        base = message10,
                        criteria = lambda x: x.type == ERROR,
                        init = packvalue(ERROR, 'type'))

message_echo = nstruct((raw, 'data'),
                        name = 'message_echo',
                        base = message10,
                        criteria = lambda x: x.type in (ECHO_REQUEST, ECHO_REPLY),
                        init = packvalue(ECHO_REQUEST, 'type'))

message10_sum_request = nstruct((int32[0], 'numbers'),
                                name = 'message10_sum_request',
                                base = message10,
                                criteria = lambda x: x.type == SUM_REQUEST,
                                init = packvalue(SUM_REQUEST, 'type'))
                                
message10_sum_reply = nstruct((int32, 'result'),
                                name = 'message10_sum_reply',
                                base = message10,
                                criteria = lambda x: x.type == SUM_REPLY,
                                init = packvalue(SUM_REPLY, 'type'))

if __name__ == '__main__':
    from pprint import pprint
    def test_protocol():
        messages = [message10_sum_request(xid = 0, numbers = [1,2,3]),
                    message_echo(xid = 1),
                    message_echo(xid = 2, type = ECHO_REPLY)]
        messages_data = b''.join(m._tobytes() for m in messages)
        print(repr(messages_data))
        messages2 = message[0].create(messages_data)
        pprint(dump(messages2))
    test_protocol()
