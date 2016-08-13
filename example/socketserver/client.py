from __future__ import print_function
import socket
import protocoldef as d
from namedstruct import dump

if __name__ == '__main__':
    s = socket.create_connection(('127.0.0.1', 9723))
    try:
        s.sendall(d.message[0].tobytes([d.message10_sum_request(numbers = [1,2,3]),
                                        d.message10_sum_request(numbers = list(range(0,100))),
                                        d.message10_sum_request(numbers = [i*i for i in range(0,100)]),]))
        data = b''
        while True:
            data2 = s.recv(4096)
            if not data2:
                break
            data += data2
            r = d.message[3].parse(data)
            if r:
                messages, _ = r
                break
        print(dump(messages))
    finally:
        s.close()
