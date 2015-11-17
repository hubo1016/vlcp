'''
Created on 2015/7/13

@author: hubo
'''
from .namedstruct import nstruct, prim, raw, char, enum, varchr

int8 = prim('b', 'int8')

uint8 = prim('B', 'uint8')

boolean = prim('?', 'bool')

int16 = prim('h', 'int16')

uint16 = prim('H', 'uint16')

int32 = prim('i', 'int32')

uint32 = prim('I', 'uint32')

int64 = prim('q', 'int64')

uint64 = prim('Q', 'uint64')

single = prim('f', 'float')

double = prim('d', 'double')
