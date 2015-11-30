'''
Created on 2015/7/13

@author: hubo
'''
from .namedstruct import nstruct, prim, raw, char, enum, varchr, cstr, optional

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

int8_le = prim('b', 'int8', '<')

uint8_le = prim('B', 'uint8', '<')

boolean_le = prim('?', 'bool', '<')

int16_le = prim('h', 'int16', '<')

uint16_le = prim('H', 'uint16', '<')

int32_le = prim('i', 'int32', '<')

uint32_le = prim('I', 'uint32', '<')

int64_le = prim('q', 'int64', '<')

uint64_le = prim('Q', 'uint64', '<')

single_le = prim('f', 'float', '<')

double_le = prim('d', 'double', '<')
