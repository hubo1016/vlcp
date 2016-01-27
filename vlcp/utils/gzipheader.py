'''
Created on 2015/11/26

:author: hubo
'''

from namedstruct import *
from namedstruct import sizefromlen, packvalue

def _pack_xlen(ns):
    ns.xlen = ns._realsize() - 2

extra = nstruct((uint16_le, 'xlen'),
                name='extra',
                size=lambda x: x.xlen + 2,
                prepack=_pack_xlen,
                padding=1,
                endian='<'
                )

flg = enum('flg', globals(), uint8_le, True,
           FTEXT = 1<<0,
           FHCRC = 1<<1,
           FEXTRA = 1<<2,
           FNAME = 1<<3,
           FCOMMENT = 1<<4
           )

_flags = [('extra', FEXTRA),
          ('fname', FNAME),
          ('fcomment', FCOMMENT),
          ('fhcrc', FHCRC)]

def _pack_flg(s):
    for name,f in _flags:
        if hasattr(s, name):
            s.flg = s.flg | f
        else:
            s.flg = s.flg & (~f)


def _init_header(s):
    s.id1 = 0x1f
    s.id2 = 0x8b
    s.cm = 8
    s.xfl = 2
    s.os = 255

header = nstruct(
                 (uint8_le, 'id1'),
                 (uint8_le, 'id2'),
                 (uint8_le, 'cm'),
                 (flg, 'flg'),
                 (uint32_le, 'mtime'),
                 (uint8_le, 'xfl'),
                 (uint8_le, 'os'),
                 (optional(extra, 'extra', lambda x: x.flg & FEXTRA),),
                 (optional(cstr, 'fname', lambda x: x.flg & FNAME),),
                 (optional(cstr, 'fcomment', lambda x: x.flg & FCOMMENT),),
                 (optional(uint16_le, 'fhcrc', lambda x: x.flg & FHCRC),),
                 name = 'header',
                 endian = '<',
                 padding = 1,
                 init = _init_header,
                 prepack=_pack_flg
                 )

tail = nstruct(
               (uint32_le, 'crc32'),
               (uint32_le, 'isize'),
               endian = '<',
               name = 'tail',
               padding = 1
               )
