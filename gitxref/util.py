import binascii
import hashlib
from collections import defaultdict

from bitarray import bitarray


def b2h(binsha):
    return binascii.hexlify(binsha).decode('utf8')

def hashblob(f):
    h = hashlib.sha1()
    h.update(b'blob %d\0' % f.stat().st_size)
    h.update(f.read_bytes())
    return h.digest()

def bitarray_zero(length):
    b = bitarray(length, endian='big')
    b[:] = False
    return b


def bitarray_defaultdict(length):
    return defaultdict(lambda: bitarray_zero(length))
