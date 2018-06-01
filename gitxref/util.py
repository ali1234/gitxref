import binascii


def b2h(binsha):
    return binascii.hexlify(binsha).decode('utf8')