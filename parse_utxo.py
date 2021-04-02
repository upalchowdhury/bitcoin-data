from hashlib import sha256
from re import match
import plyvel
from binascii import hexlify, unhexlify
from base58 import b58encode
import sys


###### This code is adopted from 
https://github.com/sr-gi/bitcoin_tools


NSPECIALSCRIPTS = 6


def txout_decompress(x):
    """ Decompresses the Satoshi amount of a UTXO stored in the LevelDB. 
    """

    if x == 0:
        return 0
    x -= 1
    e = x % 10
    x /= 10
    if e < 9:
        d = (x % 9) + 1
        x /= 9
        n = x * 10 + d
    else:
        n = x + 1
    while e > 0:
        n *= 10
        e -= 1
    return n


def b128_decode(data):
    n = 0
    i = 0
    while True:
        d = int(data[2 * i:2 * i + 2], 16)
        n = n << 7 | d & 0x7F
        if d & 0x80:
            n += 1
            i += 1
        else:
            return n


def parse_b128(utxo, offset=0):
    data = utxo[offset:offset+2]
    offset += 2
    more_bytes = int(data, 16) & 0x80  
    while more_bytes:
        data += utxo[offset:offset+2]
        more_bytes = int(utxo[offset:offset+2], 16) & 0x80
        offset += 2

    return data, offset


def decode_utxo(coin, outpoint, version=0.15):

    assert outpoint[:2] == '43'
        
    assert len(outpoint) >= 68

    tx_id = outpoint[2:66]

    tx_index = b128_decode(outpoint[66:])

    code, offset = parse_b128(coin)
    code = b128_decode(code)
    height = code >> 1
    coinbase = code & 0x01

    data, offset = parse_b128(coin, offset)
    amount = txout_decompress(b128_decode(data))

    
    out_type, offset = parse_b128(coin, offset)
    out_type = b128_decode(out_type)

    if out_type in [0, 1]:
        data_size = 40  
    elif out_type in [2, 3, 4, 5]:
        data_size = 66  
        offset -= 2
    else:
        data_size = (out_type - NSPECIALSCRIPTS) * 2  # If the data is not compacted, the out_type corresponds

    script = coin[offset:]

    assert len(script) == data_size

    out = [{'amount': amount, 'out_type': out_type, 'data': script}]

    return {'tx_id': tx_id, 'index': tx_index, 'coinbase': coinbase, 'outs': out, 'height': height}

def parse_ldb(fin_name, version=0.15, types=(0, 1)):
    counter = 0

    prefix = b'C'
    db = plyvel.DB(fin_name, compression=None)  

    o_key = db.get((unhexlify("0e00") + "obfuscate_key"))

    if o_key is not None:
        o_key = hexlify(o_key)[2:]

    not_decoded = [0, 0]
    for key, o_value in db.iterator(prefix=prefix):
        key = hexlify(key)
        if o_key is not None:
            value = deobfuscate_value(o_key, hexlify(o_value))
        else:
            value = hexlify(o_value)

        value = decode_utxo(value, key, version)

        for out in value['outs']:
            # 0 --> P2PKH
            # 1 --> P2SH

            if counter % 100 == 0:
                sys.stdout.write('\r parsed transactions: %d' % counter)
                sys.stdout.flush()
            counter += 1

            if out['out_type'] == 0:
                if out['out_type'] not in types:
                    continue
                add = hash160_to_btcaddress(out['data'], 0)
                yield add, out['amount'], value['height']
            elif out['out_type'] == 1:
                if out['out_type'] not in types:
                    continue
                add = hash160_to_btcaddress(out['data'], 5)
                yield add, out['amount'], value['height']
            elif out['out_type'] in (2, 3, 4, 5):
                if out['out_type'] not in types:
                    continue
                add = 'P2PK'
                yield add, out['amount'], value['height']
            else:
                not_decoded[0] += 1
                not_decoded[1] += out['amount']

    print('\nunable to decode %d transactions' % not_decoded[0])
    print('totaling %d satoshi' % not_decoded[1])

    db.close()


def deobfuscate_value(obfuscation_key, value):
    """
    De-obfuscate a given value parsed from the chainstate.
    """

    l_value = len(value)
    l_obf = len(obfuscation_key)

    if l_obf < l_value:
        extended_key = (obfuscation_key * ((l_value / l_obf) + 1))[:l_value]
    else:
        extended_key = obfuscation_key[:l_value]

    r = format(int(value, 16) ^ int(extended_key, 16), 'x')

    if len(r) is l_value-1:
        r = r.zfill(l_value)

    assert len(value) == len(r)

    return r


def change_endianness(x):

    #  make it even by adding a 0
    if (len(x) % 2) == 1:
        x += "0"
    y = x.decode('hex')
    z = y[::-1]
    return z.encode('hex')


def hash160_to_btcaddress(h160, v):
    """ 
    Calculates the Bitcoin address of a given RIPEMD-160 hash from an elliptic curve public key.
    """
    if match('^[0-9a-fA-F]*$', h160):
        h160 = unhexlify(h160)
    vh160 = chr(v) + h160

    h = sha256(sha256(vh160).digest()).digest()

    addr = vh160 + h[0:4]

    addr = b58encode(addr)

    return addr