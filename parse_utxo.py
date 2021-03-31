from hashlib import sha256
from re import match
import plyvel
from binascii import hexlify, unhexlify
from base58 import b58encode
import sys

# THIS functions are from bitcoin_tools and was only mildly changed.
# Please refer to readme.md for the proper link to that library.

# Fee per byte range
NSPECIALSCRIPTS = 6


def txout_decompress(x):
    """ Decompresses the Satoshi amount of a UTXO stored in the LevelDB. Code is a port from the Bitcoin Core C++
    source:
        https://github.com/bitcoin/bitcoin/blob/v0.13.2/src/compressor.cpp#L161#L185
    :param x: Compressed amount to be decompressed.
    :type x: int
    :return: The decompressed amount of satoshi.
    :rtype: int
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
    """ Performs the MSB base-128 decoding of a given value. Used to decode variable integers (varints) from the LevelDB.
    The code is a port from the Bitcoin Core C++ source. Notice that the code is not exactly the same since the original
    one reads directly from the LevelDB.
    The decoding is used to decode Satoshi amounts stored in the Bitcoin LevelDB (chainstate). After decoding, values
    are decompressed using txout_decompress.
    The decoding can be also used to decode block height values stored in the LevelDB. In his case, values are not
    compressed.
    Original code can be found in:
        https://github.com/bitcoin/bitcoin/blob/v0.13.2/src/serialize.h#L360#L372
    Examples and further explanation can be found in b128_encode function.
    :param data: The base-128 encoded value to be decoded.
    :type data: hex str
    :return: The decoded value
    :rtype: int
    """

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
    """ Parses a given serialized UTXO to extract a base-128 varint.
    :param utxo: Serialized UTXO from which the varint will be parsed.
    :type utxo: hex str
    :param offset: Offset where the beginning of the varint if located in the UTXO.
    :type offset: int
    :return: The extracted varint, and the offset of the byte located right after it.
    :rtype: hex str, int
    """

    data = utxo[offset:offset+2]
    offset += 2
    more_bytes = int(data, 16) & 0x80  # MSB b128 Varints have set the bit 128 for every byte but the last one,
    # indicating that there is an additional byte following the one being analyzed. If bit 128 of the byte being read is
    # not set, we are analyzing the last byte, otherwise, we should continue reading.
    while more_bytes:
        data += utxo[offset:offset+2]
        more_bytes = int(utxo[offset:offset+2], 16) & 0x80
        offset += 2

    return data, offset


def decode_utxo(coin, outpoint, version=0.15):
    """
    
    """
        # First we will parse all the data encoded in the outpoint, that is, the transaction id and index of the utxo.
        # Check that the input data corresponds to a transaction.
    assert outpoint[:2] == '43'
        # Check the provided outpoint has at least the minimum length (1 byte of key code, 32 bytes tx id, 1 byte index)
        
    assert len(outpoint) >= 68
        # Get the transaction id (LE) by parsing the next 32 bytes of the outpoint.
    tx_id = outpoint[2:66]
        # Finally get the transaction index by decoding the remaining bytes as a b128 VARINT
    tx_index = b128_decode(outpoint[66:])

        # Once all the outpoint data has been parsed, we can proceed with the data encoded in the coin, that is, block
        # height, whether the transaction is coinbase or not, value, script type and script.
        # We start by decoding the first b128 VARINT of the provided data, that may contain 2*Height + coinbase
    code, offset = parse_b128(coin)
    code = b128_decode(code)
    height = code >> 1
    coinbase = code & 0x01

        # The next value in the sequence corresponds to the utxo value, the amount of Satoshi hold by the utxo. Data is
        # encoded as a B128 VARINT, and compressed using the equivalent to txout_compressor.
    data, offset = parse_b128(coin, offset)
    amount = txout_decompress(b128_decode(data))

        # Finally, we can obtain the data type by parsing the last B128 VARINT
    out_type, offset = parse_b128(coin, offset)
    out_type = b128_decode(out_type)

    if out_type in [0, 1]:
        data_size = 40  # 20 bytes
    elif out_type in [2, 3, 4, 5]:
        data_size = 66  # 33 bytes (1 byte for the type + 32 bytes of data)
        offset -= 2
        # Finally, if another value is found, it represents the length of the following data, which is uncompressed.
    else:
        data_size = (out_type - NSPECIALSCRIPTS) * 2  # If the data is not compacted, the out_type corresponds
            # to the data size adding the number os special scripts (nSpecialScripts).

        # And the remaining data corresponds to the script.
    script = coin[offset:]

        # Assert that the script hash the expected length
    assert len(script) == data_size

        # And to conclude, the output can be encoded. We will store it in a list for backward compatibility with the
        # previous decoder
    out = [{'amount': amount, 'out_type': out_type, 'data': script}]

    # Even though there is just one output, we will identify it as outputs for backward compatibility with the previous
    # decoder.
    return {'tx_id': tx_id, 'index': tx_index, 'coinbase': coinbase, 'outs': out, 'height': height}

def parse_ldb(fin_name, version=0.15, types=(0, 1)):
    counter = 0

    prefix = b'C'

    # Open the LevelDB
    db = plyvel.DB(fin_name, compression=None)  # Change with path to chainstate

    # Load obfuscation key (if it exists)
    o_key = db.get((unhexlify("0e00") + "obfuscate_key"))

    # If the key exists, the leading byte indicates the length of the key (8 byte by default). If there is no key,
    # 8-byte zeros are used (since the key will be XORed with the given values).
    if o_key is not None:
        o_key = hexlify(o_key)[2:]

    # For every UTXO (identified with a leading 'c'), the key (tx_id) and the value (encoded utxo) is displayed.
    # UTXOs are obfuscated using the obfuscation key (o_key), in order to get them non-obfuscated, a XOR between the
    # value and the key (concatenated until the length of the value is reached) if performed).
    not_decoded = [0, 0]
    for key, o_value in db.iterator(prefix=prefix):
        key = hexlify(key)
        if o_key is not None:
            value = deobfuscate_value(o_key, hexlify(o_value))
        else:
            value = hexlify(o_value)

        # If the decode flag is passed, we also decode the utxo before storing it. This is really useful when running
        # a full analysis since will avoid decoding the whole utxo set twice (once for the utxo and once for the tx
        # based analysis)

        value = decode_utxo(value, key, version)

        for out in value['outs']:
            # 0 --> P2PKH
            # 1 --> P2SH
            # 2 - 3 --> P2PK(Compressed keys)
            # 4 - 5 --> P2PK(Uncompressed keys)

            if counter % 100 == 0:
                sys.stdout.write('\r parsed transactions: %d' % counter)
                sys.stdout.flush()
            counter += 1

            if out['out_type'] == 0:
                if out['out_type'] not in types:
                    continue
                add = hash_160_to_btc_address(out['data'], 0)
                yield add, out['amount'], value['height']
            elif out['out_type'] == 1:
                if out['out_type'] not in types:
                    continue
                add = hash_160_to_btc_address(out['data'], 5)
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
    :param obfuscation_key: Key used to obfuscate the given value (extracted from the chainstate).
    :type obfuscation_key: str
    :param value: Obfuscated value.
    :type value: str
    :return: The de-obfuscated value.
    :rtype: str.
    """

    l_value = len(value)
    l_obf = len(obfuscation_key)

    # Get the extended obfuscation key by concatenating the obfuscation key with itself until it is as large as the
    # value to be de-obfuscated.
    if l_obf < l_value:
        extended_key = (obfuscation_key * ((l_value / l_obf) + 1))[:l_value]
    else:
        extended_key = obfuscation_key[:l_value]

    r = format(int(value, 16) ^ int(extended_key, 16), 'x')

    # In some cases, the obtained value could be 1 byte smaller than the original, since the leading 0 is dropped off
    # when the formatting.
    if len(r) is l_value-1:
        r = r.zfill(l_value)

    assert len(value) == len(r)

    return r


def change_endianness(x):
    """ Changes the endianness (from BE to LE and vice versa) of a given value.
    :param x: Given value which endianness will be changed.
    :type x: hex str
    :return: The opposite endianness representation of the given value.
    :rtype: hex str
    """

    # If there is an odd number of elements, we make it even by adding a 0
    if (len(x) % 2) == 1:
        x += "0"
    y = x.decode('hex')
    z = y[::-1]
    return z.encode('hex')


def hash_160_to_btc_address(h160, v):
    """ Calculates the Bitcoin address of a given RIPEMD-160 hash from an elliptic curve public key.
    :param h160: RIPEMD-160 hash.
    :type h160: bytes
    :param v: version (prefix) used to calculate the Bitcoin address.
     Possible values:
        - 0 for main network (PUBKEY_HASH).
        - 111 For testnet (TESTNET_PUBKEY_HASH).
    :type v: int
    :return: The corresponding Bitcoin address.
    :rtype: hex str
    """

    # If h160 is passed as hex str, the value is converted into bytes.
    if match('^[0-9a-fA-F]*$', h160):
        h160 = unhexlify(h160)

    # Add the network version leading the previously calculated RIPEMD-160 hash.
    vh160 = chr(v) + h160
    # Double sha256.
    h = sha256(sha256(vh160).digest()).digest()
    # Add the two first bytes of the result as a checksum tailing the RIPEMD-160 hash.
    addr = vh160 + h[0:4]
    # Obtain the Bitcoin address by Base58 encoding the result
    addr = b58encode(addr)

    return addr