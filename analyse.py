import os
import tempfile
import argparse
import sqlite3
import s3fs
from utils import parse_ldb

home = "/home/ubuntu"
chainstatedata = home + ".bitcon/chainstate"
blockdata = home + ".bitcoin/blocks"
index = home + ".bitcoin/index"

s3path = "s3://data/"
# def input_args():
#     parser = argparse.ArgumentParser(description='Process UTXO set from chainstate and return unspent output per'
#                                                  ' address for P2PKH and P2SH addresses')
#     parser.add_argument(
#         'chainstate',
#         metavar='PATH_TO_CHAINSTATE_DIR',
#         type=str,
#         help='path to bitcoin chainstate directory (usually in full node data dir)'
#     )
    
#     parser.add_argument(
#         'out',
#         metavar='OUTFILE',
#         type=str,
#         default=None,
#         help='output file in .csv'
#     )
    
#     parser.add_argument(
#         '--P2PKH',
#         metavar='bool',
#         type=bool,
#         default=True,
#         help='include P2PKH transactions, default 1'
#     )
#     parser.add_argument(
#         '--P2SH',
#         metavar='bool',
#         type=bool,
#         default=True,
#         help='include P2PSH transactions, default 1'
#     )
#     parser.add_argument(
#         '--P2PK',
#         metavar='bool',
#         type=bool,
#         default=False,
#         help='include P2PK transactions, default 0 '
#              'warning - cannot decode address for this type of transactions, the total output'
#              'for these addresses will be included under P2PK entry in output csv file'
#     )
    
#     a = parser.parse_args()

#     if a.sort not in {None, 'ASC', 'DESC'}:
#         raise AssertionError('--sort can be only "ASC" or "DESC"')

#     if a.keep_sqlite and not a.lowmem:
#         raise AssertionError('--keep_sqlite cannot be used with --lowmem')
#     return a


# def get_types(in_args):
#     keep_types = set()
#     if in_args.P2PKH:
#         keep_types.add(0)
#     if in_args.P2SH:
#         keep_types.add(1)
#     if in_args.P2PK:
#         keep_types |= {2, 3, 4, 5}
#     return keep_types


def compute():

    add_dict = dict()
    for add, val, height in parse_ldb(
            fin_name=chainstatedata,
            version=0.15,
            types=P2PKH):
        if add in add_dict:
            add_dict[add][0] += val
            add_dict[add][1] = height
        else:
            add_dict[add] = [val, height]

    for key in add_dict.iterkeys():
        ll = add_dict[key]
        yield key, ll[0], ll[1]


if __name__ == '__main__':

    s3 = s3fs.S3FileSystem(anon=True)

    add_iter = in_mem(args)
    BUCKET_NAME = "chainstate_data"
    w = ['address,value_satoshi,last_height']
    with s3.open(f"{BUCKET_NAME}/addr_bal.csv",'w') as f:
        c = 0
        for address, sat_val, block_height in add_iter:
            if sat_val == 0:
                continue
            w.append(
                address + ',' + str(sat_val) + ',' + str(block_height)
            )
            c += 1
            if c == 1000:
                f.write('\n'.join(w) + '\n')
                w = []
                c = 0
            if c > 0:
                f.write('\n'.join(w) + '\n')
            f.write('\n')
        print('done')