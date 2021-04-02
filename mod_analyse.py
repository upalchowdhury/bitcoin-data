import os
import tempfile
import argparse
import sqlite3
import s3fs
from parse_utxo import parse_ldb

home = "/home/ubuntu"
chainstatedata = home + ".bitcon/chainstate/"
blockdata = home + ".bitcoin/blocks"
index = home + ".bitcoin/index"


def compute():

    """
    This function is to parse leveldb and store address, output balance and block height and store it in dictionary 
    """

    add_dict = dict()
    for add, val, height in parse_ldb(
            fin_name=chainstatedata,
            version=0.15,
            types={0,1}):
        if add in add_dict:
            add_dict[add][0] += val
            add_dict[add][1] = height
        else:
            add_dict[add] = [val, height]

    for key in add_dict.iterkeys():
        l = add_dict[key]
        yield key, l[0], l[1]


if __name__ == '__main__':

    s3 = s3fs.S3FileSystem(anon=True)

    add_iter = compute()

    BUCKET_NAME = "addr-bal-output"

    ## output the address and balance in a csv file and only with positive balances

    aggregated = ['address,value,height']
    with s3.open(f'{BUCKET_NAME}/smaller_size.csv','w') as f:
        c = 0
        for address, val, block_height in add_iter:
            if val == 0:
                continue
            aggregated.append(
                address + ',' + str(val) + ',' + str(block_height)
            )
            c += 1
            if c == 1000:
                f.write('\n'.join(aggregated) + '\n')
                aggregated = []
                c = 0
            if c > 0:
                f.write('\n'.join(aggregated) + '\n')
            f.write('\n')
        print('done')