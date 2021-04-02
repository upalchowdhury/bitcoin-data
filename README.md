### This repository have scripts to parse out data from bitcoin blocks and utxo set.

Resources used:
    - aws ec2 intance (c5.4x large) running full node and s3 bucket for data storage. 


`mod_analysis.py` is used to parse the leveldb data base and output the address, value in satoshis and block height. The output is in s3://addr-bal-output/parsed-UTXO-data/

`deserialize_blocks.sh` is used to deserialize the blocks data using `bitcoin-cli`. For this i have only used 11 blocks. The ouput is in s3://addr-bal-output/blocks-jsondata/

`spark_jsonTocsv.py` is used to normalize blocks data in to csv format. For this excercise i have only output `blockheight` and `time`. But with this all other data can be normalized from the block using proper schema. Outputs are in s3://addr-bal-output/normalized-data-from-json/



Sources used for this project:

    1. https://github.com/sr-gi/bitcoin_tools
    2. https://eprint.iacr.org/2017/1095.pdf
    3. https://www.youtube.com/watch?v=zGDTt9Q3vyM