#!/bin/bash

start_block=1
end_block=12


for ((i=$start_block; i<$end_block;i+=1))
do

        for ((j=i;j<i+1;j++))
        do
                echo "blocks $j"

                height=$j

                blockhash=$(bitcoin-cli getblockhash $height)

                file="block"${j}".json"

                bitcoin-cli  getblock ${blockhash} 2 | aws s3 cp - s3://addr-bal-output/${file} &
        done
        wait
done