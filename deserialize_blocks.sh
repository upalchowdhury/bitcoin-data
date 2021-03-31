#!/bin/bash

BLOCKSTART=1000
BLOCKEND=2000


for ((i=$BLOCKSTART; i<$BLOCKEND;i+=60))
do

	for ((j=i;j<i+60;j++))
	do
		echo "Sending block $j"

		BLOCKHEIGHT=$j

		BLOCKHASH=$(bitcoin-cli getblockhash $BLOCKHEIGHT)
    echo $BLOCKHASH

		FILENAME="block"${j}".json"

		bitcoin-cli getblock ${BLOCKHASH} 2 | aws s3 cp - s3://blockdata/${FILENAME} &
	done
	wait
done