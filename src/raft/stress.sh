#!/bin/bash

set -e

i=1
while [ 1 ]; do
	echo "start first $i test"
	go test -run 2A
	i=$(($i+1))
done
