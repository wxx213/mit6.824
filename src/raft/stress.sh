#!/bin/bash

set -e

export GOTRACEBACK=crash
rm -rf core raft.test

i=1
while [ 1 ]; do
	echo "start first $i test"
	go test -gcflags="-N -l" -c
	./raft.test -test.run 2A
	./raft.test -test.run 2B
	./raft.test -test.run 2C
	i=$(($i+1))
done
