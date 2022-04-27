#!/bin/bash

set -e

export GOTRACEBACK=crash
rm -rf core kvraft.test

i=1
while [ 1 ]; do
	echo "start first $i test"
	go test -gcflags="-N -l" -c
	./kvraft.test -test.run 3A
	./kvraft.test -test.run 3B
	i=$(($i+1))
done
