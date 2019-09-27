#!/usr/bin/env bash

SIZE=$1
go run bench.go -op 0 -size $SIZE

for i in {0..4}
do
    go run bench.go -op 1 -size $SIZE
done