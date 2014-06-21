#!/bin/bash

if [ -z $2 ]; then
  duration=0
else
  duration=$2
fi

if [ -z $3 ]; then
  seqend=1
else
  seqend=$3
fi

echo "segfault-$1-stderr-begin" 1>&2
for i in $(seq 1 $seqend); do
  sleep $2 0
  echo "segfault-$1-${i}"
done
echo "segfault-$1-stderr-end" 1>&2
ulimit -s 1
so() { so; }; so

