#!/bin/bash

if [ -z $1 ]; then
  duration=0
else
  duration=$1
fi

if [ -z $2 ]; then
  seqend=0
else
  seqend=$2
fi

echo "term-$3-stderr-begin" >&2
for i in $(seq 1 $seqend); do
  sleep $duration
  echo "term-$3-${i}"
done
echo "term-$3-stderr-end" >&2
kill $$ SIGTERM

