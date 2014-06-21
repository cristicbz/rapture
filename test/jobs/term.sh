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

echo "term-$1-stderr-begin" >&2
for i in $(seq 1 $seqend); do
  sleep $duration
  echo "term-$1-${i}"
done
echo "term-$1-stderr-end" >&2
kill $$ SIGTERM

