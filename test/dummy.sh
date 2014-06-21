#!/bin/bash

if [ "$1" = "fail" ]; then
  echo
  for i in 25 50; do
    sleep $2
    echo "${i}% ($1)"
  done
  echo 'controlled failure'
  exit -1
else
  echo
  for i in 25 50 75; do
    sleep $2
    echo "${i}% ($1)"
  done
fi

