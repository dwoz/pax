#!/bin/bash

PIDS=()

for id in $(cat ./conf |cut -d':' -f1); do
   #echo $id
   python server.py $id &
   PIDS[${#PIDS[@]}]=$!
done
  n=$(($n-1))

function ctrl_c() {
   for pid in "${PIDS[@]}"; do
      kill $pid
   done
}

trap ctrl_c INT

wait ${PIDS[-1]}
