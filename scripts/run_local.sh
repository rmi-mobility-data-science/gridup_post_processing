#!/bin/bash

INPUTS_PATH=$1
OUTPUTS_PATH=$2
CROSS_TABLE_PATH=$3
AGGREGATION_COL=$4

if [ "$#" -ne 4 ]; then
  echo "Usage: $0 <INPUTS_PATH> <OUTPUTS_PATH> <CROSS_TABLE_PATH> <AGGREGATION_COL>"
  exit 1
fi

docker run --rm \
  -v $(pwd)/data:/data \
  aggregation:v1.0.0 \
  --inputs_path /data/${INPUTS_PATH#data/} \
  --outputs_path /data/${OUTPUTS_PATH#data/} \
  --cross_table_path /data/${CROSS_TABLE_PATH#data/} \
  --aggregation_col $AGGREGATION_COL