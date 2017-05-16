#!/bin/bash

if [[ "$1" == "help" ]]; then
  echo "Usage: $0 <YYYY-MM-DD>(YYYY-03-31, YYYY-06-30, YYYY-09-30, YYYY-12-31)"
  exit 1
fi

insight_season="$1"
today=$(date +'%Y-%m-%d')

./stock_seeker.py \
  --stock_list="./data/stocklist_full.csv" \
  --refetch_price \
  --force_refine \
  --num_fetcher_threads="20" \
  --data_directory="./data" \
  --insight_season="$insight_season" \
  --insight_output="insight.${today}.csv"

# --force_refetch: always fetch the raw data
# --refetch_price: always fetch the latest price
# --force_refine: always refine the raw data
# --annual: fetch seasonal or annual data
