#!/usr/bin/bash

insight_date="$1"

./stock_seeker.py \
  --stock_list="~/stocklist.full.csv" \
  --num_fetcher_threads="20" \
  --data_directory="./data" \
  --insight_date="$insight_date" \
  --insight_output="insight.${insight_date}.csv"

# --skip_fetching_raw_data: no fetching raw data from data sources
# --force_refetch: always fetch the raw data
# --force_refine: always refine the raw data
# --annual: fetch seasonal or annual data
