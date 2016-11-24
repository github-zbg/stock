#!/usr/bin/python2.7
# -*- coding: utf-8 -*-

# The main controller of the whole workflow.

import argparse
import csv
import datetime
import logging
import os
import sys

import flags
import batch_data_fetcher
import data_fetcher
import data_insights
import date_util
import stock_info

FLAGS = flags.FLAGS
flags.ArgParser().add_argument('--data_directory', default='./data',
    help='The base directory of output.')
flags.ArgParser().add_argument('--annual', default=False, action='store_true',
    help='Whether to run seasonal(default) or annual data.')
flags.ArgParser().add_argument('--insight_output', default=None,
    help='The output of insight data.')


def _GetDataDirectory():
  abs_base = FLAGS.data_directory
  if not os.path.isabs(FLAGS.data_directory):
    abs_base = os.path.abspath(FLAGS.data_directory)

  sub_dir = 'annual' if FLAGS.annual else 'seasonal'

  today = datetime.date.today()
  start_date = date_util.GetSeasonStartDate(today)
  if FLAGS.annual:
    start_date = datetime.date(today.year, 1, 1)

  full_path = os.path.join(abs_base, sub_dir, start_date.isoformat())
  if not os.path.exists(full_path):
    os.makedirs(full_path, 0755)  # create recursively

  return full_path


def _GetHeader(column_map):
  # special columns in order
  special = ['Code', 'Name', 'Season']
  special_set = set(special)

  header = []
  for col in special:
    if col in column_map:
      header.append(col)

  sorted_keys = column_map.keys()
  sorted_keys.sort()  # ordered
  for k in sorted_keys:
    if k not in special_set:
      header.append(k)

  return header


def RunData():
  # load a map of {code -> stock}
  stocks = stock_info.LoadAllStocks()
  # randomly pick 10 stocks
  stock_list = [stocks[c] for c in stocks.keys()[:10] ]

  directory = _GetDataDirectory()
  logging.info('Data directory: %s', directory)

  logging.info('Start batch data fetching')
  fetcher = data_fetcher.NeteaseSeasonFetcher(directory)
  batch = batch_data_fetcher.BatchDataFetcher(fetcher, FLAGS.num_fetcher_threads)
  batch.Fetch(stock_list)
  logging.info('Batch data fetching completed')

  logging.info('Start data insighs')
  insighter = data_insights.DataInsights(directory)
  row_of_insights = []
  for stock in stock_list:
    row_of_insights.append(insighter.DoStats(stock))

  # output insighs
  if len(row_of_insights) > 0:
    outfile = sys.stdout  # output to stdout by default
    if FLAGS.insight_output:
      outfile = open(os.path.join(directory, FLAGS.insight_output), 'w')

    header = _GetHeader(row_of_insights[0])
    writer = csv.DictWriter(outfile, fieldnames=header)
    writer.writeheader()
    writer.writerows(row_of_insights)


def main():
  # Parse command line flags into FLAGS.
  flags.ArgParser().parse_args(namespace=FLAGS)
  # Set logging level
  logging.basicConfig(level=logging.INFO)
  # Run
  RunData()

if __name__ == "__main__":
  main()
