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

  # get season or year
  def _season_month(month):
    # 1,2,3 -> 1
    # 4,5,6 -> 4
    # 7,8,9 -> 7
    # 10,11,12 -> 10
    return (month - 1) / 3 * 3 + 1

  today = datetime.date.today()
  start_date = datetime.date(today.year, _season_month(today.month), 1)
  if FLAGS.annual:
    start_date = datetime.date(today.year, 1, 1)

  full_path = os.path.join(abs_base, sub_dir, start_date.isoformat())
  if not os.path.exists(full_path):
    os.makedirs(full_path, 0755)  # create recursively

  return full_path


def _GetHeader(column_map):
  # special columns in order
  special = ['Code', 'Season']
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
  directory = _GetDataDirectory()
  logging.info('Data directory: %s', directory)

  fetcher = data_fetcher.NeteaseSeasonFetcher(directory)
  stock_code_list = [
      '000977',  # 浪潮信息
      '002241',  # 歌尔股份
      '002508',  # 老板电器
      '002007',  # 华兰生物
      '000651',  # 格力电器
  ]

  logging.info('Start batch data fetching')
  batch = batch_data_fetcher.BatchDataFetcher(fetcher, FLAGS.num_fetcher_threads)
  batch.Fetch(stock_code_list)
  logging.info('Batch data fetching completed')

  logging.info('Start data insighs')
  insighter = data_insights.DataInsights(directory)
  row_of_insights = []
  for stock_code in stock_code_list:
    row_of_insights.append(insighter.DoStats(stock_code))

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