#!/usr/bin/python2.7
# -*- coding: utf-8 -*-

"""Generate all stocklist based on the csv downloaded."""

import argparse
import csv
import datetime
import logging
import sys

import flags

FLAGS = flags.FLAGS
flags.ArgParser().add_argument(
    '--input_csv',
    default='data/stocklist_shanghai.csv,data/stocklist_shenzhen.csv',
    help='The input csv files to generate the final stocklist.')
flags.ArgParser().add_argument(
    '--output_csv',
    default='data/stocklist_full.csv',
    help='The output csv.')


def Generate(input_csv_list, output_csv):
  # The mapping from input column to output column
  mapping = {
      u'A股代码'.encode('gbk'): u'A股代码'.encode('utf8'),
      u'A股简称'.encode('gbk'): u'A股简称'.encode('utf8'),
      u'A股上市日期'.encode('gbk'): u'上市日期'.encode('utf8'),
      u'所属行业'.encode('gbk'): u'2012年行业名称'.encode('utf8'),
  }
  output_rows = []
  for input_csv in input_csv_list:
    logging.info('Loading %s ...', input_csv)
    reader = csv.DictReader(open(input_csv))
    for row in reader:
      output = {}
      for input_column, output_column in mapping.iteritems():
        value = row.get(input_column)
        if value:
          output_value = value.decode('gbk').encode('utf8').strip()
          output[output_column] = output_value
      if output:
        output_rows.append(output)
  # output
  logging.info('Writing %s with %d stocks', output_csv, len(output_rows))
  header = [
      u'A股代码'.encode('utf8'),
      u'A股简称'.encode('utf8'),
      u'上市日期'.encode('utf8'),
      u'2012年行业名称'.encode('utf8'),
  ]
  writer = csv.DictWriter(open(output_csv, 'w'), fieldnames=header)
  writer.writeheader()
  writer.writerows(output_rows)


def main():
  # Parse command line flags into FLAGS.
  flags.ArgParser().parse_args(namespace=FLAGS)

  logging.basicConfig(level=logging.INFO)

  input_csv = FLAGS.input_csv.split(',')
  output_csv = FLAGS.output_csv

  if not input_csv or not output_csv:
    logging.fatal('--input_csv and --output_csv are required.')

  Generate(input_csv, output_csv)

if __name__ == "__main__":
  main()
