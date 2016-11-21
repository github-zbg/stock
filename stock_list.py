#!/usr/bin/python2.7
# -*- coding: utf-8 -*-

import argparse
import csv
import logging
import os
import re
import sys

import flags

FLAGS = flags.FLAGS
flags.ArgParser().add_argument(
    '--stock_list',
    default='',
    help='Comma separated stock list files.')


class Stock(object):
  def __init__(self, code, name):
    self.__code = code
    self.__name = name

  def code(self):
    return self.__code

  def name(self):
    return self.__name


def LoadAllStocks():
  """ Returns a map of {code -> Stock}.
  """
  code_column = u'A股代码'.encode('GBK')
  name_column = u'A股简称'.encode('GBK')
  all_stocks = {}
  for stock_file in FLAGS.stock_list.split(','):
    stock_file = stock_file.strip()
    if stock_file.startswith('~'):
      stock_file = os.path.expanduser(stock_file)

    reader = csv.DictReader(open(stock_file))
    for row in reader:
      code = row.get(code_column, '')
      name = row.get(name_column, '')
      code = code.strip().decode('GBK').encode('UTF8')
      name = name.strip().decode('GBK').encode('UTF8')
      if not code or not name:
        continue
      # logging.info('stock: "%s", name: "%s"', code, name)
      stock = Stock(code, name)
      all_stocks[code] = stock

  logging.info('Load %d stocks in all.', len(all_stocks))
  return all_stocks


def main():
  # Parse command line flags into FLAGS.
  flags.ArgParser().parse_args(namespace=FLAGS)
  logging.basicConfig(level=logging.INFO)

  LoadAllStocks()

if __name__ == "__main__":
  main()
