#!/usr/bin/python2.7
# -*- coding: utf-8 -*-

import csv;
import os;
import re;
import sys;
import urllib2;

# The base class
class DataFetcher:
  def __init__(self, directory):
    self.__directory = directory

  # Fetch data of a given stock code from sources
  def Fetch(self, stock_code):


# The base of Netease data fetcher
class NeteaseFetcher(DataFetcher):
  _balance_page = ''  # The balance sheet page pattern
  _income_page = ''   # The income sheet page pattern
  _cash_page = ''     # The cashflow sheet page pattern
  _main_metrics_page = ''  # The main metrics page pattern
  _profit_metrics_page = ''  # The profitable metrics page pattern
  _liability_metrics_page = ''  # The liability metrics page pattern
  _growth_metrics_page = ''  # The growth metrics page pattern
  _operating_metrics_page = ''  # The operating metrics page pattern
  _price_history_page = ''   # The price history page pattern

# BALANCE_PATTERN = 'http://quotes.money.163.com/service/zycwzb_%s.html?type=year'
# PROFIT_PATTERN = 'http://quotes.money.163.com/service/zycwzb_%s.html?type=year&part=ylnl'

  def Fetch(self, stock_code):
    # setup the sources of a certain stock
    data_sources = _SetupDataSources(stock_code)
    _FetchFromSources(data_sources)

    print 'Fetching', code, name
    response = urllib2.urlopen(BALANCE_PATTERN % code)
    content = response.read()

    filename = '%s.%s.csv' % (code, name)
    print 'Saving', filename
    f = open(filename, 'w')
    f.write(content)
    f.close()


def main():
  assert len(sys.argv) > 1, 'Company list is required'
  print 'Reading company list from ', sys.argv[1]
  reader = csv.reader(open(sys.argv[1], 'r'))

  success = 0
  total = 0
  for num, code, name in reader:
    total += 1
    # extract stock code
    m = re.match(r'^\w\w(\d+)$', code)
    if m is None:
      print 'Stock code is invalid:', code
      continue
    code = m.group(1)
    # decode stock name
    name = name.decode('GBK').encode('utf-8')
    FetchData(code, name)
    success += 1

  print 'Total:', total, 'success:', success


if __name__ == "__main__":
  main()
