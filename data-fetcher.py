#!/usr/bin/python2.7
# -*- coding: utf-8 -*-

import csv;
import os;
import re;
import sys;
import urllib2;

# The base class
class DataFetcher(object):
  def __init__(self, directory):
    self._directory = directory

  # Fetch data of a given stock code from sources
  def Fetch(self, stock_code):
    pass


# The base of Netease data fetcher
class NeteaseFetcher(DataFetcher):
  _data_pages = [
      'balance',  # The balance sheet page pattern
      'income',   # The income sheet page pattern
      'cash',     # The cashflow sheet page pattern
      'main_metrics',       # The main metrics page pattern
      'profit_metrics',     # The profitable metrics page pattern
      'liability_metrics',  # The liability metrics page pattern
      'growth_metrics',     # The growth metrics page pattern
      'operating_metrics',  # The operating metrics page pattern
      # 'price_history',      # The price history page pattern
  ]

  def __init__(self, directory):
    super(NeteaseFetcher, self).__init__(directory)

  def Fetch(self, stock_code):
    # setup the sources of a certain stock
    data_sources = self._SetupDataSources(stock_code)
    # fetch the raw data
    self._FetchFromSources(stock_code, data_sources)
    # process and calculate some derived data
    self._RefineData()

  def _FetchFromSources(self, stock_code, data_sources):
    for page_name in self._data_pages:
      page_url = data_sources.get(page_name)
      assert page_url
      self._FetchUrl(stock_code, page_name, page_url)

  def _FetchUrl(self, stock_code, page_name, page_url):
    print 'Fetching', page_name, 'for', stock_code, 'at', page_url
    response = urllib2.urlopen(page_url)
    content = response.read()

    filename = '%s.%s.csv' % (stock_code, page_name)
    print 'Saving', page_name, 'to', filename
    f = open(os.path.join(self._directory, filename), 'w')
    f.write(content)
    f.close()

  def _SetupDataSources(self, stock_code):
    return {}

  def _RefineData(self):
    pass


# Netease per season data fetcher
class NeteaseSeasonFetcher(NeteaseFetcher):
  def __init__(self, directory):
    super(NeteaseSeasonFetcher, self).__init__(directory)

  def _SetupDataSources(self, stock_code):
    return {
        'balance': ('http://quotes.money.163.com/service/zcfzb_%s.html' % stock_code),
        'income': ('http://quotes.money.163.com/service/lrb_%s.html' % stock_code),
        'cash': ('http://quotes.money.163.com/service/xjllb_%s.html' % stock_code),
        'main_metrics': ('http://quotes.money.163.com/service/zycwzb_%s.html?type=season' % stock_code),
        'profit_metrics': ('http://quotes.money.163.com/service/zycwzb_%s.html?type=season&part=ylnl' % stock_code),
        'liability_metrics': ('http://quotes.money.163.com/service/zycwzb_%s.html?type=season&part=chnl' % stock_code),
        'growth_metrics': ('http://quotes.money.163.com/service/zycwzb_%s.html?type=season&part=cznl' % stock_code),
        'operating_metrics': ('http://quotes.money.163.com/service/zycwzb_%s.html?type=season&part=yynl' % stock_code),
        'price_history': (''),
    }


def main():
  directory = './data/test'
  stock_code = '000977'
  fetcher = NeteaseSeasonFetcher(directory)
  print 'Fetching', stock_code
  fetcher.Fetch(stock_code)

if __name__ == "__main__":
  main()
