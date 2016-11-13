#!/usr/bin/python2.7
# -*- coding: utf-8 -*-

import argparse
import csv
import logging
import os
import re
import sys
import urllib2

argument_parser = argparse.ArgumentParser()
argument_parser.add_argument('--skip_fetching_data', default=False, action='store_true',
    help='Whether to totally skip fetching raw data from url.')
argument_parser.add_argument('--force_refetch', default=False, action='store_true',
    help='If true, always refetch the data even if it already exists.')

FLAGS = argument_parser.parse_args()


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
    self._RefineData(stock_code)

  def _FetchFromSources(self, stock_code, data_sources):
    if FLAGS.skip_fetching_data:
      return
    logging.info('Fetching %s ...', stock_code)
    for page_name in self._data_pages:
      page_url = data_sources.get(page_name)
      assert page_url
      self._FetchUrl(stock_code, page_name, page_url)

  def _FetchUrl(self, stock_code, page_name, page_url):
    filename = '%s.%s.csv' % (stock_code, page_name)
    full_filepath = os.path.join(self._directory, filename)
    if not FLAGS.force_refetch and os.path.exists(full_filepath):
      logging.info('%s exists. Skip fetching %s for %s', full_filepath, page_name, stock_code)
      return

    logging.info('Fetching %s for %s at %s', page_name, stock_code, page_url)
    response = urllib2.urlopen(page_url, timeout=15)  # 15 seconds timeout
    try:
      content = response.read()
    except URLError, e:
      if hasattr(e, 'code'):  # HTTPError
        logging.error('Http error %d for url: %s', e.code, page_url)
      elif hasattr(e, 'reason'):
        logging.error('Url error: %s, with reason %s', page_url, str(e.reason))
      return

    logging.info('Saving %s to %s', page_name, filename)
    f = open(full_filepath, 'w')
    f.write(content)
    f.close()

  def _SetupDataSources(self, stock_code):
    return {}

  def _RefineData(self, stock_code):
    logging.info('Refining %s ...', stock_code)
    max_seasons = 12 * 4  # limit to the latest 12 years
    # {season -> {metrics -> value} }
    full_raw_data = self._LoadFullRawData(stock_code, max_seasons)
    seasons = full_raw_data.keys()
    seasons.sort(reverse=True)  # from the latest season

    metrics_names = [
        u'主营业务收入(万元)@main_metrics',
        # u'经营活动产生的现金流量净额(万元)',
    ]

    refined_metrics_data = []  # [metrics_name, value for each season]
    for metrics_name in metrics_names:
      metrics_name = metrics_name.encode('UTF8')
      metrics_data = [full_raw_data[s].get(metrics_name) for s in seasons]
      size = len(metrics_data)
      growth_data = [None] * size
      for i in range(size):
        last_year = i + 4
        if metrics_data[i] and last_year < size and metrics_data[last_year]:
          growth_data[i] = (metrics_data[i] - metrics_data[last_year]) / abs(metrics_data[last_year]) * 100.0
      # append row to refined data
      refined_name = metrics_name.split('@')[0]
      refined_metrics_data.append([refined_name] + metrics_data)
      refined_metrics_data.append([refined_name + '_growth'] + growth_data)

    writer = csv.writer(open(os.path.join(self._directory, '%s.refined.csv' % stock_code), 'w'))
    writer.writerow([u'指标'.encode('UTF8')] + seasons)  # header
    writer.writerows(refined_metrics_data)

  def _LoadFullRawData(self, stock_code, max_seasons):
    full_data = {}  # {season -> {metrics -> value} }
    for page in self._data_pages:
      self._LoadPage(page, stock_code, max_seasons, full_data)
    return full_data

  def _LoadPage(self, page, stock_code, max_seasons, full_data):
    datafile = os.path.join(self._directory, '%s.%s.csv' % (stock_code, page))
    reader = csv.DictReader(open(datafile))
    seasons = list(reader.fieldnames)[1:]  # keep only the seasons
    seasons = [s for s in seasons if len(s.strip()) > 0]
    seasons.sort(reverse=True)  # from the latest season
    if len(seasons) > max_seasons:
      seasons = seasons[:max_seasons]
    # netease use gbk
    metrics_column_name_gbk = list(reader.fieldnames)[0]
    for row in reader:
      metrics_name = row[metrics_column_name_gbk]
      # differentiate metrics in different pages.
      metrics_name = '%s@%s' % (metrics_name.decode('GBK').encode('UTF8'), page)
      for season in seasons:
        per_season_data = full_data.setdefault(season, {})
        value = None
        value_string = row.get(season)
        if value_string and re.match(r'^-?\d+(\.\d+)?$', value_string):
          value = float(value_string)
        per_season_data.update({metrics_name: value})


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
  logging.basicConfig(level=logging.INFO)
  directory = './data/test'
  stock_code = '000977'
  fetcher = NeteaseSeasonFetcher(directory)
  fetcher.Fetch(stock_code)

if __name__ == "__main__":
  main()
