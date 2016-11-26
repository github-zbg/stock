#!/usr/bin/python2.7
# -*- coding: utf-8 -*-

import argparse
import csv
import datetime
import logging
import os
import re
import sys
import urllib2

import flags
import date_util
import stock_info

FLAGS = flags.FLAGS
flags.ArgParser().add_argument('--skip_fetching_data', default=False, action='store_true',
    help='If set, totally skip fetching raw data from url.')
flags.ArgParser().add_argument('--force_refetch', default=False, action='store_true',
    help='If set, always refetch the data even if it already exists.')

Stock = stock_info.Stock


# The base class
class DataFetcher(object):
  def __init__(self, directory):
    self._directory = directory

  # Fetch data of a given stock from sources
  def Fetch(self, stock):
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
      'price_history',      # The price history page pattern
  ]

  def __init__(self, directory):
    super(NeteaseFetcher, self).__init__(directory)
    # this is the next seasons' start date.
    self._reporting_seasons = self._GetReportingSeasons()

  def _GetReportingSeasons(self):
    """ Returns a list of reporting seasons that we are interested in.
    must be overridden.
    """
    assert False, 'Must override _GetReportingSeasons';

  def _SetupDataSources(self, stock):
    """ Setup the real urls of data sources.
    must be overridden.
    """
    assert False, 'Must override _SetupDataSources.';

  def Fetch(self, stock):
    # setup the sources of a certain stock
    data_sources = self._SetupDataSources(stock)
    # fetch the raw data
    self._FetchFromSources(stock, data_sources)
    # process and calculate some derived data
    self._RefineData(stock)

  def _FetchFromSources(self, stock, data_sources):
    if FLAGS.skip_fetching_data:
      return
    logging.info('Fetching %s(%s) ...', stock.code(), stock.name())
    for page_name in self._data_pages:
      page_url = data_sources.get(page_name)
      assert page_url
      self._FetchUrl(stock, page_name, page_url)

  def _FetchUrl(self, stock, page_name, page_url):
    filename = '%s.%s.csv' % (stock.code(), page_name)
    full_filepath = os.path.join(self._directory, filename)
    if not FLAGS.force_refetch and os.path.exists(full_filepath):
      logging.info('%s exists. Skip fetching %s for %s(%s)',
          full_filepath, page_name, stock.code(), stock.name())
      return

    logging.info('Fetching %s for %s(%s) at %s',
        page_name, stock.code(), stock.name(), page_url)
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

  def _RefineData(self, stock):
    logging.info('Refining %s(%s) ...', stock.code(), stock.name())
    seasons_end = [date_util.GetLastDay(d) for d in self._reporting_seasons]
    # {seasons_end -> {metrics -> value} }
    full_raw_data = self._LoadFullRawData(stock, seasons_end)
    seasons = full_raw_data.keys()
    seasons.sort(reverse=True)  # from the latest season

    metrics_names = [
        u'主营业务收入(万元)@main_metrics'.encode('UTF8'),
        # u'经营活动产生的现金流量净额(万元)',
    ]

    refined_metrics_data = []  # [metrics_name, value for each season]
    for metrics_name in metrics_names:
      metrics_data = [full_raw_data[season].get(metrics_name) for season in seasons]
      size = len(metrics_data)
      growth_data = [None] * size
      for i in range(size):
        last_year = i + 4  # YoY growth
        if metrics_data[i] and last_year < size and metrics_data[last_year]:
          growth_data[i] = (metrics_data[i] - metrics_data[last_year]) / abs(metrics_data[last_year]) * 100.0
      # append row to refined data
      refined_name = metrics_name.split('@')[0]
      refined_metrics_data.append([refined_name] + metrics_data)
      refined_metrics_data.append([refined_name + '_growth'] + growth_data)

    writer = csv.writer(open(os.path.join(self._directory, '%s.refined.csv' % stock.code()), 'w'))
    writer.writerow([u'指标'.encode('UTF8')] + seasons)  # header
    writer.writerows(refined_metrics_data)

  def _LoadFullRawData(self, stock, seasons_end):
    full_data = {}  # {seasons_end -> {metrics -> value} }
    for page in self._data_pages:
      if page != 'price_history':
        self._LoadPage(page, stock, seasons_end, full_data)
    return full_data

  def _LoadPage(self, page, stock, seasons_end, full_data):
    datafile = os.path.join(self._directory, '%s.%s.csv' % (stock.code(), page))
    reader = csv.DictReader(open(datafile))
    # The first column is metrics name.
    metrics_column_name_gbk = list(reader.fieldnames)[0]
    for row in reader:
      metrics_name = row[metrics_column_name_gbk]
      # append "page" to differentiate metrics in different pages.
      metrics_name = '%s@%s' % (metrics_name.decode('GBK').encode('UTF8'), page)
      for season in seasons_end:
        season_string = season.isoformat()
        per_season_data = full_data.setdefault(season_string, {})
        value = None
        value_string = row.get(season_string)
        if value_string and re.match(r'^-?\d+(\.\d+)?$', value_string):
          value = float(value_string)
        per_season_data.update({metrics_name: value})


# Netease per season data fetcher
class NeteaseSeasonFetcher(NeteaseFetcher):
  def __init__(self, directory):
    super(NeteaseSeasonFetcher, self).__init__(directory)

  def _GetReportingSeasons(self):
    """ Returns a list of seasons in reserver order. E.g. [2016-10-01, 2016-07-01].
    """
    max_seasons = 12 * 4  # limit to the latest 12 years
    return date_util.GetLastNSeasonsStart(datetime.date.today(), max_seasons)

  def _SetupDataSources(self, stock):
    price_end_date = self._reporting_seasons[0].strftime('%Y%m%d')
    # The earliest reporting season is the previous day of reporting_seasons[-1].
    # So we need the price since that season's start date.
    price_start_date = date_util.GetSeasonStartDate(
        date_util.GetLastDay(self._reporting_seasons[-1])).strftime('%Y%m%d')
    # the stock code in the price history url should be tranformed.
    code = '0%s' % stock.code() if stock.code().startswith('6') else '1%s' % stock.code()
    return {
        'balance': ('http://quotes.money.163.com/service/zcfzb_%s.html' % stock.code()),
        'income': ('http://quotes.money.163.com/service/lrb_%s.html' % stock.code()),
        'cash': ('http://quotes.money.163.com/service/xjllb_%s.html' % stock.code()),
        # the 'report' type is seasonal data
        'main_metrics': ('http://quotes.money.163.com/service/zycwzb_%s.html?type=report' % stock.code()),
        'profit_metrics': ('http://quotes.money.163.com/service/zycwzb_%s.html?type=report&part=ylnl' % stock.code()),
        'liability_metrics': ('http://quotes.money.163.com/service/zycwzb_%s.html?type=report&part=chnl' % stock.code()),
        'growth_metrics': ('http://quotes.money.163.com/service/zycwzb_%s.html?type=report&part=cznl' % stock.code()),
        'operating_metrics': ('http://quotes.money.163.com/service/zycwzb_%s.html?type=report&part=yynl' % stock.code()),
        # the price fields are: close price, total value and market value.
        'price_history': ('http://quotes.money.163.com/service/chddata.html' +
            '?code=%s&start=%s&end=%s&fields=TCLOSE;TCAP;MCAP' % (code, price_start_date, price_end_date)),
    }


def main():
  # Parse command line flags into FLAGS.
  flags.ArgParser().parse_args(namespace=FLAGS)

  logging.basicConfig(level=logging.INFO)
  directory = './data/test'
  stock = Stock('000977', '浪潮信息')
  fetcher = NeteaseSeasonFetcher(directory)
  fetcher.Fetch(stock)

if __name__ == "__main__":
  main()
