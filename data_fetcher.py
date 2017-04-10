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
flags.ArgParser().add_argument('--skip_fetching_raw_data', default=False, action='store_true',
    help='If set, totally skip fetching raw data from data sources.')
flags.ArgParser().add_argument('--force_refetch', default=False, action='store_true',
    help='If set, always refetch the raw data even if it already exists.')
flags.ArgParser().add_argument('--force_refine', default=False, action='store_true',
    help='If set, always refine the raw data even if the result already exists.')

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
    if not FLAGS.skip_fetching_raw_data:
      self._FetchFromSources(stock, data_sources)
    # process and calculate some derived data
    self._RefineData(stock)

  def _FetchFromSources(self, stock, data_sources):
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
    try:
      response = urllib2.urlopen(page_url, timeout=15)  # 15 seconds timeout
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
    refine_output = os.path.join(self._directory, '%s.refined.csv' % stock.code())
    if not FLAGS.force_refine and os.path.exists(refine_output):
      logging.info('%s exists. Skip refining %s(%s)',
          refine_output, stock.code(), stock.name())
      return

    logging.info('Refining %s(%s) ...', stock.code(), stock.name())
    seasons_in_string = [season.isoformat() for season in self._reporting_seasons]
    # {seasons_end -> {metrics -> value} }
    full_raw_data = self._LoadFullRawData(stock, self._reporting_seasons)

    # {metrics_name -> {seasons_end -> value} }
    refined_metrics_data = {}

    metrics_names_for_growth = [
        u'主营业务收入(万元)@main_metrics'.encode('UTF8'),
        u'基本每股收益(元)@main_metrics'.encode('UTF8'),
        u'净利润(万元)@main_metrics'.encode('UTF8'),
        u'经营活动产生的现金流量净额(万元)@main_metrics'.encode('UTF8'),
        u'股东权益不含少数股东权益(万元)@main_metrics'.encode('UTF8'),
    ]
    for metrics_name in metrics_names_for_growth:
      metrics_data = [full_raw_data[season].get(metrics_name)
          for season in seasons_in_string]
      size = len(metrics_data)
      growth_data = [None] * size
      for i in range(size):
        last_year = i + 4  # YoY growth
        if metrics_data[i] and last_year < size and metrics_data[last_year]:
          growth_data[i] = (metrics_data[i] - metrics_data[last_year]) / abs(metrics_data[last_year]) * 100.0
      # add to refined data
      refined_name = metrics_name.split('@')[0]
      refined_metrics_data[refined_name] = dict(zip(seasons_in_string, metrics_data))
      refined_metrics_data[refined_name + '_growth'] = dict(zip(seasons_in_string, growth_data))

    # calculate PE.
    seasonal_pe = self._CalculatePeFromEps(stock, refined_metrics_data)
    refined_metrics_data['PE'] = seasonal_pe
    seasonal_pe = self._CalculatePeFromMarketValue(stock, refined_metrics_data)
    refined_metrics_data['PE_MV'] = seasonal_pe

    # calculate PB.
    seasonal_pb = self._CalculatePbFromMarketValue(stock, refined_metrics_data)
    refined_metrics_data['PB_MV'] = seasonal_pb

    # calculate market value
    seasonal_market_value = self._GetSeasonalMarketValue(stock)
    refined_metrics_data['MV'] = seasonal_market_value

    # write csv
    metrics_column = u'指标'.encode('UTF8')
    # the columns are in this order.
    header = [metrics_column] + seasons_in_string
    writer = csv.DictWriter(open(refine_output, 'w'), fieldnames=header)
    writer.writeheader()
    for metrics_name, values in refined_metrics_data.iteritems():
      row = {metrics_column: metrics_name}
      row.update(values)
      writer.writerow(row)

  def _CalculatePeFromEps(self, stock, refined_metrics_data):
    seasonal_pe = {}
    price_column = u'收盘价'.encode('GBK')
    price_history = self._LoadAllPrices(stock, price_column)
    seasonal_price = self._GetSeasonalAveragePrice(price_history, self._reporting_seasons)
    seasonal_eps = refined_metrics_data.get(u'基本每股收益(元)'.encode('UTF8'))
    for season in self._reporting_seasons:
      season_string = season.isoformat()
      price = seasonal_price.get(season_string)
      eps = seasonal_eps.get(season_string)
      # how to convert seasonal eps to annual eps
      multiplier = {3: 4.0, 6: 2.0, 9: 4.0 / 3.0, 12: 1.0}
      if eps is not None and eps > 1e-4:
        eps *= multiplier[season.month]
      elif eps is not None:
        eps = 1e-4  # negative or 0 eps
      pe = price / eps if price and eps else None
      seasonal_pe[season_string] = pe
    return seasonal_pe

  def _CalculatePeFromMarketValue(self, stock, refined_metrics_data):
    seasonal_pe = {}
    price_column = u'总市值'.encode('GBK')
    mv_history = self._LoadAllPrices(stock, price_column)
    seasonal_mv = self._GetSeasonalAveragePrice(mv_history, self._reporting_seasons)
    seasonal_income = refined_metrics_data.get(u'净利润(万元)'.encode('UTF8'))
    for season in self._reporting_seasons:
      season_string = season.isoformat()
      price = seasonal_mv.get(season_string)
      eps = seasonal_income.get(season_string)  # note that the unit is 10K
      # how to convert seasonal eps to annual eps
      multiplier = {3: 4.0, 6: 2.0, 9: 4.0 / 3.0, 12: 1.0}
      if eps is not None and eps > 1e-4:
        eps *= multiplier[season.month]
      elif eps is not None:
        eps = 1e-4  # negative or 0 eps
      pe = price / 10000.0 / eps if price and eps else None
      if pe is not None:
        pe = min(pe, 2000)  # cap PE to 2000
      seasonal_pe[season_string] = pe
    return seasonal_pe

  def _CalculatePbFromMarketValue(self, stock, refined_metrics_data):
    seasonal_pb = {}
    price_column = u'总市值'.encode('GBK')
    mv_history = self._LoadAllPrices(stock, price_column)
    seasonal_mv = self._GetSeasonalAveragePrice(mv_history, self._reporting_seasons)
    seasonal_net_asset = refined_metrics_data.get(u'股东权益不含少数股东权益(万元)'.encode('UTF8'))
    for season in self._reporting_seasons:
      season_string = season.isoformat()
      price = seasonal_mv.get(season_string)
      asset = seasonal_net_asset.get(season_string)  # note that the unit is 10K
      if asset is not None and asset <= 1e-4:
        asset = 1.0  # negative or 0 asset

      pb = price / 10000.0 / asset if price and asset else None
      if pb is not None:
        pb = min(pb, 2000)  # cap PB to 2000
      seasonal_pb[season_string] = pb
    return seasonal_pb

  def _GetSeasonalMarketValue(self, stock):
    """ Retuns {season -> market value}. """
    mv_column = u'总市值'.encode('GBK')
    mv_history = self._LoadAllPrices(stock, mv_column)
    seasonal_mv = {}
    if len(mv_history) > 0:
      earliest_day = min(mv_history.keys())
      for season in self._reporting_seasons:
        mv = self._GetPriceOnDay(mv_history, season, earliest_day)
        seasonal_mv[season.isoformat()] = mv
    return seasonal_mv

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

  def _LoadAllPrices(self, stock, price_column):
    """ Retuns {date(string) -> price(float)}. """
    pricefile = os.path.join(self._directory, '%s.price_history.csv' % stock.code())
    reader = csv.DictReader(open(pricefile))
    date_column = u'日期'.encode('GBK')
    all_prices = {}
    for row in reader:
      all_prices[row[date_column]] = float(row[price_column])
    return all_prices

  def _GetSeasonalAveragePrice(self, all_prices, seasons):
    """ Retuns {season -> average price}. """
    seasonal_price = {}
    if len(all_prices) == 0:
      return seasonal_price

    earliest_day = min(all_prices.keys())
    for season_end in seasons:
      season_start = date_util.GetSeasonStartDate(season_end)
      day = season_start
      last_price = self._GetPriceOnDay(all_prices, day, earliest_day)
      prices = []
      while day <= season_end:
        p = all_prices.get(day.isoformat())
        if p is not None:  # only count weekdays, on weekends p is None.
          p = p if p > 1e-6 else last_price
          if p > 1e-6:
            prices.append(p)
            last_price = p
        day += datetime.timedelta(days=1)
      seasonal_price[season_end.isoformat()] = (sum(prices) / len(prices) if len(prices) else None)
    return seasonal_price

  def _GetPriceOnDay(self, all_prices, day, earliest_day):
    """ Returns the market price on a specific day. Returns the price of previous days
    if the stock does not trade on that day.
    """
    p = all_prices.get(day.isoformat(), 0.0)
    while p <= 1e-6 and day.isoformat() > earliest_day:
      day -= datetime.timedelta(days=1)  # the previous day
      p = all_prices.get(day.isoformat(), 0.0)
    return p if p > 1e-6 else 0.0


# Netease per season data fetcher
class NeteaseSeasonFetcher(NeteaseFetcher):
  def __init__(self, directory):
    super(NeteaseSeasonFetcher, self).__init__(directory)

  def _GetReportingSeasons(self):
    """ Returns a list of seasons in reserver order. E.g. [2016-09-30, 2016-06-30].
    """
    max_seasons = 12 * 4  # limit to the latest 12 years
    return [date_util.GetLastDay(d)
        for d in date_util.GetLastNSeasonsStart(datetime.date.today(), max_seasons)]

  def _SetupDataSources(self, stock):
    price_end_date = self._reporting_seasons[0].strftime('%Y%m%d')
    # The earliest reporting season is reporting_seasons[-1].
    # So we need the price since that season's start date.
    price_start_date = date_util.GetSeasonStartDate(self._reporting_seasons[-1]).strftime('%Y%m%d')
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
  # stock = Stock('600789', '鲁抗医药', '医药', '2001-01-01')
  stock = Stock('000977', '浪潮信息', '医药', '2001-01-01')
  # stock = Stock('300039', '上海凯宝', '医药', '2011-01-01')
  # stock = Stock('000621', '*ST比特', 'Unknown', '2011-01-01')
  fetcher = NeteaseSeasonFetcher(directory)
  fetcher.Fetch(stock)

if __name__ == "__main__":
  main()
