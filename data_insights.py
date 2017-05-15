#!/usr/bin/python2.7
# -*- coding: utf-8 -*-

import argparse
import bisect
import csv
import datetime
import logging
import math
import numpy
import os
import re
import sys
from scipy import stats

import flags
import date_util
import stock_info

Stock = stock_info.Stock

FLAGS = flags.FLAGS
flags.ArgParser().add_argument(
    '--insight_season',
    help='The season to do insights: YYYY-03-31, YYYY-06-30, YYYY-09-30, YYYY-12-31')


class InsightData(object):
  """ Contains {metrics_name -> value} and the order to output metrics."""
  def __init__(self):
    self._columns = []  # the data can be output in columns order
    self._data = {}

  def AddColumns(self, cols):
    for c in cols:
      assert not c in self._columns  # no duplicate columns
    self._columns += cols
    return self

  def UpdateData(self, data_map):
    for k in data_map.keys():
      assert k in self._columns  # column should be registered
    self._data.update(data_map)
    return self

  def Merge(self, other):
    self.AddColumns(other.columns())
    self.UpdateData(other.data())
    return self

  def columns(self):
    return self._columns

  def data(self):
    return self._data


# Calculate insights for each stock
class DataInsights(object):
  def __init__(self, directory):
    self._directory = directory
    # parse and check the insight season.
    self._insight_season = date_util.GetLastSeasonEndDate(datetime.date.today())
    if FLAGS.insight_season:
      self._insight_season = datetime.datetime.strptime(FLAGS.insight_season, '%Y-%m-%d').date()

  # Calculate statistical insights
  def DoStats(self, stock):
    logging.info('Insighting %s(%s) on season %s ...',
        stock.code(), stock.name(), self._insight_season.isoformat())
    # revenue from main business
    datafile = os.path.join(self._directory, '%s.refined.csv' % stock.code())
    reader = csv.DictReader(open(datafile))

    # column[1] is the latest day, columns[2:] are the seasons.
    seasons = list(reader.fieldnames)[1:]
    seasons.sort(reverse=True)  # the latest season first
    if self._insight_season.isoformat() not in seasons:
      logging.warning('%s(%s) has no data on season %s.',
          stock.code(), stock.name(), self._insight_season.isoformat())

    metrics_column_name = u'指标'.encode('UTF8')
    metrics_functions = {
        u'主营业务收入(万元)_growth'.encode('UTF8'): self._DoRevenueGrowthStats,
        u'PE_MV'.encode('UTF8'): self._DoPEStats,
        u'PB_MV'.encode('UTF8'): self._DoPBStats,
        u'MV'.encode('UTF8'): self._GetMarketValue,
    }

    # The insight data to return
    insight_data = InsightData()
    insight_data.AddColumns(['Code', 'Name', 'Industry', 'IPO', 'Season'])
    insight_data.UpdateData({
        'Season': self._insight_season.isoformat(),
        'Code': stock.code(),
        'Name': stock.name(),
        'Industry': stock.industry(),
        'IPO': stock.ipo_date(),
    })

    metrics_insight = {}  # The insight for each metrics
    for row in reader:
      metrics_name = row[metrics_column_name]
      metrics_function = metrics_functions.get(metrics_name)
      if not metrics_function:
        continue

      logging.info('Running stats for %s(%s) on %s', stock.code(), stock.name(), metrics_name)
      # list of (season, value), value could be None.
      seasonal_data = [(s, float(row[s])) if re.match(r'^-?\d+(\.\d+)?$', row[s]) else (s, None)
          for s in seasons]
      metrics_insight[metrics_name] = metrics_function(stock, seasonal_data)

    insight_order = [
        u'MV'.encode('UTF8'),
        u'主营业务收入(万元)_growth'.encode('UTF8'),
        u'PE_MV'.encode('UTF8'),
        u'PB_MV'.encode('UTF8'),
    ]
    for metrics in insight_order:
      insight = metrics_insight.get(metrics)
      if insight:
        insight_data.Merge(insight)
    logging.info('Insighting %s(%s) done.', stock.code(), stock.name())
    return insight_data

  def _GetMarketValue(self, stock, seasonal_data):
    """ seasonal_data is list of (season, value), value could be None.
    """
    insight = InsightData()
    insight.AddColumns(['MarketValue_latest', 'MarketValue_at_season'])

    season_index = self._GetInsightSeasonIndex(seasonal_data)
    if season_index < 0 or seasonal_data[season_index][1] is None:
      logging.info('No market value found for %s(%s) on %s',
          stock.code(), stock.name(), self._insight_season.isoformat())
      return insight

    def ConvertMV(mv):
      if not mv:
        return None
      if mv >= 1e8:
        mv = '%.1f亿' % (mv / 1e8)
      else:
        mv = '%.1f万' % (mv / 1e4)
      return mv

    season_mv = ConvertMV(seasonal_data[season_index][1])
    latest_mv = ConvertMV(seasonal_data[0][1])
    insight.UpdateData({
      'MarketValue_latest': latest_mv,
      'MarketValue_at_season': season_mv,
    })
    return insight

  def _DoRevenueGrowthStats(self, stock, seasonal_data):
    """ seasonal_data is list of (season, value)
    """
    insight = InsightData()
    insight.AddColumns(['revenue_growth_at_season'])
    periods_configs = [12]  # number of pervious seasons to consider
    for num in periods_configs:
      insight.AddColumns([
        '%dseasons_revenue_growth_mean' % num,
        '%dseasons_revenue_growth_lower' % num,
        '%dseasons_revenue_growth_upper' % num,
        '%dseasons_revenue_growth_quantile' % num,
      ])

    season_index = self._GetInsightSeasonIndex(seasonal_data)
    if season_index < 0 or seasonal_data[season_index][1] is None:
      logging.info('No revenue growth found for %s(%s) on %s',
          stock.code(), stock.name(), self._insight_season.isoformat())
      return insight

    data_at_season = seasonal_data[season_index][1]
    insight.UpdateData({'revenue_growth_at_season': round(data_at_season, 2)})

    for num in periods_configs:
      previous_seasons = [v[1] for v in seasonal_data[season_index + 1 : season_index + 1 + num] if v[1]]
      if len(previous_seasons) < num:
        logging.info('%s(%s) has no enough data for %d seasons revenue growth CI before %s',
            stock.code(), stock.name(), num, self._insight_season.isoformat())
      else:
        # CI by the previous seasons data
        (mean, lower, upper, quantile) = self._PastAverageAndPercentInPast(
            data_at_season, previous_seasons)
        insight.UpdateData({
          '%dseasons_revenue_growth_mean' % num: round(mean, 2),
          '%dseasons_revenue_growth_lower' % num: round(lower, 2),
          '%dseasons_revenue_growth_upper' % num: round(upper, 2),
          '%dseasons_revenue_growth_quantile' % num: round(quantile, 1),
        })

    return insight

  def _DoPEStats(self, stock, seasonal_data):
    """ seasonal_data is list of (season, value)
    """
    insight = InsightData()
    insight.AddColumns(['PE_latest', 'PE_at_season'])
    periods_configs = [12]  # number of pervious seasons to consider
    for num in periods_configs:
      insight.AddColumns([
        '%dseasons_PE_mean' % num,
        '%dseasons_PE_lower' % num,
        '%dseasons_PE_upper' % num,
        '%dseasons_PE_quantile' % num,
      ])

    season_index = self._GetInsightSeasonIndex(seasonal_data)
    if season_index < 0 or seasonal_data[season_index][1] is None:
      logging.info('No PE found for %s(%s) on %s',
          stock.code(), stock.name(), self._insight_season.isoformat())
      return insight

    pe_at_season = seasonal_data[season_index][1]
    pe_latest = seasonal_data[0][1]
    insight.UpdateData({
      'PE_latest': round(pe_latest, 1),
      'PE_at_season': round(pe_at_season, 1),
    })

    for num in periods_configs:
      previous_seasons = [v[1] for v in seasonal_data[season_index + 1 : season_index + 1 + num] if v[1]]
      if len(previous_seasons) < num:
        logging.info('%s(%s) has no enough data for %d seasons PE CI before %s',
            stock.code(), stock.name(), num, self._insight_season.isoformat())
      else:
        # CI by previous seasons data
        (mean, lower, upper, quantile) = self._PastAverageAndPercentInPast(
            pe_at_season, previous_seasons)
        insight.UpdateData({
          '%dseasons_PE_mean' % num: round(mean, 1),
          '%dseasons_PE_lower' % num: round(lower, 1),
          '%dseasons_PE_upper' % num: round(upper, 1),
          '%dseasons_PE_quantile' % num: round(quantile, 1),
        })

    return insight

  def _DoPBStats(self, stock, seasonal_data):
    """ seasonal_data is list of (season, value)
    """
    insight = InsightData()
    insight.AddColumns(['PB_latest', 'PB_at_season'])
    periods_configs = [12]  # number of pervious seasons to consider
    for num in periods_configs:
      insight.AddColumns([
        '%dseasons_PB_mean' % num,
        '%dseasons_PB_lower' % num,
        '%dseasons_PB_upper' % num,
        '%dseasons_PB_quantile' % num,
      ])

    season_index = self._GetInsightSeasonIndex(seasonal_data)
    if season_index < 0 or seasonal_data[season_index][1] is None:
      logging.info('No PB found for %s(%s) on %s',
          stock.code(), stock.name(), self._insight_season.isoformat())
      return insight

    pb_at_season = seasonal_data[season_index][1]
    pb_latest = seasonal_data[0][1]
    insight.UpdateData({
      'PB_latest': round(pb_latest, 1),
      'PB_at_season': round(pb_at_season, 1),
    })

    for num in periods_configs:
      previous_seasons = [v[1] for v in seasonal_data[season_index + 1 : season_index + 1 + num] if v[1]]
      if len(previous_seasons) < num:
        logging.info('%s(%s) has no enough data for %d seasons PB CI before %s',
            stock.code(), stock.name(), num, self._insight_season.isoformat())
      else:
        # CI by previous seasons data
        (mean, lower, upper, quantile) = self._PastAverageAndPercentInPast(
            pb_at_season, previous_seasons)
        insight.UpdateData({
          '%dseasons_PB_mean' % num: round(mean, 1),
          '%dseasons_PB_lower' % num: round(lower, 1),
          '%dseasons_PB_upper' % num: round(upper, 1),
          '%dseasons_PB_quantile' % num: round(quantile, 1),
        })

    return insight

  def _PastAverageAndPercentInPast(self, season_metrics, past_metrics):
    """ Returns tuple of past average stats with CI and the season's quantile
    if put in the past. (mean, lower, upper, quantile_in_past).
    """
    quantile = None  # by default
    mean = lower = upper = None

    refined_data = [d if d else 0.0 for d in past_metrics]
    size = len(refined_data)
    mean = sum(refined_data) / float(size)
    # avg(x^2)
    square_mean = sum([i ** 2 for i in refined_data]) / float(size)
    s = math.sqrt((square_mean - mean ** 2) * float(size) / float(size - 1))

    # assume subject to t distribution, df=size-1, 95% confidence
    t = stats.t.ppf(0.975, size - 1)
    lower = mean - t * s / math.sqrt(size)
    upper = mean + t * s / math.sqrt(size)

    # s can be 0 when past_metrics are const.
    point = (season_metrics - mean) * 100.0
    if abs(s) > 1e-6:
      point = (season_metrics - mean) / (s / math.sqrt(size))
    quantile = stats.t.cdf(point, size - 1) * 100.0
    return (mean, lower, upper, quantile)

  def _GetInsightSeasonIndex(self, seasons):
    if len(seasons) == 0:
      return -1
    # Season string format YYYY-MM-DD.
    insight_season = self._insight_season.isoformat()
    # assume seasons are in descending order.
    start = 0
    end = len(seasons) - 1
    while start != end:
      mid = (start + end) / 2
      if insight_season < seasons[mid][0]:
        start = mid + 1
      else:
        end = mid
    if insight_season == seasons[start][0]:
      return start
    return -1


def main():
  # Parse command line flags into FLAGS.
  flags.ArgParser().parse_args(namespace=FLAGS)

  logging.basicConfig(level=logging.INFO)
  directory = './data/test'
  stock = Stock('000977', '浪潮信息', '医药', '2001-01-01')
  insighter = DataInsights(directory)
  insight = insighter.DoStats(stock)

  header = insight.columns()
  writer = csv.DictWriter(sys.stdout, fieldnames=header)
  writer.writeheader()
  writer.writerow(insight.data())

if __name__ == "__main__":
  main()
