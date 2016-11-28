#!/usr/bin/python2.7
# -*- coding: utf-8 -*-

import argparse
import bisect
import csv
import datetime
import logging
import math
import os
import re
import sys

import flags
import date_util
import stock_info

Stock = stock_info.Stock

FLAGS = flags.FLAGS
flags.ArgParser().add_argument(
    '--insight_date',
    default=date_util.GetLastSeasonEndDate(datetime.date.today()).isoformat(),
    help='The time to do insights: YYYY-03-31, YYYY-06-30, YYYY-09-30, YYYY-12-31.')


# Calculate insights for each stock
class DataInsights(object):
  def __init__(self, directory):
    self._directory = directory
    # parse and check the insight date.
    self._insight_date = datetime.datetime.strptime(FLAGS.insight_date, '%Y-%m-%d').date()

  # Calculate statistical insights
  def DoStats(self, stock):
    logging.info('Insighting %s(%s) on season %s ...',
        stock.code(), stock.name(), self._insight_date.isoformat())
    # revenue from main business
    datafile = os.path.join(self._directory, '%s.refined.csv' % stock.code())
    reader = csv.DictReader(open(datafile))

    seasons = list(reader.fieldnames)[1:]  # keep only the dates
    seasons.sort(reverse=True)  # the latest season first
    if self._insight_date.isoformat() not in seasons:
      logging.warning('%s(%s) has no data on season %s.',
          stock.code(), stock.name(), self._insight_date.isoformat())

    metrics_column_name = u'指标'.encode('UTF8')
    metrics_functions = {
        u'主营业务收入(万元)_growth'.encode('UTF8'): self._DoRevenueGrowthStats,
        u'PE'.encode('UTF8'): self._DoPEStats,
    }

    # The data to return: column -> value
    insight_data = {
        'Season': self._insight_date.isoformat(),
        'Code': stock.code(),
        'Name': stock.name(),
    }
    for row in reader:
      metrics_name = row[metrics_column_name]
      metrics_function = metrics_functions.get(metrics_name)
      if not metrics_function:
        continue

      if not row[self._insight_date.isoformat()]:
        logging.warning('%s(%s) has no data on season %s for insighting %s.',
            stock.code(), stock.name(), self._insight_date.isoformat(), metrics_name)
        continue

      logging.info('Running stats for %s(%s) on %s', stock.code(), stock.name(), metrics_name)
      # list of (season, value)
      seasonal_data = [(s, float(row[s])) if re.match(r'^-?\d+(\.\d+)?$', row[s]) else (s, None)
          for s in seasons]
      stats = metrics_function(stock, seasonal_data)
      insight_data.update(stats)

    logging.info('Insighting %s(%s) done.', stock.code(), stock.name())
    return insight_data

  def _DoRevenueGrowthStats(self, stock, seasonal_data):
    """ seasonal_data is list of (season, value)
    """
    season_index = self._GetInsightDateIndex(seasonal_data)
    if season_index < 0:
      return result

    # number of pervious seasons to consider and t-dist constants
    # list of (num_of_perious_seasons, t-dist)
    periods_configs = [
      # 8 seasons
      # (8, [(0.99, 3.4995), (0.98, 2.9980), (0.95, 2.3646)]),
      # 12 seasons
      (12, [(0.99, 3.1058), (0.98, 2.7181), (0.95, 2.2010)]),
    ]

    # stats column -> value
    result = {
        'revenue_growth_at_season': None,
    }

    data_at_season = seasonal_data[season_index][1]
    result['revenue_growth_at_season'] = round(data_at_season, 2)

    for num, t_list in periods_configs:
      result.update({
        '%dseasons_revenue_growth_mean' % num: None,
        '%dseasons_revenue_growth_lower' % num: None,
        '%dseasons_revenue_growth_upper' % num: None,
        '%dseasons_revenue_growth_quantile' % num: None,
      })

      previous_seasons = [v[1] for v in seasonal_data[season_index + 1 : season_index + 1 + num] if v[1]]
      if len(previous_seasons) < num:
        logging.info('%s(%s) has no enough data for %d seasons revenue growth CI before %s',
            stock.code(), stock.name(), num, self._insight_date.isoformat())
      else:
        # CI by the previous seasons data
        (mean, lower, upper, quantile) = self._PastAverageAndQuantileInPast(
            data_at_season, previous_seasons, t_list)
        result.update({
          '%dseasons_revenue_growth_mean' % num: round(mean, 2),
          '%dseasons_revenue_growth_lower' % num: round(lower, 2),
          '%dseasons_revenue_growth_upper' % num: round(upper, 2),
          '%dseasons_revenue_growth_quantile' % num: round(quantile, 1),
        })

    return result

  def _DoPEStats(self, stock, seasonal_data):
    season_index = self._GetInsightDateIndex(seasonal_data)
    if season_index < 0:
      return result

    # number of pervious seasons to consider and t-dist constants
    # list of (num_of_perious_seasons, t-dist)
    periods_configs = [
      # 8 seasons
      # (8, [(0.99, 3.4995), (0.98, 2.9980), (0.95, 2.3646)]),
      # 12 seasons
      (12, [(0.99, 3.1058), (0.98, 2.7181), (0.95, 2.2010)]),
    ]

    # stats column -> value
    result = {
        'PE_at_season': None,
    }

    data_at_season = seasonal_data[season_index][1]
    result['PE_at_season'] = round(data_at_season, 1)

    for num, t_list in periods_configs:
      result.update({
        '%dseasons_PE_mean' % num: None,
        '%dseasons_PE_lower' % num: None,
        '%dseasons_PE_upper' % num: None,
        '%dseasons_PE_quantile' % num: None,
      })

      previous_seasons = [v[1] for v in seasonal_data[season_index + 1 : season_index + 1 + num] if v[1]]
      if len(previous_seasons) < num:
        logging.info('%s(%s) has no enough data for %d seasons PE CI before %s',
            stock.code(), stock.name(), num, self._insight_date.isoformat())
      else:
        # CI by previous seasons data
        (mean, lower, upper, quantile) = self._PastAverageAndQuantileInPast(
            data_at_season, previous_seasons, t_list)
        result.update({
          '%dseasons_PE_mean' % num: round(mean, 1),
          '%dseasons_PE_lower' % num: round(lower, 1),
          '%dseasons_PE_upper' % num: round(upper, 1),
          '%dseasons_PE_quantile' % num: round(quantile, 1),
        })

    return result

  def _PastAverageAndQuantileInPast(self, season_metrics, past_metrics, t_list):
    """ Returns tuple of past average stats with CI and the season's quantile
    if put in the past. (mean, lower, upper, quantile_in_past).
    Assumes t_list is tuple (quantile, t) in descending order: 99%, 98%, 95%, etc
    """
    quantile = None  # by default
    mean = lower = upper = None
    for q, t in t_list:
      mean, lower, upper = self._AverageWithCI(past_metrics, t)
      if not quantile:
        if season_metrics > upper:
          # top ones
          quantile = (1.0 - (1.0 - q) / 2.0) * 100.0
        elif season_metrics < lower:
          # tail ones
          quantile = (1.0 - q) / 2.0 * 100.0
    if not quantile:
      quantile = 50.0  # means random
    return (mean, lower, upper, quantile)

  def _AverageWithCI(self, metrics_data, t):
    """ Returns (mean, lower, upper). """
    refined_data = [d if d else 0.0 for d in metrics_data]
    size = len(refined_data)
    mean = sum(refined_data) / float(size)
    # avg(x^2)
    square_mean = sum([i ** 2 for i in refined_data]) / float(size)
    s = math.sqrt((square_mean - mean ** 2) * float(size) / float(size - 1))
    # assume subject to t distribution
    lower = mean - t * s / math.sqrt(size)
    upper = mean + t * s / math.sqrt(size)
    return (mean, lower, upper)

  def _GetInsightDateIndex(self, seasons):
    if len(seasons) == 0:
      return -1
    insight_season = self._insight_date.isoformat()
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
  stock = Stock('000977', '浪潮信息')
  insighter = DataInsights(directory)
  stats = insighter.DoStats(stock)

  header = stats.keys()
  writer = csv.DictWriter(sys.stdout, fieldnames=header)
  writer.writeheader()
  writer.writerow(stats)

if __name__ == "__main__":
  main()
