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

    # convert the seasonal metrics data into floats
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

      logging.info('Running stats for %s', metrics_name)
      # list of (season, value)
      seasonal_data = [(s, float(row[s])) if re.match(r'^-?\d+(\.\d+)?$', row[s]) else (s, None)
          for s in seasons]
      stats = metrics_function(stock, seasonal_data)
      insight_data.update(stats)

    logging.info('Insighting %s(%s) done.', stock.code(), stock.name())
    return insight_data

  def _DoRevenueGrowthStats(self, stock, seasonal_data):
    # stats column -> value
    result = {
        'revenue_growth_at_season': None,
        '2year_revenue_growth_mean': None,
        '2year_revenue_growth_lower': None,
        '2year_revenue_growth_upper': None,
        '2year_revenue_growth_quantile': None,
        '3year_revenue_growth_mean': None,
        '3year_revenue_growth_lower': None,
        '3year_revenue_growth_upper': None,
        '3year_revenue_growth_quantile': None,
    }

    season_index = self._GetInsightDateIndex(seasonal_data)
    if season_index < 0:
      return result

    data_at_season = seasonal_data[season_index][1]
    result['revenue_growth_at_season'] = data_at_season

    data_8seasons = [v[1] for v in seasonal_data[season_index + 1 : season_index + 9] if v[1]]
    if len(data_8seasons) < 8:
      logging.info('%s(%s) has no enough data for 8 seasons revenue growth CI before %s',
          stock.code(), stock.name(), self._insight_date.isoformat())
    else:
      # CI by 2 years data
      t7_list = [(0.99, 3.4995), (0.98, 2.9980), (0.95, 2.3646)]
      (mean, lower, upper, quantile) = self._PastAverageAndQuantileInPast(
          data_at_season, data_8seasons, t7_list)
      result.update({
        '2year_revenue_growth_mean': mean,
        '2year_revenue_growth_lower': lower,
        '2year_revenue_growth_upper': upper,
        '2year_revenue_growth_quantile': quantile,
      })

    data_12seasons = [v[1] for v in seasonal_data[season_index + 1 : season_index + 13] if v[1]]
    if len(data_12seasons) < 12:
      logging.info('%s(%s) has no enough data for 12 seasons revenue growth CI before %s',
          stock.code(), stock.name(), self._insight_date.isoformat())
    else:
      # CI by 3 years data
      t11_list = [(0.99, 3.1058), (0.98, 2.7181), (0.95, 2.2010)]
      (mean, lower, upper, quantile) = self._PastAverageAndQuantileInPast(
          data_at_season, data_12seasons, t11_list)
      result.update({
        '3year_revenue_growth_mean': mean,
        '3year_revenue_growth_lower': lower,
        '3year_revenue_growth_upper': upper,
        '3year_revenue_growth_quantile': quantile,
      })

    return result

  def _DoPEStats(self, stock, seasonal_data):
    # stats column -> value
    result = {
        'PE_at_season': None,
        '2year_PE_mean': None,
        '2year_PE_lower': None,
        '2year_PE_upper': None,
        '2year_PE_quantile': None,
        '3year_PE_mean': None,
        '3year_PE_lower': None,
        '3year_PE_upper': None,
        '3year_PE_quantile': None,
    }

    season_index = self._GetInsightDateIndex(seasonal_data)
    if season_index < 0:
      return result

    data_at_season = seasonal_data[season_index][1]
    result['PE_at_season'] = data_at_season

    data_8seasons = [t[1] for t in seasonal_data[season_index + 1 : season_index + 9] if t[1]]
    if len(data_8seasons) < 8:
      logging.info('%s(%s) has no enough data for 8 seasons PE CI before %s',
          stock.code(), stock.name(), self._insight_date.isoformat())
    else:
      # CI by 2 years data
      t7_list = [(0.99, 3.4995), (0.98, 2.9980), (0.95, 2.3646)]
      (mean, lower, upper, quantile) = self._PastAverageAndQuantileInPast(
          data_at_season, data_8seasons, t7_list)
      result.update({
        '2year_PE_mean': mean,
        '2year_PE_lower': lower,
        '2year_PE_upper': upper,
        '2year_PE_quantile': quantile,
      })

    data_12seasons = [v[1] for v in seasonal_data[season_index + 1 : season_index + 13] if v[1]]
    if len(data_12seasons) < 12:
      logging.info('%s(%s) has no enough data for 12 seasons PE CI before %s',
          stock.code(), stock.name(), self._insight_date.isoformat())
    else:
      # CI by 3 years data
      t11_list = [(0.99, 3.1058), (0.98, 2.7181), (0.95, 2.2010)]
      (mean, lower, upper, quantile) = self._PastAverageAndQuantileInPast(
          data_at_season, data_12seasons, t11_list)
      result.update({
        '3year_PE_mean': mean,
        '3year_PE_lower': lower,
        '3year_PE_upper': upper,
        '3year_PE_quantile': quantile,
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
      if not quantile and (season_metrics > upper or season_metrics < lower):
        quantile = (1.0 - (1.0 - q) / 2.0) * 100.0
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
