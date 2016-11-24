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
    help='The time to do insights: YYYY-3-31, YYYY-6-30, YYYY-9-30, YYYY-12-31.')


# Calculate insights for each stock
class DataInsights(object):
  def __init__(self, directory):
    self._directory = directory

  # Calculate statistical insights
  def DoStats(self, stock):
    logging.info('Insighting %s(%s) ...', stock.code(), stock.name())
    # revenue from main business
    datafile = os.path.join(self._directory, '%s.refined.csv' % stock.code())
    reader = csv.DictReader(open(datafile))
    seasons = list(reader.fieldnames)
    seasons = seasons[1:]  # keep only the dates
    seasons.sort(reverse=True)  # from the latest season

    metrics_column_name = u'指标'.encode('UTF8')
    metrics_names = [
        u'主营业务收入(万元)_growth'.encode('UTF8'),
    ]

    # The data to return: column -> value
    insight_data = {
        'Season': FLAGS.insight_date,
        'Code': stock.code(),
        'Name': stock.name(),
    }
    for row in reader:
      metrics_name = row[metrics_column_name]
      if not metrics_name in metrics_names:
        continue

      # convert the matrix row into floats
      metrics_data = [float(row[s]) if re.match(r'^-?\d+(\.\d+)?$', row[s]) else None for s in seasons]
      revenue_stats = self._DoRevenueGrowthStats(stock.code(), metrics_data, seasons)
      insight_data.update(revenue_stats)

    logging.info('Insighting %s(%s) done.', stock.code(), stock.name())
    return insight_data

  def _DoRevenueGrowthStats(self, stock_code, metrics_data, seasons):
    assert len(metrics_data) == len(seasons)

    # stats column -> value
    result = {
        '2year_revenue_growth_mean': None,
        '2year_revenue_growth_lower': None,
        '2year_revenue_growth_upper': None,
        '3year_revenue_growth_mean': None,
        '3year_revenue_growth_lower': None,
        '3year_revenue_growth_upper': None,
    }

    date_index = self._GetInsightDate(seasons)
    if date_index < 0:
      logging.warning('%s has no data for season %s.', stock_code, FLAGS.insight_date)
      return result

    data_2years = [v for v in metrics_data[date_index : date_index + 8] if v]
    if len(data_2years) < 8:
      logging.info('%s has no enough data for 2 years revenue growth CI till %s',
          stock_code, FLAGS.insight_date)
    else:
      # CI by 2 years data
      t7 = 2.3646   # 95% CI
      (mean, lower, upper) = self._AverageWithCI(data_2years, seasons[date_index], t7)
      result.update({
        '2year_revenue_growth_mean': mean,
        '2year_revenue_growth_lower': lower,
        '2year_revenue_growth_upper': upper,
      })

    data_3years = [v for v in metrics_data[date_index : date_index + 12] if v]
    if len(data_3years) < 12:
      logging.info('%s has no enough data for 3 years revenue growth CI till %s',
          stock_code, FLAGS.insight_date)
    else:
      # CI by 3 years data
      t11 = 2.2010  # 95% CI
      (mean, lower, upper) = self._AverageWithCI(data_3years, seasons[date_index], t11)
      result.update({
        '3year_revenue_growth_mean': mean,
        '3year_revenue_growth_lower': lower,
        '3year_revenue_growth_upper': upper,
      })

    return result


  def _AverageWithCI(self, metrics_data, season, t):
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
    # print season, '%d seasons' % size, mean, lower, upper
    return (mean, lower, upper)

  def _GetInsightDate(self, seasons):
    if len(seasons) == 0:
      return -1
    # seasons are in descending order.
    start = 0
    end = len(seasons) - 1
    while start != end:
      mid = (start + end) / 2
      if FLAGS.insight_date < seasons[mid]:
        start = mid + 1
      else:
        end = mid
    if FLAGS.insight_date == seasons[start]:
      return start
    return -1


def main():
  # Parse command line flags into FLAGS.
  flags.ArgParser().parse_args(namespace=FLAGS)

  logging.basicConfig(level=logging.INFO)
  directory = './data/seasonal/2016-10-01'
  stock = Stock('603131', '上海沪工')
  insighter = DataInsights(directory)
  print insighter.DoStats(stock)

if __name__ == "__main__":
  main()
