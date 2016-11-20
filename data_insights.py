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

FLAGS = flags.FLAGS

def GetArgParser(with_help=True):
  parser = argparse.ArgumentParser(
      add_help=with_help,
      parents=[
      ])
  parser.add_argument('--insight_date', default=_GetLastSeasonEndDate(),
      help='The time to do insights: YYYY-3-31, YYYY-6-30, YYYY-9-30, YYYY-12-31.')
  return parser


def _GetLastSeasonEndDate():
  """ Returns the last season end date of today. """
  def _season_month(month):
    # 1,2,3 -> 1
    # 4,5,6 -> 4
    # 7,8,9 -> 7
    # 10,11,12 -> 10
    return (month - 1) / 3 * 3 + 1

  today = datetime.date.today()
  # next_start is YYYY-01-01, YYYY-04-01, YYYY-07-01, YYYY-10-01
  next_start = datetime.date(today.year, _season_month(today.month), 1)
  last_end = (next_start - datetime.timedelta(days=1)).isoformat()
  return last_end
  # return '%d-%d-%d' % (last_end.year, last_end.month, last_end.day)


# Calculate insights for each stock
class DataInsights(object):
  def __init__(self, directory):
    self._directory = directory

  # Calculate statistical insights
  def DoStats(self, stock_code):
    logging.info('Insighting %s ...', stock_code)
    # revenue from main business
    datafile = os.path.join(self._directory, '%s.refined.csv' % stock_code)
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
        'Code': stock_code,
    }
    for row in reader:
      metrics_name = row[metrics_column_name]
      if not metrics_name in metrics_names:
        continue

      # convert the matrix row into floats
      metrics_data = [float(row[s]) if re.match(r'^-?\d+(\.\d+)?$', row[s]) else None for s in seasons]
      revenue_stats = self._DoRevenueGrowthStats(stock_code, metrics_data, seasons)
      insight_data.update(revenue_stats)

    logging.info('Insighting %s done.', stock_code)
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

    if date_index + 8 >= len(seasons):
      logging.info('%s has no enough data for 2 years revenue growth CI till %s',
          stock_code, FLAGS.insight_date)
    else:
      # CI by 2 years data
      t7 = 2.3646   # 95% CI
      (mean, lower, upper) = self._AverageWithCI(
          metrics_data[date_index : date_index + 8],
          seasons[date_index],
          t7)
      result.update({
        '2year_revenue_growth_mean': mean,
        '2year_revenue_growth_lower': lower,
        '2year_revenue_growth_upper': upper,
      })

    if date_index + 12 >= len(seasons):
      logging.info('%s has no enough data for 3 years revenue growth CI till %s',
          stock_code, FLAGS.insight_date)
    else:
      # CI by 3 years data
      t11 = 2.2010  # 95% CI
      (mean, lower, upper) = self._AverageWithCI(
          metrics_data[date_index : date_index + 12],
          seasons[date_index],
          t11)
      result.update({
        '3year_revenue_growth_mean': mean,
        '3year_revenue_growth_lower': lower,
        '3year_revenue_growth_upper': upper,
      })

    return result


  def _AverageWithCI(self, metrics_data, season, t):
    """ Returns (mean, lower, upper). """
    size = len(metrics_data)
    mean = sum(metrics_data) / float(size)
    # avg(x^2)
    square_mean = sum([i ** 2 for i in metrics_data]) / float(size)
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
  GetArgParser().parse_args(namespace=FLAGS)

  logging.basicConfig(level=logging.INFO)
  directory = './data/test'
  stock_code = '000977'
  insighter = DataInsights(directory)
  insighter.DoStats(stock_code)

if __name__ == "__main__":
  main()
