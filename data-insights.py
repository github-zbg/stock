#!/usr/bin/python2.7
# -*- coding: utf-8 -*-

import argparse
import bisect
import csv
import logging
import math
import os
import re
import sys

argument_parser = argparse.ArgumentParser()
argument_parser.add_argument('--insight_date', default='latest',
    help='The time to do insights: YYYY-03-31, YYYY-06-30, YYYY-09-30, YYYY-12-31 or latest.')
FLAGS = None

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

    refined_metrics_data = []  # [metrics_name, value for each season]
    for row in reader:
      metrics_name = row[metrics_column_name]
      if not metrics_name in metrics_names:
        continue

      # find a metics row
      metrics_data = [float(row[s]) if re.match(r'^-?\d+(\.\d+)?$', row[s]) else None for s in seasons]
      self._DoRevenueGrowthStats(metrics_data, seasons)

  def _DoRevenueGrowthStats(self, metrics_data, seasons):
    assert len(metrics_data) == len(seasons)
    date_index = self._GetInsightDate(seasons)
    if date_index < 0:
      return
    for i in range(4):  # calcuate 4 seasons(1 year)
      if date_index + i >= len(seasons):
        logging.info('No data for previous %d of %s', i, seasons[date_index])
        break
      # CI by 2 years data
      t7 = 2.3646   # 95% CI
      self._AverageWithCI(
          metrics_data[date_index + i:date_index + i + 8],
          seasons[date_index + i],
          8,
          t7)
      # CI by 3 years data
      t11 = 2.2010  # 95% CI
      self._AverageWithCI(
          metrics_data[date_index + i:date_index + i + 12],
          seasons[date_index + i],
          12,
          t11)

  def _AverageWithCI(self, metrics_data, season, top_n, t):
    if len(metrics_data) < top_n:
      logging.info('No %d seasons data for %s', top_n, season)
      return

    mean = sum(metrics_data) / float(top_n)
    square_mean = sum([i ** 2 for i in metrics_data]) / float(top_n)
    s = math.sqrt((square_mean - mean ** 2) * float(top_n) / float(top_n - 1))
    # subject to t distribution
    lower = mean - t * s / math.sqrt(top_n)
    upper = mean + t * s / math.sqrt(top_n)
    print season, '%d seasons' % top_n, mean, lower, upper

  def _GetInsightDate(self, seasons):
    if FLAGS.insight_date.lower() == 'latest':
      return 0
    i = bisect.bisect_left(seasons, FLAGS.insight_date)
    if i >= len(seasons) or seasons[i] != FLAGS.insight_date:  # does not exist
      return -1
    return i


def main():
  global FLAGS
  FLAGS = argument_parser.parse_args()

  logging.basicConfig(level=logging.INFO)
  directory = './data/test'
  stock_code = '000977'
  insighter = DataInsights(directory)
  insighter.DoStats(stock_code)

if __name__ == "__main__":
  main()
