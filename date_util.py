#!/usr/bin/python2.7
# -*- coding: utf-8 -*-

import datetime
import logging
import os
import sys


def GetLastDay(day):
  return day - datetime.timedelta(days=1)


def GetSameDayOfLastYear(day):
  """ Returns the same day of last year. """
  return datetime.date(day.year - 1, day.month, day.day)


def GetSeasonMonth(month):
  assert 1 <= month and month <= 12
  # 1,2,3 -> 1
  # 4,5,6 -> 4
  # 7,8,9 -> 7
  # 10,11,12 -> 10
  return (month - 1) / 3 * 3 + 1


def GetSeasonStartDate(day):
  """ Returns the season's start date of that day.
      day: datetime.date
  """
  return datetime.date(day.year, GetSeasonMonth(day.month), 1)


def GetLastSeasonEndDate(day):
  """ Returns the last season's end date of that day.
      day: datetime.date
  """
  # season start is YYYY-01-01, YYYY-04-01, YYYY-07-01, YYYY-10-01
  season_start = GetSeasonStartDate(day)
  return GetLastDay(season_start)


def GetLastNSeasonsStart(day, last_n):
  """ Returns a list of last N seasons since that day.
      day: datetime.date
      last_n: int
  """
  assert last_n > 0
  season_month = [10, 7, 4, 1]
  start_index = 0 + (10 - GetSeasonMonth(day.month)) / 3
  start_year = day.year
  result = []
  for i in range(last_n):
    month = season_month[(start_index + i) % 4]
    year = start_year - (start_index + i) / 4
    result.append(datetime.date(year, month, 1))
  return result
