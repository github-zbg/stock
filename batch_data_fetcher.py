#!/usr/bin/python2.7
# -*- coding: utf-8 -*-

import argparse
import logging
import threading
import Queue

import flags
import data_fetcher

FLAGS = flags.FLAGS

def GetArgParser(with_help=True):
  parser = argparse.ArgumentParser(
      add_help=with_help,
      parents=[
        data_fetcher.GetArgParser(with_help=False),
      ])
  parser.add_argument('--num_fetcher_threads', type=int, default=1,
      help='The max number of data fetcher threads in parallel.')
  return parser


class BatchDataFetcher:
  def __init__(self, data_fetcher, max_threads):
    assert 0 < max_threads and max_threads <= 100
    self.__max_threads = max_threads
    self.__data_fetcher = data_fetcher  # the real data fetcher
    self.__threads = []  # all running threads
    self.__fetching_queue = Queue.Queue(max_threads)
    self.__all_fetch_done = False;

  def Fetch(self, stock_code_list):
    if len(stock_code_list) == 0:
      return

    num_threads = min(len(stock_code_list), self.__max_threads)
    self.__all_fetch_done = False;
    for i in range(num_threads):
      t = threading.Thread(target=self.__RunThread, name=('FetchThread-%d', i))
      self.__threads.append(t)
      t.start()
    logging.info('%d threads started', len(self.__threads))

    for stock in stock_code_list:
      self.__fetching_queue.put(stock, block=True)

    # all stocks fetched
    self.__all_fetch_done = True
    for thread in self__threads:
      thread.join(timeout=60)  # wait at most 1 minute for the threads to exit
      assert not thread.is_alive()

  def __RunThread(self):
    while not self.__all_fetch_done:
      # block at most 10 seconds to get the next stock
      stock_code = self.__fetching_queue.get(block=True, timeout=10)
      self.__data_fetcher.Fetch(stock_code)


def main():
  global FLAGS
  FLAGS = GetArgParser().parse_args()

  logging.basicConfig(level=logging.INFO)
  directory = './data/test'
  fetcher = data_fetcher.NeteaseSeasonFetcher(directory)
  stock_code_list = [
      '000977',  # 浪潮信息
      '002241',  # 歌尔股份
      '002508',  # 老板电器
      '002007',  # 华兰生物
      '000651',  # 格力电器
  ]

  print 'Start batch fetching'
  batch = BatchDataFetcher(fetcher, FLAGS.num_fetcher_threads)
  batch.Fetch(stock_code_list)

if __name__ == "__main__":
  main()
