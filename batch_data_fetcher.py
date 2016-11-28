#!/usr/bin/python2.7
# -*- coding: utf-8 -*-

import argparse
import datetime
import logging
import threading
import Queue

import flags
import data_fetcher
import stock_info

FLAGS = flags.FLAGS
flags.ArgParser().add_argument('--num_fetcher_threads', type=int, default=1,
    help='The max number of data fetcher threads in parallel.')

Stock = stock_info.Stock


class BatchDataFetcher:
  def __init__(self, data_fetcher, max_threads):
    assert 0 < max_threads and max_threads <= 100
    self.__max_threads = max_threads
    self.__data_fetcher = data_fetcher  # the real underlying data fetcher

    # all running threads
    self.__threads = []
    # the stocks waiting for threads to process
    self.__fetching_queue = Queue.Queue(max_threads)
    # the flag indicating all stocks have been put into queue.
    self.__all_stock_put = False;

  def Fetch(self, stock_list):
    if len(stock_list) == 0:
      return

    num_threads = min(len(stock_list), self.__max_threads)
    for i in range(num_threads):
      t = threading.Thread(target=self.__RunThread, name=('FetchThread-%d' % i))
      self.__threads.append(t)
      t.start()
    logging.info('%d threads started', len(self.__threads))

    start_ts = datetime.datetime.now()
    for stock in stock_list:
      self.__fetching_queue.put(stock, block=True)

    # all stocks put in the queue
    self.__all_stock_put = True
    self.__fetching_queue.join()  # block till all stocks fetched

    end_ts = datetime.datetime.now()
    logging.info('Total time elapsed in fetching data: %s', str(end_ts - start_ts))

    for thread in self.__threads:
      thread.join(timeout=10)  # ensure all threads exit
      assert not thread.is_alive()

  def __RunThread(self):
    while not self.__all_stock_put or not self.__fetching_queue.empty():
      # block at most 10 seconds to get the next stock
      stock = self.__fetching_queue.get(block=True, timeout=10)
      try:
        self.__data_fetcher.Fetch(stock)
      except Exception, e:
        logging.error('Error in fetching %s(%s): %s', stock.code(), stock.name(), e)
      finally:
        self.__fetching_queue.task_done()  # decrease the queue item in process


def main():
  # Parse command line flags into FLAGS.
  flags.ArgParser().parse_args(namespace=FLAGS)

  logging.basicConfig(level=logging.INFO)
  directory = './data/test'
  fetcher = data_fetcher.NeteaseSeasonFetcher(directory)
  stock_list = [
      Stock('000977', '浪潮信息'),
      Stock('002241', '歌尔股份'),
      Stock('002508', '老板电器'),
      Stock('002007', '华兰生物'),
      Stock('000651', '格力电器'),
  ]

  print 'Start batch fetching'
  batch = BatchDataFetcher(fetcher, FLAGS.num_fetcher_threads)
  batch.Fetch(stock_list)

if __name__ == "__main__":
  main()
