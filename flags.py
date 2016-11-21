#!/usr/bin/python2.7
# -*- coding: utf-8 -*-

import argparse

# Arbitary internal class holding the command line flags.
class __F(object):
  pass

# The shared parse result of command line flags.
FLAGS = __F()

# The shared parser to define arguments.
__arg_parser = argparse.ArgumentParser()
def ArgParser():
  return __arg_parser
