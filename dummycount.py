#!/usr/bin/env python

import sys

from pyspark import SparkContext

sc = SparkContext(appName="DummyCountPy")
distData = sc.parallelize(range(1,101), 100)
result = distData.flatMap(lambda x : range(1, 101)).count()

print '10000 is %d' % result

# stop sparkContenxt
sc.stop()