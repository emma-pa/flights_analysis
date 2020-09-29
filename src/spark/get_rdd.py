# -*- coding: utf-8 -*-

import pyspark

from flight_data import read_csvs, read_plane_data

def get_RDD_from_flight_data(fnames, limit=None, sc=None, numSlices=None):
    """
    @author jbl
    """
    if sc is None:
        sparkconf = pyspark.SparkConf()
        sparkconf.set('spark.port.maxRetries', 128)
        sc = pyspark.SparkContext(conf=sparkconf)
    if numSlices is None:
        numSlices = 1000
    D = sc.parallelize(read_csvs(fnames, limit), numSlices=numSlices)
    return sc, D
