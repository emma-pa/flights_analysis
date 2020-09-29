# -*- coding: utf-8 -*-

import csv
import datetime
import itertools
import collections

limiteur = lambda generator, limit: (data for _, data in zip(range(limit), generator))

Flight = collections.namedtuple(
    "Flight",
    ("year", "month", "day_month", "day_week", "hour", "ArrDelay", "DepDelay", "cancelled", "tailnum", "plane_age"),
)

def _read_one_csv(filename):
    """Read a file which contains flights data and retrieve usefull information in a Flight tuple."""
    Plane = read_plane_data()
    with open(filename) as f:
        for row in csv.DictReader(f):
            year = int(row["Year"]) if row["Year"] != 'NA' else row["Year"]
            month = int(row["Month"]) if row["Month"] != 'NA' else row["Month"]
            day_month = int(row["DayofMonth"]) if row["DayofMonth"] != 'NA' else row["DayofMonth"]
            day_week = int(row["DayOfWeek"]) if row["DayOfWeek"] != 'NA' else row["DayOfWeek"]
            tailnum = row["TailNum"]
            hh_mm = row["CRSDepTime"]
            if len(hh_mm) < 4:
                hh_mm = "0" + hh_mm
            hour = int(hh_mm[:2])
            ArrDelay = int(row["ArrDelay"]) if row["ArrDelay"] != 'NA' else row["ArrDelay"]
            DepDelay = int(row["DepDelay"]) if row["DepDelay"] != 'NA' else row["DepDelay"]
            cancelled = bool(int(row["Cancelled"])) if row["Cancelled"] != 'NA' else row["Cancelled"]
            plane_age = year - Plane[tailnum] if tailnum in Plane and year != 'NA' else 'NA'
            yield Flight(year, month, day_month, day_week, hour, ArrDelay, DepDelay,cancelled, tailnum, plane_age)

def read_csvs(fnames, limit=None):
    """
    @author jbl
    """
    gen = itertools.chain(*[_read_one_csv(fname) for fname in fnames])
    if limit is None:
        return gen
    return limiteur(gen, limit)

def read_plane_data(filename="/project_data/plane-data.csv"):
    """Read the plane data file and retrieve a dictionnary which links the tail number and the delivery year of the plane."""
    Plane = dict()
    with open(filename) as f:
        for row in csv.DictReader(f):
            tailnum = row["tailnum"]
            # Some weird cases are not None but a string 'None'
            if row["year"] is not None and row["year"] != 'None':
                Plane[tailnum] = int(row["year"])
    return Plane
