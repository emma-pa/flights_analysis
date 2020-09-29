# -*- coding: utf-8 -*-

import textwrap
import collections

import numpy as np
import cassandra.cluster

from flight_data import Flight

def _insert_query_by_hour(flight):
    """Build the query to insert datas in the DB.

    Parameters
    ----------
    flight : Flight
           The flight to insert in the DB.

    Return
    ------
    query : string
            The query to execute.
    """
    query = textwrap.dedent(
        f"""
        INSERT INTO flight_by_time
        (
            start_year,
            start_month,
            start_day_month,
            start_day_week,
            start_hour,
            cancelled,
            arr_delay,
            dep_delay,
            tailnum,
            plane_age
        )
        VALUES
        (
            {flight.year},
            {flight.month},
            {flight.day_month},
            {flight.day_week},
            {flight.hour},
            {flight.cancelled},
            {flight.ArrDelay if flight.ArrDelay != 'NA' else 'null'},
            {flight.DepDelay if flight.DepDelay != 'NA' else 'null'},
            '{flight.tailnum if flight.tailnum != 'NA' else ''}',
            {flight.plane_age if flight.plane_age != 'NA' else 'null'}
        );
        """
    )
    return query

INSERTS_Q = (
    _insert_query_by_hour,
)

class ConnectionDB:
    def __init__(self):
        self._cluster = cassandra.cluster.Cluster()
        self._session = self._cluster.connect("paroisem_final")

    def __del__(self):
        self._cluster.shutdown()

class InsertFlight(ConnectionDB):
    """To insert data into the DB which stores flights"""
    def __init__(self):
        ConnectionDB.__init__(self)

    def __del__(self):
        ConnectionDB.__del__(self)

    def insert_datastream(self, stream):
        """Insert datas into the DB.

        Parameters
        ----------
        stream : iterable
                 Iterable where values to insert are taken.
        """
        for flight in stream:
            # ArrDelay and DepDelay and TailNum can be 'NA' when cancelled is true
            if (flight.year != 'NA'
                and flight.month != 'NA'
                and flight.day_month != 'NA'
                and flight.day_week != 'NA'
                and flight.hour != 'NA'
                and ((flight.cancelled == False and flight.ArrDelay != 'NA' and flight.DepDelay != 'NA') or flight.cancelled == True)):
                for q in INSERTS_Q:
                    query = q(flight)
                    self._session.execute(query)
