# -*- coding: utf-8 -*-

import datetime
import textwrap
import functools
import collections

import numpy as np
import matplotlib.pyplot as plt

import feed_cassandra as feed

class GetFlight(feed.ConnectionDB):
    """To access DB which stores flights data and retrieve flights."""
    def __init__(self):
        feed.ConnectionDB.__init__(self)

    def __del__( self ):
        feed.ConnectionDB.__del__(self)

    def get_flight_one_day_hour(self, dt):
        """
        Get the flights of a given hour and day.

        Parameters
        ----------
        dt : object datetime
             Date of the flights to retrieve.
        """
        query = textwrap.dedent(
            f"""
            SELECT
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
            FROM
                flight_by_time
            WHERE
                    start_year={dt.year}
                AND
                    start_month={dt.month}
                AND
                    start_day_month={dt.day}
                AND
                    start_day_week={dt.weekday()+1}
                AND
                    start_hour={dt.hour}
            ;
            """
        )
        for r in self._session.execute(query):
            yield feed.Flight(
                r.start_year,
                r.start_month,
                r.start_day_month,
                r.start_day_week,
                r.start_hour,
                r.arr_delay,
                r.dep_delay,
                r.cancelled,
                r.tailnum,
                r.plane_age
            )

    def get_hour_flights_between_dates(self, dt1, dt2, hour, includeCancelledFlights=False):
        """
        Get flights of a given hour between two dates.

        Parameters
        ----------
        dt1 : object datetime
              1rst date of the interval.

        dt2 : object datetime
              2nd date of the interval.

        hour : integer
               Hour of the trips to retrieve.

        includeCancelledFlights : boolean
                                  Include cancelled flights or not.
        """
        duration = dt2-dt1
        for day in range(duration.days):
            date = dt1 + datetime.timedelta(days=day)
            date = date.replace(hour=hour)
            for flight in self.get_flight_one_day_hour(date):
                if (flight.cancelled and includeCancelledFlights) or (not flight.cancelled):
                    yield flight

    def get_day_flights_between_dates(self, dt1, dt2, week_day, includeCancelledFlights=False):
        """
        Get flights of a given day of week between two dates.

        Parameters
        ----------
        dt1 : object datetime
              1rst date of the interval.

        dt2 : object datetime
              2nd date of the interval.

        week_day : integer
                   Day of week of the trips to retrieve.

        includeCancelledFlights : boolean
                                  Include cancelled flights or not.
        """
        duration = dt2-dt1
        if duration.days < 7:
            raise NotEnoughTime
        for day in range(duration.days):
            date = dt1 + datetime.timedelta(days=day)
            if date.weekday() == week_day:
                for hour in range(24):
                    date = date.replace(hour=hour)
                    for flight in self.get_flight_one_day_hour(date):
                        if (flight.cancelled and includeCancelledFlights) or (not flight.cancelled):
                            yield flight

    def get_season_flights_between_dates(self, dt1, dt2, season, includeCancelledFlights=False):
        """
        Get flights of a given day of week between two dates.

        Parameters
        ----------
        dt1 : object datetime
              1rst date of the interval.

        dt2 : object datetime
              2nd date of the interval.

        season : integer
                 Season of the trips to retrieve.

        includeCancelledFlights : boolean
                                  Include cancelled flights or not.
        """
        duration = dt2-dt1
        for day in range(duration.days):
            date = dt1 + datetime.timedelta(days=day)
            if get_season(date) == season:
                for hour in range(24):
                    date = date.replace(hour=hour)
                    for flight in self.get_flight_one_day_hour(date):
                        if (flight.cancelled and includeCancelledFlights) or (not flight.cancelled):
                            yield flight

class NotEnoughTime(Exception):
    pass

#
# Shared methods
#
def _comp_mean_std_delay(stream):
    """Compute mean and standard deviation of arrival and departure delays a stream of flights."""
    prepared_data =  map(lambda f: np.array([1, f.ArrDelay, f.ArrDelay**2, f.DepDelay, f.DepDelay**2]), stream)
    results = functools.reduce(lambda x, y: x+y, prepared_data, 0)
    if isinstance(results, int) and results == 0:
        return 0, 0, 0, 0
    else:
        sum_1, sum_ArrDelay, sum_ArrDelay2, sum_DepDelay, sum_DepDelay2 = results
        mean_ArrDelay = sum_ArrDelay/sum_1
        mean_DepDelay = sum_DepDelay/sum_1
        std_ArrDelay = np.sqrt(sum_ArrDelay2/sum_1 - (mean_ArrDelay)**2)
        std_DepDelay = np.sqrt(sum_DepDelay2/sum_1 - (mean_DepDelay)**2)
        return (mean_ArrDelay, mean_DepDelay, std_ArrDelay, std_DepDelay)

def _comp_cancelled_proportion(stream):
    """Compute proportion (in percentage) of number of cancelled flights over number of flights of a stream."""
    prepared_data =  map(lambda f: np.array([1, 1 if f.cancelled else 0]), stream)
    results = functools.reduce(lambda x, y: x+y, prepared_data, 0)
    if isinstance(results, int) and results == 0:
        return 0
    else:
        sum_1, sum_nb_cancelled = results
        # Percentage of cancelled flights, remove outliers
        return sum_nb_cancelled/sum_1*100 if sum_1 != 1 else 0

#
# Average delays and average count of cancelled flights per hour of day
#
def avg_std_per_hour_between_dates(dt1, dt2):
    """Calculate the average delay per hour at the departure and at the arrival, between two given dates.

    Parameters
    ----------
    dt1, dt2 : object datetime
               Dates to select data.
    """
    getter = GetFlight()
    mean_ArrDelay = np.zeros(24)
    mean_DepDelay = np.zeros(24)
    std_ArrDelay = np.zeros(24)
    std_DepDelay = np.zeros(24)
    prop_cancelled = np.zeros(24)
    for h in range(24):
        mean_ArrDelay[h], mean_DepDelay[h], std_ArrDelay[h], std_DepDelay[h] = _comp_mean_std_delay(getter.get_hour_flights_between_dates(dt1, dt2, h))
        prop_cancelled[h] = _comp_cancelled_proportion(getter.get_hour_flights_between_dates(dt1, dt2, h, True))
    del getter
    return mean_ArrDelay, mean_DepDelay, std_ArrDelay, std_DepDelay, prop_cancelled

def view_results_hour(mean_ArrDelay, mean_DepDelay, std_ArrDelay, std_DepDelay, prop_cancelled):
    """Save barplots and errorbar plots to display means and standard deviations of arrival
       and departure delays and proportion of cancelled flights for every hour of a day."""
    hours = np.arange(24)
    width = 0.35
    plt.figure(figsize=(15,5))
    plt.subplot(121)
    plt.xticks(hours)
    plt.bar(hours - width/2, mean_ArrDelay, width, label='Arrival')
    plt.bar(hours + width/2, mean_DepDelay, width, label='Departure')
    plt.xlabel('Hours', fontsize=12)
    plt.ylabel('Average delay (minutes)', fontsize=12)
    plt.title('Average arrival and departure delays per hour')
    plt.legend()
    plt.subplot(122)
    plt.bar(hours, prop_cancelled)
    plt.title('Percentage of cancelled flights per hour')
    plt.xlabel('Hours', fontsize=12)
    plt.ylabel('Percentage of cancelled flights', fontsize=12)
    plt.savefig('avg_delay_cancelled.png')

    plt.clf()

    plt.figure(figsize=(15,5))
    plt.subplot(121)
    plt.errorbar(hours, mean_ArrDelay, yerr=std_ArrDelay)
    plt.xlabel('Hours', fontsize=12)
    plt.ylabel('Average arrival delay', fontsize=12)
    plt.title('Average arrival delays and standard deviation per hour')
    plt.subplot(122)
    plt.errorbar(hours, mean_DepDelay, yerr=std_DepDelay)
    plt.xlabel('Hours', fontsize=12)
    plt.ylabel('Average departure delay', fontsize=12)
    plt.title('Average departure delays and standard deviation per hour')
    plt.savefig('avg_std_delay_hour.png')

#
# Average delays and average count of cancelled flights per day of week
#
def avg_std_per_day_between_dates(dt1, dt2):
    """Calculate the average delay per day of week at the departure and at the arrival, between two given dates."""
    getter = GetFlight()
    mean_ArrDelay = np.zeros(7)
    mean_DepDelay = np.zeros(7)
    prop_cancelled = np.zeros(7)
    std_ArrDelay = np.zeros(7)
    std_DepDelay = np.zeros(7)
    try:
        for d in range(7):
            mean_ArrDelay[d], mean_DepDelay[d], std_ArrDelay[d], std_DepDelay[d] = _comp_mean_std_delay(getter.get_day_flights_between_dates(dt1, dt2, d))
            prop_cancelled[d] = _comp_cancelled_proportion(getter.get_day_flights_between_dates(dt1, dt2, d, True))
    except NotEnoughTime:
        print("Exception : 7 days or more are needed between dt1 and dt2")
    del getter
    return mean_ArrDelay, mean_DepDelay, std_ArrDelay, std_DepDelay, prop_cancelled

def view_results_day(mean_ArrDelay, mean_DepDelay, std_ArrDelay, std_DepDelay, prop_cancelled):
    """Save barplots and errorbar plots to display means and standard deviations of arrival
       and departure delays and proportion of cancelled flights for every day of a week."""
    days = np.arange(1,8)
    width = 0.35
    plt.figure(figsize=(18,5))
    plt.subplot(121)
    plt.xticks(days, ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'))
    plt.bar(days - width/2, mean_ArrDelay, width, label='Arrival')
    plt.bar(days + width/2, mean_DepDelay, width, label='Departure')
    plt.xlabel('Days', fontsize=12)
    plt.ylabel('Average delay (minutes)', fontsize=12)
    plt.title('Average arrival and departure delays per day of week')
    plt.legend()
    plt.subplot(122)
    plt.xticks(days, ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'))
    plt.bar(days, prop_cancelled)
    plt.title('Percentage of cancelled flights per day of week')
    plt.xlabel('Days', fontsize=12)
    plt.ylabel('Percentage of cancelled flights', fontsize=12)
    plt.savefig('avg_delay_cancelled_day_week.png')

    plt.clf()

    plt.figure(figsize=(15,5))
    plt.subplot(121)
    plt.errorbar(days, mean_ArrDelay, yerr=std_ArrDelay)
    plt.xticks(days, ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'))
    plt.xlabel('Days', fontsize=12)
    plt.ylabel('Average arrival delay', fontsize=12)
    plt.title('Average arrival delays and standard deviation per day of week')
    plt.subplot(122)
    plt.errorbar(days, mean_DepDelay, yerr=std_DepDelay)
    plt.xticks(days, ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'))
    plt.xlabel('Days', fontsize=12)
    plt.ylabel('Average departure delay', fontsize=12)
    plt.title('Average departure delays and standard deviation per day of week')
    plt.savefig('avg_std_delay_day.png')

#
# Average delays and average count of cancelled flights per season
#
def get_season(dt):
    """Retrieve the season of a given date."""
    if datetime.datetime(dt.year-1, 12, 21) <= dt <= datetime.datetime(dt.year, 3, 19):
        return 0
    if datetime.datetime(dt.year, 3, 19) <= dt <= datetime.datetime(dt.year, 6, 19):
        return 1
    if datetime.datetime(dt.year, 6, 20) <= dt <= datetime.datetime(dt.year, 9, 21):
        return 2
    if datetime.datetime(dt.year, 9, 22) <= dt <= datetime.datetime(dt.year, 12, 20):
        return 3

def avg_std_per_season_between_dates(dt1, dt2):
    """Calculate the average delay per season at the departure and at the arrival, between two given dates."""
    getter = GetFlight()
    mean_ArrDelay = np.zeros(4)
    mean_DepDelay = np.zeros(4)
    prop_cancelled = np.zeros(4)
    std_ArrDelay = np.zeros(4)
    std_DepDelay = np.zeros(4)
    for s in range(4):
        mean_ArrDelay[s], mean_DepDelay[s], std_ArrDelay[s], std_DepDelay[s] = _comp_mean_std_delay(getter.get_season_flights_between_dates(dt1, dt2, s))
        prop_cancelled[s] = _comp_cancelled_proportion(getter.get_season_flights_between_dates(dt1, dt2, s, True))
    del getter
    return mean_ArrDelay, mean_DepDelay, std_ArrDelay, std_DepDelay, prop_cancelled


def view_results_season(mean_ArrDelay, mean_DepDelay, std_ArrDelay, std_DepDelay, prop_cancelled):
    """Save barplots and errorbar plots to display means and standard deviations of arrival
       and departure delays and proportion of cancelled flights for every season of a year."""
    season = np.arange(4)
    width = 0.35
    plt.figure(figsize=(18,5))
    plt.subplot(121)
    plt.xticks(season, ('Winter', 'Spring', 'Summer', 'Automumn'))
    plt.bar(season - width/2, mean_ArrDelay, width, label='Arrival')
    plt.bar(season + width/2, mean_DepDelay, width, label='Departure')
    plt.xlabel('Season', fontsize=12)
    plt.ylabel('Average delay (minutes)', fontsize=12)
    plt.title('Average arrival and departure delays per season')
    plt.legend()
    plt.subplot(122)
    plt.xticks(season, ('Winter', 'Spring', 'Summer', 'Automumn'))
    plt.bar(season, prop_cancelled)
    plt.title('Percentage of cancelled flights per season')
    plt.xlabel('Season', fontsize=12)
    plt.ylabel('Percentage of cancelled flights', fontsize=12)
    plt.savefig('avg_delay_cancelled_season.png')

    plt.clf()

    plt.figure(figsize=(15,5))
    plt.subplot(121)
    plt.errorbar(season, mean_ArrDelay, yerr=std_ArrDelay)
    plt.xticks(season, ('Winter', 'Spring', 'Summer', 'Automumn'))
    plt.xlabel('Seson', fontsize=12)
    plt.ylabel('Average arrival delay', fontsize=12)
    plt.title('Average arrival delays and standard deviation per season')
    plt.subplot(122)
    plt.errorbar(season, mean_DepDelay, yerr=std_DepDelay)
    plt.xticks(season, ('Winter', 'Spring', 'Summer', 'Automumn'))
    plt.xlabel('Season', fontsize=12)
    plt.ylabel('Average departure delay', fontsize=12)
    plt.title('Average departure delays and standard deviation per season')
    plt.savefig('avg_std_delay_season.png')

# Calculate means and standard deviations per hour in 2007
avg_ArrDelay, avg_DepDelay, std_ArrDelay, std_DepDelay, prop_cancelled = avg_std_per_hour_between_dates(datetime.datetime(2007,1,1),datetime.datetime(2008,1,1))
view_results_hour(avg_ArrDelay, avg_DepDelay, std_ArrDelay, std_DepDelay, prop_cancelled)
# Calculate means and standard deviations per day of week in 2007
avg_ArrDelay, avg_DepDelay, std_ArrDelay, std_DepDelay, prop_cancelled = avg_std_per_day_between_dates(datetime.datetime(2007,1,1),datetime.datetime(2008,1,1))
view_results_day(avg_ArrDelay, avg_DepDelay, std_ArrDelay, std_DepDelay, prop_cancelled)
# Calculate means and standard deviations per season in 2007
avg_ArrDelay, avg_DepDelay, std_ArrDelay, std_DepDelay, prop_cancelled = avg_std_per_season_between_dates(datetime.datetime(2007,1,1),datetime.datetime(2008,1,1))
view_results_season(avg_ArrDelay, avg_DepDelay, std_ArrDelay, std_DepDelay, prop_cancelled)
