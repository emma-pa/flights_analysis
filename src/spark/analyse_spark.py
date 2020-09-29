# -*- coding: utf-8 -*-

import itertools
import datetime
import textwrap

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from get_rdd import get_RDD_from_flight_data, read_csvs

#
# Average delay per group of age per year
#
def age_group(flight):
    """Give the age category."""
    if flight.plane_age <= 5:
        return 0
    if flight.plane_age <= 10:
        return 1
    if flight.plane_age <= 15:
        return 2
    if flight.plane_age <= 20:
        return 3
    if flight.plane_age <= 25:
        return 4
    return 5

def _comp_mean_std(data):
    """Compute mean and standard deviation."""
    key, (sum_1, sum_ArrDelay, sum_DepDelay, sum_ArrDelay_2, sum_DepDelay_2) = data
    mean_ArrDelay = sum_ArrDelay/sum_1
    mean_DepDelay = sum_DepDelay/sum_1
    std_ArrDelay = np.sqrt(sum_ArrDelay_2/sum_1 - mean_ArrDelay**2)
    std_DepDelay = np.sqrt(sum_DepDelay_2/sum_1 - mean_DepDelay**2)
    return key, (mean_ArrDelay, std_ArrDelay, mean_DepDelay, std_DepDelay, sum_1)

def get_D_clean(D, includeCancelledFlights=False):
    """Clean the RDD before using it for planes age analysis.
    Parameters
    ----------
    D : Spark RDD
        The RDD to clean.

    includeCancelledFlights : boolean
                              Precise if cancelled flights should be included or not.
    """
    D = D.filter(lambda f: (
        (f.cancelled and includeCancelledFlights) or (not f.cancelled)
        and f.year != 'NA'
        and f.month != 'NA'
        and f.day_month != 'NA'
        and f.day_week != 'NA'
        and f.hour != 'NA'
        and f.plane_age != 'NA'
        and ((f.cancelled == False and f.ArrDelay != 'NA' and f.DepDelay != 'NA') or f.cancelled == True))
    )
    return D

def avg_delay_per_age_group(D):
    """Compute mean and standard deviation of arrival and departure delay for different age categories and years."""
    D = get_D_clean(D)
    delay_group = (
        D.map(lambda f: ((age_group(f), f.year), np.array([1, f.ArrDelay, f.DepDelay, f.ArrDelay**2, f.DepDelay**2])))
        .reduceByKey(lambda x, y: x + y)
        .map(_comp_mean_std)
        .collect()
    )

    years = np.array([year for ((group, year), (mean_ArrDelay, std_ArrDelay, mean_DepDelay, std_DepDelay, count_group)) in delay_group])
    years = np.unique(years)

    dico_ArrDelay = {}
    dico_DepDelay = {}
    for year in years:
        dico_ArrDelay[f"mean-{year}"] = np.zeros(6)
        dico_DepDelay[f"mean-{year}"] = np.zeros(6)
        dico_ArrDelay[f"count-{year}"] = np.zeros(6)
        dico_DepDelay[f"count-{year}"] = np.zeros(6)
        dico_ArrDelay[f"std-{year}"] = np.zeros(6)
        dico_DepDelay[f"std-{year}"] = np.zeros(6)

    df_ArrDelay = pd.DataFrame(data=dico_ArrDelay)
    df_DepDelay = pd.DataFrame(data=dico_DepDelay)
    for ((group, year), (mean_ArrDelay, std_ArrDelay, mean_DepDelay, std_DepDelay, count_group)) in delay_group:
        df_ArrDelay.loc[group, f"mean-{year}"] = mean_ArrDelay
        df_ArrDelay.loc[group, f"count-{year}"] = count_group
        df_ArrDelay.loc[group, f"std-{year}"] = std_ArrDelay

        df_DepDelay.loc[group, f"mean-{year}"] = mean_DepDelay
        df_DepDelay.loc[group, f"count-{year}"] = count_group
        df_DepDelay.loc[group, f"std-{year}"] = std_DepDelay

    return df_ArrDelay, df_DepDelay

def view_results_one_year(mean_ArrDelay, mean_DepDelay, std_ArrDelay, std_DepDelay):
    """Save barplots which displays means and standard deviation of arrival and departure delays.

    Parameters
    ----------
    mean_ArrDelay,  mean_DepDelay: Pandas DataFrames
                                   Contains means of one year for the different age categories.

    std_ArrDelay, std_DepDelay : Panda DataFrames
                                 Contains standard deviations of one year for the different age categories.
    """
    mean_ArrDelay = mean_ArrDelay.to_numpy()
    mean_DepDelay = mean_DepDelay.to_numpy()
    std_ArrDelay = std_ArrDelay.to_numpy()
    std_DepDelay = std_DepDelay.to_numpy()
    width = 0.35
    x = np.arange(6)

    plt.figure(figsize=(10,5))
    plt.xticks(x, ('0-5 ans', '5-10 ans', '10-15 ans', '15-20 ans', '20-25 ans', '+25 ans'))
    plt.bar(x - width/2, mean_ArrDelay, width, label='Arrival')
    plt.bar(x + width/2, mean_DepDelay, width, label='Departure')
    plt.legend()
    plt.title('Average arrival and departure delay per age category')
    plt.xlabel('Age categories', fontsize=12)
    plt.ylabel('Average arrival and departure delay', fontsize=12)
    plt.savefig('avg_delay.png')

    plt.clf()

    plt.figure(figsize=(15,5))
    plt.subplot(121)
    plt.errorbar(x, mean_ArrDelay, yerr=std_ArrDelay)
    plt.xticks(x, ('0-5 ans', '5-10 ans', '10-15 ans', '15-20 ans', '20-25 ans', '+25 ans'))
    plt.xlabel('Age categories', fontsize=12)
    plt.ylabel('Average arrival delay', fontsize=12)
    plt.title('Average arrival delays and standard deviation per category')
    plt.subplot(122)
    plt.errorbar(x, mean_DepDelay, yerr=std_DepDelay)
    plt.xticks(x, ('0-5 ans', '5-10 ans', '10-15 ans', '15-20 ans', '20-25 ans', '+25 ans'))
    plt.xlabel('Age categories', fontsize=12)
    plt.ylabel('Average departure delay', fontsize=12)
    plt.title('Average departure delays and standard deviation per category')
    plt.savefig('avg_std_delay_categ.png')

#
# Average age of plane per year
#
def avg_age_plane_year(D):
    """Compute the middle age of planes which flew over a year per year."""
    D = get_D_clean(D)
    avg_age = (
        D.map(lambda flight: ((flight.tailnum, flight.year), flight.plane_age))
        .reduceByKey(lambda x, y: x)
        .map(lambda flight: (flight[0][1], np.array([1, flight[1]])))
        .reduceByKey(lambda x, y: x+y)
        .map(lambda year: (year[0], year[1][1]/year[1][0], year[1][0]))
        .collect()
    )
    return np.array(avg_age)

def group_over_avg(flight, avg_age):
    """Give group 1 if the plane age is over the middle age for the year of the flight, 0 if not.

    Parameters
    ----------
    flight : Flight tuple
             The flight to get the group of.

    avg_age : array-like (nb_year, 2)
              Contains planes middle age for different years.
    """
    avg_age_year = avg_age[avg_age[:,0]==flight.year, 1]
    return 1 if flight.plane_age > avg_age_year else 0

def delay_over_avg_age_year(D, avg_age):
    """Compute means and standard deviations for two groups of planes : plane older than the middle age and younger for different years."""
    D = get_D_clean(D)
    delay_group = (
        D.map(lambda f: ((group_over_avg(f, avg_age), f.year), np.array([1, f.ArrDelay, f.DepDelay, f.ArrDelay**2, f.DepDelay**2])))
        .reduceByKey(lambda x, y: x + y)
        .map(_comp_mean_std)
        .collect()
    )
    years = np.array([year for ((group, year), (mean_ArrDelay, std_ArrDelay, mean_DepDelay, std_DepDelay, count_group)) in delay_group])
    years = np.unique(years)

    dico_ArrDelay = {}
    dico_DepDelay = {}
    for year in years:
        dico_ArrDelay[f"mean-{year}"] = np.zeros(2)
        dico_DepDelay[f"mean-{year}"] = np.zeros(2)
        dico_ArrDelay[f"count-{year}"] = np.zeros(2)
        dico_DepDelay[f"count-{year}"] = np.zeros(2)
        dico_ArrDelay[f"std-{year}"] = np.zeros(2)
        dico_DepDelay[f"std-{year}"] = np.zeros(2)

    df_ArrDelay = pd.DataFrame(data=dico_ArrDelay)
    df_DepDelay = pd.DataFrame(data=dico_DepDelay)
    for ((group, year), (mean_ArrDelay, std_ArrDelay, mean_DepDelay, std_DepDelay, count_group)) in delay_group:
        df_ArrDelay.loc[group, f"mean-{year}"] = mean_ArrDelay
        df_ArrDelay.loc[group, f"count-{year}"] = count_group
        df_ArrDelay.loc[group, f"std-{year}"] = std_ArrDelay

        df_DepDelay.loc[group, f"mean-{year}"] = mean_DepDelay
        df_DepDelay.loc[group, f"count-{year}"] = count_group
        df_DepDelay.loc[group, f"std-{year}"] = std_DepDelay
    return df_ArrDelay, df_DepDelay

def view_delay_over_avg_one_year(mean_ArrDelay, mean_DepDelay, std_ArrDelay, std_DepDelay):
    """Save barplots which displays means and standard deviation of arrival and departure delays.

    Parameters
    ----------
    mean_ArrDelay,  mean_DepDelay: Pandas DataFrames
                                   Contains means of one year for the different age categories.

    std_ArrDelay, std_DepDelay : Panda DataFrames
                                 Contains standard deviations of one year for the different age categories.
    """
    mean_ArrDelay = mean_ArrDelay.to_numpy()
    mean_DepDelay = mean_DepDelay.to_numpy()
    std_ArrDelay = std_ArrDelay.to_numpy()
    std_DepDelay = std_DepDelay.to_numpy()
    width = 0.35
    x = np.arange(2)
    plt.figure(figsize=(10,5))
    plt.xticks(x, ('Planes =< Average age', 'Planes > Average age'))
    plt.bar(x - width/2, mean_ArrDelay, width, label='Arrival', yerr=std_ArrDelay)
    plt.bar(x + width/2, mean_DepDelay, width, label='Departure', yerr=std_DepDelay)
    plt.legend()
    plt.title('Average arrival and departure delay')
    plt.xlabel('Categories', fontsize=12)
    plt.ylabel('Average arrival and departure delay', fontsize=12)
    plt.savefig('avg_delay_over_avg.png')

# Get RDD with 2007 data
sc, D = get_RDD_from_flight_data(["/project_data/2007.csv"])
# Calculate means and standard deviations of arrival and departure delays in 2007 for different age groups
df_ArrDelay, df_DepDelay = avg_delay_per_age_group(D)
view_results_one_year(df_ArrDelay.loc[:,"mean-2007"], df_DepDelay.loc[:,"mean-2007"], df_ArrDelay.loc[:,"std-2007"], df_DepDelay.loc[:,"std-2007"])
# Calculate middle age of plane in 2007
avg_age = avg_age_plane_year(D)
# Calculate means and standard deviations of arrival and departure delays in 2007 for planes older and younger than the middle age
df_ArrDelay, df_DepDelay = delay_over_avg_age_year(D, avg_age)
view_delay_over_avg_one_year(df_ArrDelay.loc[:,"mean-2007"], df_DepDelay.loc[:,"mean-2007"], df_ArrDelay.loc[:,"std-2007"], df_DepDelay.loc[:,"std-2007"])
