import csv
import os
import operator
import time
from collections import (
    defaultdict,
    namedtuple,
)
from datetime import datetime, timedelta

from src.config import (
    END_STEP_IN_SECONDS,
    INSTALLS_PATH,
    OFFSET_IN_SECONDS,
    PROJECT_ROOT,
    PURCHASES_PATH,
    START_FILTER_DATE,
)
from src.helpers import is_data_in_cohort


def load_installs_data(csv_file=INSTALLS_PATH):
    with open(csv_file, "r") as f:
        datareader = csv.reader(f)
        next(datareader)
        for created, mobile_app, country in datareader:
            yield created, mobile_app, country


def load_purchases_data(csv_file):
    with open(csv_file, "r") as f:
        datareader = csv.reader(f)
        next(datareader)
        for created, mobile_app, country, install_date, revenue in datareader:
            yield created, mobile_app, country, install_date, revenue


def get_installs_filtered_by_parameters():
    """
    Filter incoming data by parameters:
        date of installs
        identifier of app
    :return: list of namedtuples
    """
    started = time.time()
    print('get_installs_filtered_by_parameters START: %s' % started)
    data = []
    installs = namedtuple('installs', 'created mobile_app country')
    for created, mobile_app, country in load_installs_data():
        if is_data_in_cohort(created, mobile_app):
            _install = installs(created, mobile_app, country)
            data.append(_install)
    print('get_installs_filtered_by_parameters Finished in %s' % (time.time() - started))
    return data


def get_installs_intervals_by_country():
    """
    Filter installs data by country.
    Create dict with key <country> and value <number of installs>
    :return: dict
    """
    started = time.time()
    print('get_installs_intervals_by_country START: %s' % started)
    dict_data = {}
    installs = get_installs_filtered_by_parameters()
    for install in installs:
        if not dict_data.get(install.country):
            dict_data[install.country] = [install]
        else:
            dict_data[install.country].append(install)
    for key, value in dict_data.items():
        dict_data[key] = len(value)
    print('get_installs_intervals_by_country Finished in %s' % (time.time() - started))
    return dict_data


def filter_purchases_data_by_cohort_parameters():
    """
    Filter incoming data by parameters:
        date of installs
        identifier of app
    Save filtered data to csv file
    :return: path to csv file
    """
    started = time.time()
    print('filter_purchases_data_by_cohort_parameters START: %s' % started)
    result_file = (os.path.join(PROJECT_ROOT, 'etc/results/filtered_purchases.csv'))
    purchases = [('created', 'mobile_app', 'country', 'install_date', 'revenue',)]
    for created, mobile_app, country, install_date, revenue in load_purchases_data(PURCHASES_PATH):
        if is_data_in_cohort(install_date, mobile_app):
            purchases.append((created, mobile_app, country, install_date, revenue,))
    cohort_purchases = open(result_file, 'w+')
    with cohort_purchases:
        writer = csv.writer(cohort_purchases)
        writer.writerows(purchases)
    print('filter_purchases_data_by_cohort_parameters Finished in %s' % (time.time() - started))
    return result_file


def get_revenue_by_period():
    """
    Count revenue for each period and country and store results
    into the dict. Example:
    {
     86400: {'US': 1, 'UK': 9...},
     172800: {'US': 2, 'UK': 33, .....}
     }
    :return: dict
    """
    started = time.time()
    _purchases_file = filter_purchases_data_by_cohort_parameters()
    result = {}
    with open(_purchases_file, "r") as f:
        datareader = csv.reader(f)
        next(datareader)
        res = sorted(datareader, key=operator.itemgetter(0))
    print('get_revenue_by_period START: %s' % started)
    per_date = defaultdict(dict)
    prev_period_in_seconds = 0
    cur_rpi_date = START_FILTER_DATE.date()
    period_in_seconds = 24 * 60 * 60
    for created, mobile_app, country, install_date, revenue in res:
        date_ = datetime.strptime(created, '%Y-%m-%d %H:%M:%S')
        if cur_rpi_date < date_.date():
            prev_period_in_seconds = period_in_seconds
            result[prev_period_in_seconds] = per_date
            per_date = defaultdict(dict)
            cur_rpi_date = date_.date()
            period_in_seconds += OFFSET_IN_SECONDS
        if per_date[country]:
            per_date[country] += float(revenue)
        else:
            per_date[country] = float(revenue) if period_in_seconds == OFFSET_IN_SECONDS \
                else float(revenue) + result[prev_period_in_seconds][country]
    print('get_revenue_by_period Finished in %s' % (time.time() - started))
    return result
