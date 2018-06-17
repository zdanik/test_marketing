import csv
import os
from collections import namedtuple
from datetime import datetime, timedelta
import time

from src.config import (
    INSTALLS_PATH,
    PROJECT_ROOT,
    PURCHASES_PATH,
)
from src.config import (
    END_STEP_IN_SECONDS,
    OFFSET_IN_SECONDS,
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
    data = []
    installs = namedtuple('installs', 'created mobile_app country')
    for created, mobile_app, country in load_installs_data():
        if is_data_in_cohort(created, mobile_app):
            _install = installs(created, mobile_app, country)
            data.append(_install)
    return data


def get_installs_intervals_by_country():
    """
    Filter installs data by country.
    Create dict with key <country> and value <number of installs>
    :return: dict
    """
    dict_data = {}
    installs = get_installs_filtered_by_parameters()
    for install in installs:
        if not dict_data.get(install.country):
            dict_data[install.country] = [install]
        else:
            dict_data[install.country].append(install)
    for key, value in dict_data.items():
        dict_data[key] = len(value)
    return dict_data


def filter_purchases_data_by_cohort_parameters():
    """
    Filter incoming data by parameters:
        date of installs
        identifier of app
    Save filtered data to csv file
    :return: path to csv file
    """
    result_file = (os.path.join(PROJECT_ROOT, 'etc/filter_results/filtered_purchases.csv'))
    purchases = [('created', 'mobile_app', 'country', 'install_date', 'revenue',)]
    for created, mobile_app, country, install_date, revenue in load_purchases_data(PURCHASES_PATH):
        if is_data_in_cohort(install_date, mobile_app):
            purchases.append((created, mobile_app, country, install_date, revenue,))
    cohort_purchases = open(result_file, 'w+')
    with cohort_purchases:
        writer = csv.writer(cohort_purchases)
        writer.writerows(purchases)
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
    _purchases_file = filter_purchases_data_by_cohort_parameters()
    period_time_in_seconds = OFFSET_IN_SECONDS
    result = {}
    purchases = namedtuple('purchases', 'created mobile_app country install_date revenue')
    cnt = 0
    while period_time_in_seconds < END_STEP_IN_SECONDS:
        internal_start = time.time()
        data = []
        for created, mobile_app, country, install_date, revenue in load_purchases_data(_purchases_file):
            if (START_FILTER_DATE <= datetime.strptime(created, '%Y-%m-%d %H:%M:%S')
                    <= (START_FILTER_DATE + timedelta(seconds=period_time_in_seconds))):
                _purchase = purchases(created, mobile_app, country, install_date, revenue)
                data.append(_purchase)
        revenue_by_country = {}
        period_data = {}
        for purchase_info in data:
            if not period_data.get(purchase_info.country):
                period_data[purchase_info.country] = [purchase_info]
            else:
                period_data[purchase_info.country].append(purchase_info)

        for country, value in period_data.items():
            revenue = 0
            for purchase in value:
                revenue += float(purchase.revenue)
                revenue_by_country[country] = revenue
        result[period_time_in_seconds] = revenue_by_country
        period_time_in_seconds += OFFSET_IN_SECONDS
        cnt += 1
        internal_end = time.time()
        print("Cycle time %s, counter %s" % (internal_end - internal_start, cnt))
    return result
