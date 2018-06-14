import os
import csv
import json
from collections import namedtuple
from datetime import datetime

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
FILE_PATH = (os.path.join(PROJECT_ROOT, 'etc/installs.csv'))


def load_installs_data(csv_file=FILE_PATH):
    with open(csv_file, "r") as f:
        datareader = csv.reader(f)
        next(datareader)
        for created, mobile_app, country in datareader:
            yield created, mobile_app, country


def filter_installs_data_by_cohort_parameters():
    """
    Filter incoming data by parameters:
        date of installs
        identifier of app
    :return: list of namedtuples
    """
    data = []
    for created, mobile_app, country in load_installs_data():
        if (datetime.strptime('2016-05-02 00:00:00', '%Y-%m-%d %H:%M:%S')
                <= datetime.strptime(created, '%Y-%m-%d %H:%M:%S')
                <= datetime.strptime('2016-05-09 23:59:59', '%Y-%m-%d %H:%M:%S')) \
                and int(mobile_app) == 2:

            installs = namedtuple('installs', 'created mobile_app country')
            _install = installs(created, mobile_app, country)
            data.append(_install)
    return data


def data_installs_intervals_by_country():
    """
    Filter installs data by country.
    Create dict with key <country> and value <number of installs>
    Save result to json file
    :return: dict
    """
    result_file = (os.path.join(PROJECT_ROOT, 'etc/filter_results/installs_count_by_realm.json'))
    dict_data = {}
    data = filter_installs_data_by_cohort_parameters()
    for install in data:
        if not dict_data.get(install.country):
            dict_data[install.country] = [install]
        else:
            dict_data[install.country].append(install)
    for key, value in dict_data.items():
        dict_data[key] = len(value)
    with open(result_file, 'w+') as result_file:
        json.dump(dict_data, result_file)
    return dict_data

data_installs_intervals_by_country()


