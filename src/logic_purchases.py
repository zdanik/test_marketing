import os
import csv
import json
from datetime import datetime, timedelta
from collections import namedtuple


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
FILE_PATH = (os.path.join(PROJECT_ROOT, 'etc/purchases.csv'))


def load_purchases_data(csv_file):
    with open(csv_file, "r") as f:
        datareader = csv.reader(f)
        next(datareader)
        for created, mobile_app, country, install_date, revenue in datareader:
            yield created, mobile_app, country, install_date, revenue


def filter_purchases_data_by_cohort_parameters():
    """
    Filter incoming data by parameters:
        date of installs
        identifier of app
    Save filtered data to csv file
    :return: path to csb file
    """
    result_file = (os.path.join(PROJECT_ROOT, 'etc/filter_results/filtered_purchases_csv.csv'))
    data = []
    for created, mobile_app, country, install_date, revenue in load_purchases_data(FILE_PATH):
        if (datetime.strptime('2016-05-02 00:00:00', '%Y-%m-%d %H:%M:%S')
                <= datetime.strptime(install_date, '%Y-%m-%d %H:%M:%S')
                <= datetime.strptime('2016-05-09 23:59:59',
                                     '%Y-%m-%d %H:%M:%S')) \
                and int(mobile_app) == 2:
            data.append((created, mobile_app, country, install_date, revenue,))
    cohort_purchases = open(result_file, 'w+')
    with cohort_purchases:
        writer = csv.writer(cohort_purchases)
        writer.writerows(data)
    return result_file


def get_revenue_by_period_files():
    """
    Count revenue for each period and country and store results
    into json file
    :return: list of file paths
    """
    # _purchases_file = filter_purchases_data_by_cohort_parameters()
    _purchases_file = (os.path.join(PROJECT_ROOT, 'etc/filter_results/filtered_purchases_csv.csv'))
    period_time_in_seconds = 24 * 60 * 60
    files = []
    while period_time_in_seconds <= 24 * 60 * 60:
        data = []
        for created, mobile_app, country, install_date, revenue in load_purchases_data(_purchases_file):
            if (datetime.strptime('2016-05-02 00:00:00', '%Y-%m-%d %H:%M:%S')
                    <= datetime.strptime(created, '%Y-%m-%d %H:%M:%S')
                    <= (datetime.strptime('2016-05-02 00:00:00', '%Y-%m-%d %H:%M:%S')
                        + timedelta(seconds=period_time_in_seconds))):
                purchases = namedtuple('purchases', 'created mobile_app country install_date revenue')
                _purchase = purchases(created, mobile_app, country, install_date, revenue)
                data.append(_purchase)
        revenue_by_country = {}
        period_data = {}
        for purchase_info in data:
            if not period_data.get(purchase_info.country):
                period_data[purchase_info.country] = [purchase_info]
            else:
                period_data[purchase_info.country].append(purchase_info)

        for key, value in period_data.items():
            revenue = 0
            for purchase in value:
                revenue += float(purchase.revenue)
                revenue_by_country[key] = revenue
        result_file = (os.path.join(
            PROJECT_ROOT,
            'etc/filter_results/{0}_period_revenue.json'.format(period_time_in_seconds))
        )
        files.append(result_file)
        with open(result_file, 'w+') as result_file:
            json.dump(revenue_by_country, result_file)

        period_time_in_seconds += 24 * 60 * 60
    return files

# def prepare_data():
#     with open((os.path.join(PROJECT_ROOT, 'etc/filter_results/86400_period_revenue.json'))) as f:
#         revenue_data = json.load(f)
#     with open((os.path.join(PROJECT_ROOT, 'etc/filter_results/installs_count_by_realm.json') ))as f:
#         installs_data = json.load(f)
#     result = {}
#     for country in revenue_data.keys():
#         result[country] = revenue_data[country] / installs_data[country]
#     print(result)
#
# prepare_data()
