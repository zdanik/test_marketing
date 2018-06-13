import csv
from datetime import datetime
from collections import namedtuple


FILE_PATH = 'incoming_files/purchases.csv'


def load_purchases_data(csv_file=FILE_PATH):
    with open(csv_file, "r") as f:
        datareader = csv.reader(f)
        next(datareader)
        for created, mobile_app, country, install_date, revenue in datareader:
            yield created, mobile_app, country, install_date, revenue


def cohort_data():
    data = []
    for created, mobile_app, country, install_date, revenue in load_purchases_data():
        if (datetime.strptime('2016-05-02 00:00:00', '%Y-%m-%d %H:%M:%S')
                <= datetime.strptime(install_date, '%Y-%m-%d %H:%M:%S')
                <= datetime.strptime('2016-05-09 23:59:59',
                                     '%Y-%m-%d %H:%M:%S')) \
                and int(mobile_app) == 2:
            data.append((created, mobile_app, country, install_date, revenue,))
    cohort_purchases = open('cohort_purchases.csv', 'w+')
    with cohort_purchases:
        writer = csv.writer(cohort_purchases)
        writer.writerows(data)

#
#
# def filter_data():
#     data = []
#     for created, mobile_app, country, install_date, revenue in load_purchases_data():
#         if (datetime.strptime('2016-05-02 00:00:00', '%Y-%m-%d %H:%M:%S')
#                 <= datetime.strptime(install_date, '%Y-%m-%d %H:%M:%S')
#                 <= datetime.strptime('2016-05-09 23:59:59',
#                                      '%Y-%m-%d %H:%M:%S')) \
#                 and int(mobile_app) == 2\
#                 and (datetime.strptime('2016-05-02 00:00:00', '%Y-%m-%d %H:%M:%S')
#                          <= datetime.strptime(created, '%Y-%m-%d %H:%M:%S')
#                          <= datetime.strptime('2016-05-02 23:59:59',
#                                               '%Y-%m-%d %H:%M:%S')):
#             purchases = namedtuple('purchases', 'created mobile_app country install_date revenue')
#             _purchase = purchases(created, mobile_app, country, install_date, revenue)
#             data.append(_purchase)
#     dict_sorted_data = {}
#     dict_data = {}
#     for row in data:
#         if not dict_data.get(row.country):
#             dict_data[row.country] = [row]
#         else:
#             dict_data[row.country].append(row)
#
#     for key, value in dict_data.items():
#         revenue = 0
#         print(key)
#         for purchase in value:
#             revenue += float(purchase.revenue)
#         dict_sorted_data[key] = revenue
#     print(dict_sorted_data)
#     return dict_data
