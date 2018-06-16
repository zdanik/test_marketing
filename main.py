import csv
import os

from src.config import PROJECT_ROOT
from src.logic import (
    get_installs_intervals_by_country,
    get_revenue_by_period,
)


def get_result_csv():
    result = []
    result_file = (
    os.path.join(PROJECT_ROOT, 'etc/filter_results/results_csv.csv')
    )
    result.append(
        ('country', 'installs', 'RP1', 'RP2', 'RP3', 'RP4', 'RP5', 'RP6', 'RP7', 'RP8', 'RP9', 'RP10',)
    )
    installs_by_country = get_installs_intervals_by_country()
    revenue_by_period = get_revenue_by_period()

    for country in installs_by_country.keys():
        record = []
        record.append(country)
        installs = installs_by_country[country]
        record.append(installs)
        for data in range(24 * 60 * 60, 11 * 24 * 60 * 60, 24 * 60 * 60):
            RP = revenue_by_period[data][country]/installs
            record.append(RP)
        result.append(record)
    csv_file = open(result_file, 'w+')
    with csv_file:
        writer = csv.writer(csv_file)
        writer.writerows(result)

if __name__ == '__main__':
    get_result_csv()
