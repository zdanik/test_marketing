import csv
import os
import time

from multiprocessing import Pool

from src.config import (
    END_STEP_IN_SECONDS,
    OFFSET_IN_SECONDS,
    PROJECT_ROOT,
    START_STEP_IN_SECONDS,
)
from src.logic import (
    get_installs_intervals_by_country,
    get_revenue_by_period,
)


def get_result_csv():
    start = time.time()
    result = []
    result_file = (os.path.join(PROJECT_ROOT, 'etc/results/results.csv'))
    result.append(
        ('country', 'installs', 'RP1', 'RP2', 'RP3', 'RP4', 'RP5', 'RP6', 'RP7', 'RP8', 'RP9', 'RP10',)
    )

    with Pool(processes=2) as pool:
        p1 = pool.apply_async(get_installs_intervals_by_country, ())
        p2 = pool.apply_async(get_revenue_by_period, ())
        installs_by_country = p1.get()
        revenue_by_period = p2.get()

    for country, installs in installs_by_country.items():
        record = [country, installs]
        for seconds in range(START_STEP_IN_SECONDS, END_STEP_IN_SECONDS, OFFSET_IN_SECONDS):
            RP = revenue_by_period[seconds][country]/installs
            record.append(RP)
        result.append(record)
    csv_file_path = open(result_file, 'w+')
    with csv_file_path:
        writer = csv.writer(csv_file_path)
        writer.writerows(result)

    end = time.time()
    print("TOTAL TIME %s" % (end-start))

if __name__ == '__main__':
    get_result_csv()
