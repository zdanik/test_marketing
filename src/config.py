import os
from datetime import datetime

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
INSTALLS_PATH = (os.path.join(PROJECT_ROOT, 'etc/installs.csv'))
PURCHASES_PATH = (os.path.join(PROJECT_ROOT, 'etc/purchases.csv'))

START_FILTER_DATE = datetime.strptime('2016-05-02 00:00:00', '%Y-%m-%d %H:%M:%S')
END_FILTER_DATE = datetime.strptime('2016-05-09 23:59:59', '%Y-%m-%d %H:%M:%S')

START_STEP_IN_SECONDS = 24 * 60 * 60
END_STEP_IN_SECONDS = 11 * 24 * 60 * 60
OFFSET_IN_SECONDS = 24 * 60 * 60