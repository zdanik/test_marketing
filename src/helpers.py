from datetime import datetime

from src.config import (
    END_FILTER_DATE,
    START_FILTER_DATE,
)

__all__ = (
    'is_data_in_cohort',
)


def is_data_in_cohort(date, mobile_app):
    return START_FILTER_DATE <= datetime.strptime(date, '%Y-%m-%d %H:%M:%S') <= END_FILTER_DATE and int(mobile_app) == 2
