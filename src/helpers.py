from datetime import datetime

__all__ = (
    'is_data_in_cohort',
)


def is_data_in_cohort(date, mobile_app):
    return ((datetime.strptime('2016-05-02 00:00:00', '%Y-%m-%d %H:%M:%S')
             <= datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
             <= datetime.strptime('2016-05-09 23:59:59', '%Y-%m-%d %H:%M:%S'))
            and int(mobile_app) == 2)
