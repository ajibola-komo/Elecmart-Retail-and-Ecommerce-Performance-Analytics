from src.config.constants import MONTH_WEIGHTS, MONTH_NUMBERS, BASE_TRANSACTION_TIME_STAMP, CURRENT_TIMESTAMP
import numpy as np
import pandas as pd

def generate_month_distribution(num_of_records):
    date_range = pd.date_range(
        start=BASE_TRANSACTION_TIME_STAMP,
        end=CURRENT_TIMESTAMP - pd.Timedelta(days=7),
        freq='D'
    )

    date_weights = np.array([
        MONTH_WEIGHTS[date.month - 1] for date in date_range
    ])

    date_weights = date_weights / date_weights.sum()

    sampled_dates = np.random.choice(
        date_range,
        size=num_of_records,
        p=date_weights
    )

    random_seconds = np.random.randint(0, 86400, size=num_of_records)
    seasonal_timestamps = sampled_dates + pd.to_timedelta(random_seconds, unit='s')

    return pd.to_datetime(seasonal_timestamps)