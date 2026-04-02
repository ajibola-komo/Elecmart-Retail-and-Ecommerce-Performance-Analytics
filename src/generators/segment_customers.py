import duckdb as db
import numpy as np
import pandas as pd


def generate_customer_segments(conn):
    all_customers = conn.execute(
        '''SELECT customer_id, signup_date, location_id, loyalty_status, customer_persona FROM dim_customer
        order by signup_date desc'''
    ).df()

    all_subset_size = int(len(all_customers) * 0.25)

    subset_size = int(len(all_customers) * 0.15) 

    def safe_sample(df, n):
        if len(df) == 0:
            return df 
        return df.sample(n=min(n, len(df)), random_state=42)
    
    all_customers = safe_sample(
        all_customers.sort_values('signup_date',ascending = False), all_subset_size
    )

    premium_customers = safe_sample(
        all_customers[all_customers['customer_persona'] == 'Premium Shopper'].sort_values('signup_date', ascending=False),
        subset_size
    )

    mid_customers = safe_sample(
        all_customers[all_customers['customer_persona'].isin(['Tech Enthusiast','Everyday Shopper'])].sort_values('signup_date', ascending=False),
        subset_size
    )

    basic_customers = safe_sample(
        all_customers[all_customers['customer_persona'].isin(['Bargain Hunter','Gift Shopper'])].sort_values('signup_date', ascending=False),
        subset_size
    )

    return {
        "all_customers": all_customers,
        "premium": premium_customers,
        "mid": mid_customers,
        "basic": basic_customers
    }