import duckdb as db
import numpy as np
from numpy.ma import ids
import pandas as pd
from src.generators.segment_customers import generate_customer_segments
from src.config.paths import (TRANSACTIONS_DDL_PATH, TRANSACTIONS_CSV_PATH, TRANSACTIONS_PARQUET_PATH)
from src.config.constants import (PAYMENT_TYPES,TRANSACTION_STATUSES, PAYMENT_TYPES_WEIGHTS, TRANSACTION_WEIGHTS, PROB_OF_REPEATED_SESSION)

def generate_transactions(conn):
    
    create_db = TRANSACTIONS_DDL_PATH.read_text()

    conn.execute(create_db)

    conn.execute(f"DELETE FROM FACT_TRANSACTION")

    sales_data = conn.execute("select transaction_id, session_id, transaction_timestamp, aov_category, sum(line_total) as transaction_subtotal from fact_sale " \
    "group by transaction_id, session_id, transaction_timestamp, aov_category").df()

    aov_categories = sales_data['aov_category'].values

    total_items = conn.execute("select transaction_id, count(product_id) as item_count from fact_sale group by transaction_id").df()
    items_count_per_transaction = total_items['item_count']
    transaction_items_count_dict = dict(zip(total_items['transaction_id'], items_count_per_transaction))

    sessions_data = conn.execute('select session_id, customer_id, session_start_time, session_end_time, campaign_id from fact_clickstream where purchased_flag = TRUE').df()
    sessions_session_ids = sessions_data['session_id'].values
    session_customer_ids = sessions_data['customer_id'].values
    session_campaign_ids = sessions_data['campaign_id'].values
    cust_session_dict = dict(zip(sessions_session_ids, session_customer_ids))
    camp_session_dict = dict(zip(sessions_session_ids, session_campaign_ids))

    customer_segments = generate_customer_segments(conn)

    all_customers = customer_segments["all_customers"]
    premium_customers = customer_segments["premium"]
    mid_level_customers = customer_segments["mid"]
    basic_level_customers = customer_segments["basic"]

    all_customer_ids = all_customers['customer_id'].values
    all_customer_sign_up_dates = all_customers['signup_date'].values
    cust_location_dict = dict(zip(all_customers['customer_id'], all_customers['location_id']))
    
    premium_customers_ids = premium_customers['customer_id'].values
    premium_customer_sign_up_dates = premium_customers['signup_date'].values
    mid_level_customers_ids = mid_level_customers['customer_id'].values
    mid_level_customer_sign_up_dates = mid_level_customers['signup_date'].values
    basic_level_customers_ids = basic_level_customers['customer_id'].values
    basic_level_customer_sign_up_dates = basic_level_customers['signup_date'].values

    premium_customers_subset = premium_customers.sample(frac=0.25, random_state=42)
    mid_level_customers_subset = mid_level_customers.sample(frac=0.25, random_state=42)
    basic_level_customers_subset = basic_level_customers.sample(frac=0.25, random_state=42)

    premium_subset_ids = premium_customers_subset['customer_id'].values
    premium_subset_signup_dates = premium_customers_subset['signup_date'].values
    mid_subset_ids = mid_level_customers_subset['customer_id'].values
    mid_subset_signup_dates = mid_level_customers_subset['signup_date'].values
    basic_subset_ids = basic_level_customers_subset['customer_id'].values
    basic_subset_signup_dates = basic_level_customers_subset['signup_date'].values

    total_transactions = len(sales_data)


    repeated_session = np.random.rand(total_transactions) <= PROB_OF_REPEATED_SESSION

    locations_data = conn.execute("select location_id from dim_location").df()
    all_location_ids = locations_data['location_id']

    promotions_data = conn.execute('select promo_id, discount_type, discount_value from dim_promotion').df()
    all_promo_ids = promotions_data['promo_id']
    all_discount_types = promotions_data['discount_type']
    all_discount_values = promotions_data['discount_value']
    promo_disc_type = dict(zip(all_promo_ids, all_discount_types))
    promo_disc_value = dict(zip(all_promo_ids, all_discount_values))

    campaigns_data = conn.execute('select campaign_id, promo_id from dim_campaign').df()
    all_campaigns_id = campaigns_data['campaign_id'].values
    campaign_promo_ids = campaigns_data['promo_id'].values
    camp_promo_dict = dict(zip(all_campaigns_id,campaign_promo_ids))

    stores_data = conn.execute('select store_id, location_id, store_type, opening_date from dim_store').df()
    stores_location_dict = dict(zip(stores_data['location_id'], stores_data['store_id']))

    transaction_ids = sales_data['transaction_id'].values
    session_ids = sales_data['session_id'].values
    transaction_timestamps = sales_data['transaction_timestamp'].values

    session_series = sales_data['session_id']
    customers_ids = session_series.map(cust_session_dict).values.copy()

    no_customer_session = pd.isna(session_ids)

    sales_channels = np.where(no_customer_session, 'In-Store', 'Online')

    true_indexes = np.where(no_customer_session)[0]

    for idx in true_indexes:
        subset_dates_map = {
            'High': premium_subset_signup_dates,
            'Mid': mid_subset_signup_dates,
            'Low': basic_subset_signup_dates
        }

        subset_ids_map = {
            'High': premium_subset_ids,
            'Mid': mid_subset_ids,
            'Low': basic_subset_ids
        }

        category = aov_categories[idx]
        if repeated_session[idx]:
            dates = subset_dates_map.get(category, all_customer_sign_up_dates)
            ids = subset_ids_map.get(category, all_customer_ids)
        else:
            dates = {
                'High': premium_customer_sign_up_dates,
                'Mid': mid_level_customer_sign_up_dates,
                'Low': basic_level_customer_sign_up_dates
                }.get(category, all_customer_sign_up_dates)
            ids = {
                    'High': premium_customers_ids,
                    'Mid': mid_level_customers_ids,
                    'Low': basic_level_customers_ids
                }.get(category, all_customer_ids)


        txn_time_np = np.datetime64(transaction_timestamps[idx])
        eligible_mask = dates <= txn_time_np
        eligible_customers = ids[eligible_mask]

        if len(eligible_customers) == 0:
                eligible_customers = all_customer_ids

        customers_ids[idx] = np.random.choice(eligible_customers)
    
    payment_types = np.random.choice(PAYMENT_TYPES, p = PAYMENT_TYPES_WEIGHTS, size=total_transactions)

    campaign_ids = session_series.map(camp_session_dict).values.copy()
    promo_ids = pd.Series(campaign_ids).map(camp_promo_dict).values.copy()

    discount_types = pd.Series(promo_ids).map(promo_disc_type).values.copy()
    discount_values = pd.Series(promo_ids).map(promo_disc_value).values.copy()

    transaction_subtotals = sales_data['transaction_subtotal'].values

    no_promo_id = pd.isna(promo_ids)
    discount_values[no_promo_id] = float(0.0)

    fixed_discount_types = discount_types == 'Fixed_Amount_Discount'
    percentage_discount_types = discount_types == 'Percentage_Discount'

    transaction_discounts_applied = np.zeros(total_transactions, dtype = float)

    transaction_discounts_applied[fixed_discount_types] = discount_values[fixed_discount_types]
    transaction_discounts_applied[percentage_discount_types] = discount_values[percentage_discount_types] * transaction_subtotals[percentage_discount_types]

    transaction_totals = np.round(transaction_subtotals - transaction_discounts_applied,2)

    items_count = pd.Series(transaction_ids).map(transaction_items_count_dict).values

    location_ids = np.empty(total_transactions, dtype=object)

    guest_transaction = pd.isna(customers_ids)

    location_ids[guest_transaction] = np.random.choice(all_location_ids, size = np.sum(guest_transaction))

    location_ids[~guest_transaction] = pd.Series(customers_ids[~guest_transaction]).map(cust_location_dict).values

    store_ids = pd.Series(location_ids).map(stores_location_dict).values

    

    transaction_statuses = np.random.choice(TRANSACTION_STATUSES, p = TRANSACTION_WEIGHTS, size = total_transactions)

    df_raw = pd.DataFrame({
        "transaction_id":transaction_ids,
        "transaction_timestamp":transaction_timestamps,
        "transaction_date_id":pd.to_datetime(transaction_timestamps).strftime('%Y%m%d').astype(int),
        "customer_id":customers_ids,
        "store_id":store_ids,
        "sales_channel":sales_channels,
        "session_id":session_ids,
        "promo_id":promo_ids,
        "campaign_id":campaign_ids,
        "transaction_subtotal":transaction_subtotals,
        "transaction_discount_applied":transaction_discounts_applied,
        "transaction_total":transaction_totals,
        "items_count":items_count,
        "payment_type":payment_types,
        "transaction_status":transaction_statuses
    })

    conn.register("df_raw",df_raw)

    conn.execute("INSERT INTO FACT_TRANSACTION SELECT * FROM DF_RAW")

    conn.execute(f"COPY FACT_TRANSACTION TO '{TRANSACTIONS_CSV_PATH}' (FORMAT CSV, HEADER true)")

    conn.execute(f'''
                    COPY FACT_TRANSACTION TO '{TRANSACTIONS_PARQUET_PATH}' (FORMAT PARQUET)
''')




