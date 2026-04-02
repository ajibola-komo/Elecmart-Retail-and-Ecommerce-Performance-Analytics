from collections import defaultdict

def segment_stores(conn):
    warehouse_df = conn.execute("""
        SELECT store_id, location_id 
        FROM dim_store 
        WHERE store_type = 'Warehouse'
    """).df()

    physical_df = conn.execute("""
        SELECT store_id, location_id 
        FROM dim_store 
        WHERE store_type != 'Warehouse'
    """).df()

    warehouse_map = defaultdict(list)
    for loc, store in zip(warehouse_df['location_id'], warehouse_df['store_id']):
        warehouse_map[loc].append(store)

    physical_map = defaultdict(list)
    for loc, store in zip(physical_df['location_id'], physical_df['store_id']):
        physical_map[loc].append(store)

    return {
        'warehouse_store_map': warehouse_map,
        'physical_store_map': physical_map
    }