#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from tqdm import tqdm
from datetime import datetime
from colour import Color
import seaborn as sns
import folium
from folium.plugins import TimestampedGeoJson
from sqlalchemy import create_engine, func
from sqlalchemy_utils import database_exists, create_database
from argparse import ArgumentParser



def postgresql_engine_constructer(postgrep_url):
    # connect to database
    engine = create_engine(f'{postgrep_url}db')
    print(f"running engine at {postgrep_url}db")
    if not database_exists(engine.url):
        create_database(engine.url)
    return engine


# # Extract, Transform, Load

def _remove_missing_data(df_chunk):
    nulls = df_chunk.loc[:, ['price', 'zip', 'posted_at']].isnull()
    is_null = ~(nulls['price'] | nulls['zip'] | nulls['posted_at'])
    return df_chunk[is_null]

def _drop_duplicates(df_chunk):
    return df_chunk.drop_duplicates()

def _bin_time(df_chunk):
    # assuming that daily fluctuations in price are not heavily valued in an effort to save space
    df_chunk['posted_at'] = df_chunk.posted_at.str[:10]
    df_chunk_sum = df_chunk.groupby(['posted_at',  'zip']).sum().reset_index()
    df_chunk_count = df_chunk.groupby(['posted_at', 'zip']).count().reset_index()
    df_chunk_sum['count'] = df_chunk_count['price'].values
    return df_chunk_sum

def _clean_zipcode(df_chunk):
    df_chunk['zip'] = df_chunk['zip'].astype(str)
    df_chunk['zip'] = df_chunk.zip.str.split('-', n=1)
    return df_chunk.explode('zip')

def _convert_timestamp(df_chunk):
        # convert date to datetime format for timestamp SQL compatability
    df_chunk['posted_at'] = pd.to_datetime(df_chunk['posted_at'], format="%Y-%m-%d")
    return df_chunk

def process(df_chunk_raw):
    df_chunk = df_chunk_raw.loc[:, ['posted_at', 'zip', 'price', 'lat', 'long']] 
    df_chunk = _remove_missing_data(df_chunk)
    df_chunk = _clean_zipcode(df_chunk)
    df_chunk = _bin_time(df_chunk)
    df_chunk = _convert_timestamp(df_chunk)
    df_chunk = _drop_duplicates(df_chunk)
    df_chunk.columns = ['date', 'zip_code', 'price', 'lat', 'long', 'count']
    
    return df_chunk


# not space efficent but computationally efficent (could save on space but be more computationally wasteful)
def _add_entries(df_chunk, old_entries):
    new_entries = set(list(zip(df_chunk['zip_code'].values, df_chunk['date'].values)))
    intersection = old_entries.intersection(new_entries)
    entries = old_entries.union(new_entries)
    return intersection, entries

def _update_intersections(df_chunk, intersection):
    if not intersection:
        return df_chunk
    inter = pd.DataFrame(list(intersection))
    inter.columns = ['zip_code', 'date']
    inter = pd.merge(left=inter, 
                     right=df_chunk.reset_index(),
                     how='left', 
                     left_on=['zip_code', 'date'],
                     right_on=['zip_code', 'date']
                    )

    ids = []
        
    for i, row in inter.iterrows():
        sql_query = engine.execute(f"                                    SELECT id, date,zip_code, lat, long, price, count from rental_prices                                    WHERE (zip_code='{row['zip_code']}'                                    AND date= to_timestamp('{str(row.date)}','YYYY-MM-DD hh24:mi:ss')                                    );                                    ")
        
        sql_row = sql_query.fetchall()
        assert len(sql_row) == 1, f'Malformed SQL query.{sql_row}'
        sql_row = sql_row[0]
        update_query = engine.execute(f"                                         UPDATE rental_prices                                         SET price = {row['price'] + sql_row[-2]},                                         count = {row['count'] + sql_row[-1]},                                         lat = {row['lat'] + sql_row[3]},                                         long = {row['long'] + sql_row[4]}                                         WHERE (zip_code='{row['zip_code']}'                                         AND date= to_timestamp('{str(row.date)}','YYYY-MM-DD hh24:mi:ss')                                        );                                        ")
        ids.append(row.id)
    return df_chunk.drop(ids)

def process_intersections(df_chunk, entries):
    intersection, entries = _add_entries(df_chunk, entries)
    df_chunk = _update_intersections(df_chunk, intersection)
    return df_chunk


def run_ETL(engine, csv_path, debug_chunk_index=-1):
    # assumes underlying CSV path is on machine
    
    # considered adding a STD value to illustrate variance in listing prices 
    # opted not to however in order to conserve space
    # engine.execute((
    # "DROP TABLE rental_prices; \
    # ;"))
    q = engine.execute((
        "CREATE TABLE rental_prices ( \
        id INT PRIMARY KEY, \
        date TIMESTAMP NOT NULL, \
        zip_code VARCHAR(255) NOT NULL, \
        lat DOUBLE PRECISION, \
        long DOUBLE PRECISION, \
        price INT NOT NULL, \
        count INT NOT NULL \
    );"))
    
    
    entries = set([])
    # can be parallelized with python's multiprocessing although not available with current resource
    chunksize=10**5
    reader = pd.read_csv(csv_path, chunksize=chunksize) 
    offset = 0 
    for i, chunk in tqdm(enumerate(reader)):
        processed_chunk = process(chunk)
        ids = np.arange(processed_chunk.shape[0]) + offset
        offset += processed_chunk.shape[0]
        processed_chunk['id'] = ids
        processed_chunk = processed_chunk.set_index('id')
        processed_chunk = process_intersections(processed_chunk, entries)
        processed_chunk.to_sql(
            'rental_prices',
            engine, 
            if_exists = 'append'
        )
        if i == debug_chunk_index:
            break

    print('ETL complete')

def main(*args):
    csv_path = args[0]
    postgrep_url = args[1]
    
    sql_engine=postgresql_engine_constructer(postgrep_url)
    run_ETL(sql_engine, csv_path, 2)

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--csv_path', default="", help='path to source csv file.')
    parser.add_argument('--postgrep_url', default="", help='url to postgrep server')
    
    args = parser.parse_args()
    main(args.csv_path, args.postgrep_url)
