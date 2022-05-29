import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import re

from create_tables import drop_tables, create_tables

def load_staging_tables(cur, conn):
    """_summary_
    Loads raw data from .json files in S3 bucket and loads it into 
    two staging tables: songs and events
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts data from the staging tables into the dimension and
    fact tables create in create_tables.py
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Runs load_staging_tables and insert_tables. Before peforming those two actions
    it will drop and re-create the tables to prevent duplicate records being created
    from the same file. 
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    cnx = "host={} dbname={} user={} password={} port={}".format(
        config.get('CLUSTER','HOST'),
        config.get('CLUSTER','DB_NAME'),
        config.get('CLUSTER','DB_USER'),
        config.get('CLUSTER','DB_PASSWORD'),
        config.get('CLUSTER','DB_PORT'),
    )

    conn = psycopg2.connect(cnx)
    cur = conn.cursor()

    print('Dropping existing tables...')
    drop_tables(cur, conn)

    print("Creating tables...")
    create_tables(cur, conn)

    try:
        print('loading staging tables...')
        load_staging_tables(cur, conn)
    except Exception as e:
        print(e)

    print('-'*100)
    print('Inserting staging tables into dim and fact tables...')
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()