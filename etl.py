import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import re

from create_tables import drop_tables, create_tables

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():

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