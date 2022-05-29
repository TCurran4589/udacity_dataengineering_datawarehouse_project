import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import configparser

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

def drop_tables(cur, conn):
    """_summary_
    Takes all tables listed in the sql_queries.py file in the drop tables array and drops
    them from the database
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """_summary_
    Takes all tables listed in the sql_queries.py file in the create tables array and creates
    them in the database
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """_summary_
    Executes the create_tables and drop_tables functions witht he provided configuration variables
    from dwh.cfg
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()