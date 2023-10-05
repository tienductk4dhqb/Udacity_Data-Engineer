import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Remove all tables."""

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create all tables."""
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Connect to redshift, drop any existing tables, create new table. Close connection
    Keyword arguments:
    * cur --    cursory to connected DB. Allows to execute SQL commands.
    * conn --   (psycopg2) connection to Postgres database (sparkify).
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
            "host={} dbname={} user={} password={} port={}".format(*config['DWH'].values())
    )
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()