import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Copy data from S3 to staging tables on Redshift."""

    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert data from Redshift staging tables to analytics tables on Redshift."""

    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Connect to redshift, drop any existing tables, coppy data from S3 to staging table on Redshift,
    Insert data from Redshift staging to tables on Redshift.
    Close connection
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
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()