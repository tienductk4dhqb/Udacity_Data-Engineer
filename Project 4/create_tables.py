import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop any existing tables from dev.
    
    Keyword arguments:
    * cur --    cursory to connected DB. Allows to execute SQL commands.
    * conn --   (psycopg2) connection to AWS Redshift database (dev).
    Output:
    * Old dev database tables are dropped from AWS Redshift.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print("Drop all table success")


def create_tables(cur, conn):
    """Create new tables (songplays, users, artists, songs, time)
        to dev.
        
    Keyword arguments:
    * cur --    cursory to connected DB. Allows to execute SQL commands.
    * conn --   (psycopg2) connection to AWS Redshift database (dev).
    Output:
    * New dev database tables are created into AWS Redshift.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print("Create all table success")


def main():
    """Connect to AWS Redshift, create new DB (dev),
        drop any existing tables, create new tables. Close DB connection.
        
    Keyword arguments (from dwh.cfg):
    * host --       AWS Redshift cluster address.
    * dbname --     DB name.
    * user --       Username for the DB.
    * password --   Password for the DB.
    * port --       DB port to connect to.
    * cur --        cursory to connected DB. Allows to execute SQL commands.
    * conn --       (psycopg2) connection to AWS Redshift database (dev).
    Output:
    * New dev is created, old tables are droppped,
        and new tables (songplays, users, artists, songs, time)
    """
    config = configparser.ConfigParser()
    config.read('airflow/dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".
                            format(*config['CLUSTER'].values()))
    print(conn)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
