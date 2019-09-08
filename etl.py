import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
     load data from S3 to Redshift    
    Args:
        cur (class): database binding
        conn: database connection
    """    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
     Insert data from staging tables to star schema
     (dimension and fact tables)    
    Args:
        cur (class): database binding
        conn: database connection
    """    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('|------| Connected to Redshift successfully! |------|')
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    print('|------| Staging tables load successfully |------|')

    insert_tables(cur, conn)
    print('|------| Star schema load successfully |------|')

    conn.close()
    print('|------| ETL successfully !! |------|')
    print('|------| connection closed !|------|')


if __name__ == "__main__":
    main()