import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
     Drop all tables in given list    
    Args:
        cur (class): database binding
        conn: database connection
    """    
    for query in drop_table_queries:   
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
     Create db tables from list of lables    
    Args:
        cur (class): database binding
        conn: database connection
    """    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('|------| Connected to Redshift successful! |------|')

    drop_tables(cur, conn)
    print('|------| Drop any existing tables! |------|')

    create_tables(cur, conn)
    print('|------| New tables created! |------|')

    conn.close()


if __name__ == "__main__":
    main()