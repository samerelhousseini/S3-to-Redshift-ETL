import configparser
import psycopg2


def load_staging_tables(cur, conn, copy_table_queries):
    """
    Load data from the log files into the staging tables
    """
    for query in copy_table_queries:
        if query == '' or query == '\n': continue
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn, insert_table_queries):
    """
    ETL from the staging tables into the final tables
    """
    for query in insert_table_queries:
        if query == '' or query == '\n': continue
        cur.execute(query)
        conn.commit()


def etl_main(copy_table_queries, insert_table_queries):
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn, copy_table_queries)
    insert_tables(cur, conn, insert_table_queries)

    conn.close()


if __name__ == "__main__":
    etl_main([], [])