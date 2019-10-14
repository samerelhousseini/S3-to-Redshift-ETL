import configparser
import psycopg2


def drop_tables(cur, conn, drop_table_queries):
    """
    Drop all tables. Handy for development purposes.
    """
    for query in drop_table_queries:
        if query == '' or query == '\n': continue
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn, create_table_queries):
    """
    Create all tables: staging and final
    """
    for query in create_table_queries:
        if query == '' or query == '\n': continue
        cur.execute(query)
        conn.commit()


def main(create_table_queries, drop_table_queries):
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn, drop_table_queries)
    create_tables(cur, conn, create_table_queries)

    conn.close()


if __name__ == "__main__":
    main([], [])