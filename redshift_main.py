import sys
sys.path.insert(0, './')

import configparser
import psycopg2
import pandas as pd
from sql_queries import *
import time
from create_tables import main
from etl import etl_main
from infra_control import createCluster, checkCluster, deleteCluster




if __name__ == "__main__":
    
    print('Checking if cluster exists. If not, then create it.')
    if checkCluster() == -1:
        createCluster()

    while checkCluster() == -2:
        time.sleep(5)
        print('Waiting for cluster to be ready ...', end='\r')
    
    print('Cluster ready, getting endpoint.')
    end_point = checkCluster()

    print('\n\nAll good, commencing ETL.')
    main(create_table_queries, drop_table_queries)
    etl_main(copy_table_queries, insert_table_queries)