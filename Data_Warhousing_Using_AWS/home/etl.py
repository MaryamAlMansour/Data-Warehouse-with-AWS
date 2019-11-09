import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

 
"""
load data from S3 into load_staging_tables will load the staging event table, and staging songs table on Redshift
"""
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
"""
Insert data into the dimensional tables. Sign to aws to see if your public tables got created
"""
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
"""
Connect redshift using 'dwh.cfg' creds, execute load staging tables function, and insert table function.
"""
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
  
    print('Connecting to redshift')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DWH'].values()))
    cur = conn.cursor()
    
    print('Loading staging tables')
    #load_staging_tables(cur, conn)
    
    print('Transform from staging')
    insert_tables(cur, conn)

    conn.close()
    print('ETL Ended')


if __name__ == "__main__":
    main()