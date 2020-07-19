from configobj import ConfigObj
from datetime import datetime
import csv
import json
import mysql.connector
import requests

config_dict = ConfigObj('config.ini')
config_mysql = config_dict['mysql']


def get_client(host: str, user: str, password: str, database: str,  **kwargs):
    """"get connection to mysql database"""
    try:
        cnx = mysql.connector.connect(user=user,
                                      password=password,
                                      host=host,
                                      database=database)
        print(f"Connected to my sql {host}. ")
        return cnx
    except Exception:
        print(f"could not connect to {host} as mysql. ")
        raise


def retrieve_reques_data(url):
    """retrieved the wanted data from the API"""
    try:
        with open('city_configuration.json') as json_city:
            data = json.load(json_city)
        url_response = []
        for city in data:
            city['alt'] = 10
            city['n'] = 50
            r = requests.get(url, params=city)
            for doc in r.json()['response']:
                url_response.append((city['city'], doc['duration'], datetime.utcfromtimestamp(doc['risetime'])))
        print(f"Retrieved url data. ")
        return url_response
    except Exception:
        print(f"could not retrive {url} data. ")
        raise


def execute_query(con: mysql.connector.connect, query: str, params= None):
    """execute my sql query"""
    try:
        cur = con.cursor()
        if params:
            cur.executemany(query, params)
            return cur

        else:
            cur.execute(query)
            return cur
        print(f"Execution of query {query} success.")
    except Exception:
        print(f"Execution of query {query} failed.")
        raise


def execute_procedure(con: mysql.connector.connect, proc_name: str):
    """execute my sql procedure"""
    try:
        con.cursor().callproc(proc_name)
        print(f"Execution of procedure {proc_name} success.")
    except Exception:
        print(f"Execution of procedure {proc_name} failed.")
        raise


def create_csv(file_name: str, column_names, rows):
    """create csv file based on mysql query result"""
    try:
        fp = open(file_name, 'w')
        myFile = csv.writer(fp, lineterminator='\n')
        myFile.writerow(column_names)
        myFile.writerows(rows)
        fp.close()
        print(f"Create csv file {file_name} - success.")
    except Exception:
        print(f"Create csv file {file_name}  - failed.")
        raise


def manage_run():
    url_response = retrieve_reques_data("http://api.open-notify.org/iss-pass.json")

    with get_client(**config_mysql) as mysql_cli:
        # Drop orbital_data_michal if exists
        sql_drop_table = 'DROP TABLE IF EXISTS orbital_data_michal'
        execute_query(mysql_cli, sql_drop_table)

        # Create orbital_data_michal table
        sql_create_table = 'CREATE TABLE orbital_data_michal (' \
                           'city varchar(50), ' \
                           'duration numeric(20), ' \
                           'runtime datetime, ' \
                           'insert_time_stamp datetime)'
        execute_query(mysql_cli, sql_create_table)

        # Insert API data
        sql_insert = 'insert into orbital_data_michal (city, duration, runtime, insert_time_stamp) ' \
                     'values (%s,%s,%s,now())'
        cur = execute_query(mysql_cli, sql_insert, url_response)
        print(cur.rowcount, "rows inserted to orbital_data_michal.")
        mysql_cli.commit()

        # Call procedure
        execute_procedure(mysql_cli, 'GetCityStatsMichal')

        # Create csv file
        sql_csv = 'SELECT cities.city, population, max_temperature, min_temperature, update_date, csm.avg_daily_flights '\
              'FROM (SELECT city, population, max_temperature, min_temperature, update_date FROM city_stats_haifa csh '\
              'UNION ALL ' \
              'SELECT city, population, max_temperature, min_temperature, update_date FROM city_stats_tel_aviv cst '\
              'UNION ALL ' \
              'SELECT city, population, max_temperature, min_temperature, update_date FROM city_stats_beer_sheva csb '\
              'UNION ALL ' \
              'SELECT city, population, max_temperature, min_temperature, update_date FROM city_stats_eilat cse) cities '\
              'LEFT JOIN city_stats_michal csm ' \
              'ON cities.city = csm.city '

        cur = execute_query(mysql_cli, sql_csv)
        rows = cur.fetchall()
        column_names = [i[0] for i in cur.description]
        create_csv('city_stats.csv', column_names, rows)


if __name__ == '__main__':
    manage_run()

