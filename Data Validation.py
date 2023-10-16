import requests
import json
import time
import numpy
import pandas
import datetime
import psycopg2
from tabulate import tabulate


## Add plan names to the list
plan_list  = [
    "ACH",
    "CHI_SV_Health_Plan",
    "MSSP Track 2",
    "ABCBS_CHI",
    "CHISV",
    "Humana_MA",
    "EngageMED",
    "Nabholz"
]

webhook_url = 'https://hooks.slack.com/services/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'

config_pg = {
    "dbname": "dap",
    "host": "arkansashealth-prod-rds.cluster-cqa4okalp3qn.us-east-1.rds.amazonaws.com", ### PG 
    "port": "5432",
    "user": "batman",
    "password": "Test#123"
}

config_rs = {
    "dbname": "arkansashealth_prod",
    "host": "arkansashealth-prod-redshift.cg8mcnhmisj5.us-east-1.redshift.amazonaws.com", # RS
    "port": "5439",
    "user": "batman",
    "password": "Test#123"
}

table_list = [
    "l2.pd_activity"
]

class Runner(object):
    @staticmethod
    def runner(file_object):

        con_pg = psycopg2.connect(dbname=config_pg["dbname"], host = config_pg["host"], port=\
            config_pg["port"], user = config_pg["user"], password = config_pg["password"])

        cur_pg = con_pg.cursor()

        result = []

        for table in table_list:
            
            for plan in plan_list:

                cur_pg.execute(f"select count(1) from {table} where sstp = '{plan}';") # lund
                test2 = str(cur_pg.fetchall()[0][0])

                result.append([plan, test2])

        np_array1 = numpy.asarray(result)

        cur_pg.close()
        con_pg.close()

        con_rs = psycopg2.connect(dbname=config_rs["dbname"], host = config_rs["host"], port=\
            config_rs["port"], user = config_rs["user"], password = config_rs["password"])

        cur_rs = con_rs.cursor()

        result = []

        for table in table_list:

            for plan in plan_list:

                cur_rs.execute(f"select count(1) from {table} where sstp = '{plan}';") # bund
                test1 = str(cur_rs.fetchall()[0][0])

                result.append([test1])

        np_array2 = numpy.asarray(result)

        cur_rs.close()
        con_rs.close()

        np_array  = numpy.concatenate((np_array1, np_array2), axis = 1)
        
        frame = pandas.DataFrame(np_array, columns = ['Plan' , 'Count PG', 'Count REDSHIFT'])
        
        message = {
            "text": "L2 ACTIVITY\n" +"```"+ str(tabulate(frame,headers='keys',tablefmt=\
                'psql', showindex = False)) + "```" + "\n:pushpin: *Note*: Counts must match"
        }

        requests.post(webhook_url,json=message, headers={'Content-Type': 'application/json'})

        yield '1'