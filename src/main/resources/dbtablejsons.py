#usr/env/python
"""
this script populates the various db tables in corresponding phpfiles . . .
usage: python dbtablejsons.py configForServer.ini
"""

import traceback
import math
import csv
import sys
import json
import ast
import time
import sqlalchemy
from sqlalchemy import *
from sqlalchemy import func
import collections
import smtplib
import sendgrid
from datetime import *
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pickle
import logging
from decimal import Decimal
import requests

from tornado.concurrent import Future
from tornado import gen
from tornado.options import define, options, parse_command_line

import tornado.httpserver
import tornado.escape
import tornado.ioloop
import tornado.escape
import tornado.web
import tornado.auth

import ConfigParser

import MySQLdb
import os.path

import pandas as pd

from datetime import datetime

config_file = sys.argv[1]
Config = ConfigParser.ConfigParser()
Config.read(config_file)

def ConfigSectionMap(section):
    dict1 = {}
    options = Config.options(section)
    for option in options:
        try:
            dict1[option] = Config.get(section, option)
            if dict1[option] == -1:
                DebugPrint("skip: %s" % option)
        except:
            traceback.print_exc()
            print("exception on %s!" % option)
            dict1[option] = None
    return dict1

global alloweddbs

alloweddbs = str(ConfigSectionMap("app")['alloweddbs'])

alloweddbs_list = alloweddbs.split(',')

for alloweddb in alloweddbs_list:
    foutname = 'web/static/tables_'+alloweddb+'.php'

    port = ConfigSectionMap(alloweddb)['port']
    password = ConfigSectionMap(alloweddb)['password']
    user = ConfigSectionMap(alloweddb)['user']
    host = ConfigSectionMap(alloweddb)['host']
    conntype = ConfigSectionMap(alloweddb)['conntype']
    db = ConfigSectionMap(alloweddb)['db']
    entity = ConfigSectionMap(alloweddb)['entity']

    dbengine = create_engine(str(entity)+"://"+str(user)+":"+str(password)+"@"+str(host)+":"+str(port)+"/"+str(db), echo= False)
    dbconn = dbengine.connect()
    metadata = MetaData(dbengine)

    all_tables_query = "show tables;"
    all_tables_result = dbconn.execute(all_tables_query)
    all_tables_list = all_tables_result.fetchall()
    arr_of_table_jsons = []
    for table in all_tables_list:
        d_to_insert = {}
        tablename = tuple(table)[0]
        print "[DEBUG] the table being inserted == "+str(tablename)
        d_to_insert["table"] = tablename
        arr_of_table_jsons.append(d_to_insert)
    big_fat_dbtables_dict = {}
    big_fat_dbtables_dict["tables"] = arr_of_table_jsons

    with open(foutname, 'w') as outfile:
        json.dump(big_fat_dbtables_dict, outfile, sort_keys = True, indent = 4, ensure_ascii=False)
    
    outfile.close()
    dbconn.close()