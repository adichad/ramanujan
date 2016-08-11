#!/usr/env/python
"""
server instead of the normal spray routes
python server.py configForServer.ini
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

global port, internalhost, internaldbname, internalusername, internalpassword

port = int(ConfigSectionMap("app")['port'])
internalhost = ConfigSectionMap("db")['internalhost']
internaldbname = ConfigSectionMap("db")['internaldbname']
internalusername = ConfigSectionMap("db")['internalusername']
internalpassword = ConfigSectionMap("db")['internalpassword']

global defaultDateTimeStr, defaultRunState, NoExceptionsStr, NoNotesStr, noInput, Zook

defaultDateTimeStr = ConfigSectionMap("defaults")['defaultDateTimeStr']
defaultRunState = ConfigSectionMap("defaults")['defaultRunState']
NoExceptionsStr = ConfigSectionMap("defaults")['NoExceptionsStr']
NoNotesStr = ConfigSectionMap("defaults")['NoNotesStr']
noInput = ConfigSectionMap("defaults")['noInput']
Zook = ConfigSectionMap("defaults")['Zook']

global mysqlport1, mysqlport2, postgresport, sqlserverport 

mysqlport1 = str(ConfigSectionMap("defaults")['mysqlport1'])
mysqlport2 = str(ConfigSectionMap("defaults")['mysqlport2'])
postgresport = str(ConfigSectionMap("defaults")['postgresport'])
sqlserverport = str(ConfigSectionMap("defaults")['sqlserverport'])

global mysqlstr, postgresstr, mssqlstr

mysqlstr = str(ConfigSectionMap("defaults")['mysqlstr'])
postgresstr = str(ConfigSectionMap("defaults")['postgresstr'])
mssqlstr = str(ConfigSectionMap("defaults")['mssqlstr'])

define("port",default=port,help="serve on the given port",type=int)
define("debug",default=False, help="running in the debug mode")

db_port_map = {}
db_port_map[mysqlport1] = mysqlstr
db_port_map[mysqlport2] = mysqlstr
db_port_map[postgresport] = postgresstr
db_port_map[sqlserverport] = sqlserverstr

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

def ss(str_):
    return "'"+str_.replace("'","")+"'"

def getUserSpecType(colname):
    # SELECT HIVE SPECIFIC TYPES ONLY e.g string integer etc etc # default STRING no mumbo jumbo
    return "none"

class BaseHandler(tornado.web.RequestHandler):
    def get_current_user(self):
        print "[DEBUG] secure cookie == "+str(self.get_secure_cookie("username"))
        return self.get_secure_cookie("username")

class DefaultHandler(tornado.web.RequestHandler):
    def get(self):
        username = self.get_argument('username', 'Ramanujan')
        userquery = "select username from users;"
        db = MySQLdb.connect(internalhost,internalusername,internalpassword,internaldbname)
        cursor = db.cursor()
        cursor.execute(userquery)
        rt = cursor.fetchall()
        list_of_users_username = []
        for r in rt:
            user = str(r[0])
            list_of_users_username.append(user)
        print "[DEBUG] username keyes in == "+str(username)
        if username not in list_of_users_username:
            retry_message = "the previous login credentials failed. Retry ?"
            self.render("login.html",message = retry_message)
        else:
            cursor = db.cursor()
            cursor.execute("SELECT host,port,dbname,dbtable,lastStarted,lastEnded,runFrequency,totalRuns,successRuns,failureRuns,currentState,exceptions,notes FROM Requests;")
            rt=cursor.fetchall()
            data = []
            for r in rt:
                data.append(r)
            if len(data) == 0:
                cols = ["host","port","dbname","dbtable","lastStarted","lastEnded","runFrequency","totalRuns","successRuns","failureRuns","currentState","exceptions","notes"]
                print "[MY DEBUG STATEMENTS] no record entry in the Requests . . ."
                df_to_show = pd.DataFrame([["N.A"] * len(cols)])
                df_to_show.columns = ["host","port","dbname","dbtable","lastStarted","lastEnded","runFrequency","totalRuns","successRuns","failureRuns","currentState","exceptions","notes"]
                health_message = "no records found ! "
            else:
                df=pd.DataFrame(data)
                df.columns = ["host","port","dbname","dbtable","lastStarted","lastEnded","runFrequency","totalRuns","successRuns","failureRuns","currentState","exceptions","notes"]
                df_to_show = df[["host","port","dbname","dbtable","runFrequency","totalRuns","successRuns","failureRuns","currentState","exceptions","notes"]]
                fault_rows = df_to_show[(df_to_show['failureRuns'] > 0)].shape[0]
                if fault_rows > 0:
                    health_message = "something\'s wrong . . ."
                else:
                    health_message = "things look good !"

            cursor = db.cursor()
            cursor.execute("SELECT cluster,topic,alias,groupName,currentState,exceptions,notes FROM kafkaRequests;")
            rt=cursor.fetchall()
            data = []
            for r in rt:
                data.append(r)
            if len(data) == 0:
                cols = ["cluster","topic","alias","groupName","currentState","exceptions","notes"]
                print "[MY DEBUG STATEMENTS] no record entry in the Kafka Requests . . ."
                kafka_df_to_show = pd.DataFrame([["N.A"] * len(cols)])
                kafka_df_to_show.columns = ["cluster","topic","alias","groupName","currentState","exceptions","notes"]
                kafka_health_message = "no kafka records found ! "
            else:
                df=pd.DataFrame(data)
                df.columns = ["cluster","topic","alias","groupName","currentState","exceptions","notes"]
                kafka_df_to_show = df[["cluster","topic","alias","groupName","currentState","exceptions","notes"]]
                kafka_health_message = "various streams in parallel !"
            self.render("index.html",Ramanujan=username,message=health_message,records=df_to_show,mesage_kafka=kafka_health_message,records_kafka=kafka_df_to_show)
            print "[MY DEBUG STATEMENTS] server up and running . . ."


class PostRequestHandlerRDBMS(tornado.web.RequestHandler):
    def post(self):
        request = tornado.escape.json_decode(self.request.body)
        requestStr = json.dumps(request)
        print "[MY DEBUG STATEMENTS] received request == "+json.dumps(request)
        internaldb = MySQLdb.connect(internalhost,internalusername,internalpassword,internaldbname)
        internalcursor = internaldb.cursor()
        print "[DEBUG] internal connection is up. . ."
        processDate = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print "[DEBUG] processDate == "+str(processDate)
        requestStr = json.dumps(request)
        print "[DEBUG] request string == "+str(requestStr)
        # compulsary fields
        host = request["host"]
        print "[DEBUG] host == "+host
        alias = request["alias"]
        print "[DEBUG] alias == "+alias
        user = request["user"]
        print "[DEBUG] user == "+user
        password = request["password"]
        print "[DEBUG] password == "+password
        port = request["port"]
        print "[DEBUG] port == "+port
        dbname = request["db"]
        print "[DEBUG] db == "+dbname
        dbtable = request["table"]
        print "[DEBUG] table == "+dbtable
        runFrequency = request["runFrequency"]
        print "[DEBUG] runFrequency == "+runFrequency
        # redundant fields
        if "druidDims" not in request.keys():
            print "[MY DEBUG STATEMENTS] enter druidDims noInput"
            request["druidDims"] = noInput
        if "druidMetrics" not in request.keys():
            print "[MY DEBUG STATEMENTS] enter druidMetrics noInput"
            request["druidMetrics"] = noInput
        if "partitionCol" not in request.keys():
            print "[MY DEBUG STATEMENTS] enter partitionCol noInput"
            request["partitionCol"] = noInput
        if "bookmarkformat" not in request.keys():
            print "[MY DEBUG STATEMENTS] enter bookmarkformat noInput"
            request["bookmarkformat"] = noInput
        requestStr = json.dumps(request)
        print "[DEBUG] request string after optional fillings == "+str(requestStr)

        cols = request["cols"]
        colnames = []
        coltypes = []
        for c in cols:
            try:
                colnames.append(c["colname"])
                coltypes.append(c["coltype"])
            except:
                print "[MY DEBUG STATEMENTS] col was == "+str(c)

        dbtype = db_port_map[str(port)]
        print "[DEBUG] dbtype used == "+dbtype
        print "[DEBUG] the db connection string == "+str(dbtype)+"://"+str(user)+":"+str(password)+"@"+str(host)+":"+str(port)+"/"+str(dbname)
        dbengine = create_engine(str(dbtype)+"://"+str(user)+":"+str(password)+"@"+str(host)+":"+str(port)+"/"+str(dbname), echo= False)
        dbconn = dbengine.connect()
        metadata = MetaData(dbengine)

        desc_table_query = "desc "+dbtable+";"
        desc_table_result = dbconn.execute(desc_table_query)
        desc_table_list = desc_table_result.fetchall()

        fullTableSchema = []

        qualified_db_table_name = alias+"_"+dbname+"_"+dbtable
        for t in desc_table_list:
            colname = t[0]
            coltype = t[1]

            fullTableSchema.append(colname)

            if colname in colnames:
                colin = colnames.index(colname)
                usertype = coltypes[colin]
            else:
                usertype = getUserSpecType(colname)
            try:
                internalcursor.execute("INSERT INTO VarTypeRecordsTable (tablename, colname, coltype, usertype) VALUES ("+','.join([ss(qualified_db_table_name),ss(colname),ss(coltype),ss(usertype)])+");")
                internaldb.commit()
            except:
                print "[DEBUG] some exception while inserting in VarTypeRecordsTable . . . for colname == "+str(colname)+" for coltype == "+str(coltype)
                internaldb.rollback()
                traceback.print_exc()
            print "[DEBUG] inserted the colname == "+colname+" with column type == "+coltype+ " and user type == "+usertype
        print "[DEBUG] column insertions are all over . . ."

        request["fullTableSchema"] = ','.join(fullTableSchema)

        requestStr = json.dumps(request)
        print "[DEBUG] request string after optional fillings and everything == "+str(requestStr)

        lastStarted = defaultDateTimeStr
        lastEnded = defaultDateTimeStr

        totalRuns = Zook
        successRuns = Zook
        failureRuns = Zook
        currentState = defaultRunState
        exceptions = NoExceptionsStr
        notes = NoNotesStr
        print "[MY DEBUG STATEMENTS] the query == "+str("INSERT into Requests (processDate,request,host,port,dbname,dbtable,lastStarted,lastEnded,runFrequency,totalRuns,successRuns,failureRuns,currentState,exceptions,notes) VALUES ("+','.join([ss(processDate),ss(requestStr),ss(host),ss(port),ss(dbname),ss(dbtable),ss(lastStarted),ss(lastEnded),ss(runFrequency),ss(totalRuns),ss(successRuns),ss(failureRuns),ss(currentState),ss(exceptions),ss(notes)])+");")
        try:
            internalcursor.execute("INSERT into Requests (processDate,request,host,port,dbname,dbtable,lastStarted,lastEnded,runFrequency,totalRuns,successRuns,failureRuns,currentState,exceptions,notes) VALUES ("+','.join([ss(processDate),ss(requestStr),ss(host),ss(port),ss(dbname),ss(dbtable),ss(lastStarted),ss(lastEnded),ss(runFrequency),ss(totalRuns),ss(successRuns),ss(failureRuns),ss(currentState),ss(exceptions),ss(notes)])+");")
            internaldb.commit()
        except:
            print "[DEBUG] some exception while inserting in Requests . . ."
            internaldb.rollback()
            traceback.print_exc()
        internaldb.close()
        print "[MY DEBUG STATEMENTS] inserted request . . ."

class PostRequestHandlerKAFKA(tornado.web.RequestHandler):
    def post(self):
        request = tornado.escape.json_decode(self.request.body)
        requestStr = json.dumps(request)
        print "[MY DEBUG STATEMENTS] received kafka request == "+json.dumps(request)
        internaldb = MySQLdb.connect(internalhost,internalusername,internalpassword,internaldbname)
        internalcursor = internaldb.cursor()
        print "[DEBUG] internal connection is up. . ."
        processDate = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print "[DEBUG] processDate == "+str(processDate)
        requestStr = json.dumps(request)
        print "[DEBUG] request string == "+str(requestStr)
        # compulsary fields
        cluster = request["cluster"]
        print "[DEBUG] cluster == "+cluster
        topic = request["topic"]
        print "[DEBUG] topic == "+topic
        alias = request["alias"]
        print "[DEBUG] alias == "+alias
        groupName = request["groupName"]
        print "[DEBUG] groupName == "+groupName
        reset_offset_on_start = request["reset_offset_on_start"]
        print "[DEBUG] reset_offset_on_start == "+reset_offset_on_start
        auto_offset_reset = request["auto_offset_reset"]
        print "[DEBUG] auto_offset_reset == "+auto_offset_reset
        bookmark = request["bookmark"]
        print "[DEBUG] bookmark == "+bookmark
        bookmarkformat = request["bookmarkformat"]
        print "[DEBUG] bookmarkformat == "+bookmarkformat
        primaryKey = request["primaryKey"]
        print "[DEBUG] primaryKey == "+primaryKey
        # redundant fields
        if "druidDims" not in request.keys():
            print "[MY DEBUG STATEMENTS] enter druidDims noInput"
            request["druidDims"] = noInput
        if "druidMetrics" not in request.keys():
            print "[MY DEBUG STATEMENTS] enter druidMetrics noInput"
            request["druidMetrics"] = noInput
        if "partitionCol" not in request.keys():
            print "[MY DEBUG STATEMENTS] enter partitionCol noInput"
            request["partitionCol"] = noInput
        if "bookmarkformat" not in request.keys():
            print "[MY DEBUG STATEMENTS] enter bookmarkformat noInput"
            request["bookmarkformat"] = noInput
        requestStr = json.dumps(request)
        print "[DEBUG] request string after optional fillings == "+str(requestStr)

        kafkaCols = request["kafkaCols"]
        kafkaColNames = []
        kafkaColTypes = []
        for kc in kafkaCols:
            kafkaColNames.append(kc["kafkaColName"])
            kafkaColTypes.append(kc["kafkaColType"])

        qualified_kafka_topic_name = cluster+"_"+topic+"_"+alias
        for c in range(0,len(kafkaColNames)):
            try:
                kafkaColname = kafkaColNames[c]
                kafkaUsertype = kafkaColTypes[c]
                internalcursor.execute("INSERT INTO kafkaVarTypeRecordsTable (topicname, kafkaColname, kafkaUsertype) VALUES ("+','.join([ss(qualified_kafka_topic_name),ss(kafkaColname),ss(kafkaUsertype)])+");")
                internaldb.commit()
            except:
                print "[DEBUG] some exception while inserting in kafkaVarTypeRecordsTable . . ."
                internaldb.rollback()
                traceback.print_exc()
            print "[DEBUG] inserted the kafkacolname == "+kafkaColname+" with user type == "+kafkaUsertype
        print "[DEBUG] kafka column insertions are all over . . ."

        requestStr = json.dumps(request)
        print "[DEBUG] request string after optional fillings and everything == "+str(requestStr)

        currentState = defaultRunState
        exceptions = NoExceptionsStr
        notes = NoNotesStr

        print "[MY DEBUG STATEMENTS] the query == "+str("INSERT into kafkaRequests (processDate,request,cluster,topic,alias,groupName,currentState,exceptions,notes) VALUES ("+','.join([ss(processDate),ss(requestStr),ss(cluster),ss(topic),ss(alias),ss(groupName),ss(currentState),ss(exceptions),ss(notes)])+");")
        try:
            internalcursor.execute("INSERT into kafkaRequests (processDate,request,cluster,topic,alias,groupName,currentState,exceptions,notes) VALUES ("+','.join([ss(processDate),ss(requestStr),ss(cluster),ss(topic),ss(alias),ss(groupName),ss(currentState),ss(exceptions),ss(notes)])+");")
            internaldb.commit()
        except:
            print "[DEBUG] some exception while inserting in Requests . . ."
            internaldb.rollback()
            traceback.print_exc()
        internaldb.close()
        print "[MY DEBUG STATEMENTS] inserted request . . ."

class PostReportHandler(tornado.web.RequestHandler):
    def post(self):
        report = tornado.escape.json_decode(self.request.body)
        print "[MY DEBUG STATEMENTS] received report == "+str(report)

class LoginHandler(BaseHandler):
    def get(self):
        """
        self.write('<html><body><form action="/login" method="post">'
                   'Username: <input type="text" name="username">'
                   '<br>'
                   'Password: <input type="password" name="password">'
                   '<br><br>'
                   '<input type="submit" value="Sign in">'
                   '</form></body></html>')
        """
        welcome_message = "Please input your login credentials :"
        self.render("login.html",message = welcome_message)
    @gen.coroutine
    def post(self):
        #request = tornado.escape.json_decode(self.request.body)
        getusername = self.get_argument('username')
        getpassword = self.get_argument('password')
        userquery = "select username,password from users;"
        internaldb = MySQLdb.connect(internalhost,internalusername,internalpassword,internaldbname)
        cursor = internaldb.cursor()
        cursor.execute(userquery)
        rt = cursor.fetchall()
        list_of_users = []
        list_of_users_username = []
        list_of_users_password = []
        for r in rt:
            user = {}
            user["username"] = username = str(r[0])
            user["password"] = password = str(r[1])
            list_of_users.append(user)
            list_of_users_username.append(username)
            list_of_users_password.append(password)

        if list_of_users_username.index(getusername) == list_of_users_password.index(getpassword) and list_of_users_password.index(getpassword) != -1 :
            self.set_secure_cookie("username", username)
            self.redirect("/Ramanujan/?username="+username)
            return
        else:
            retry_message = "the previous login credentials failed. Retry ?"
            self.render("login.html",message = retry_message)

class LogoutHandler(BaseHandler):
    @tornado.web.authenticated
    def get(self):
        self.clear_cookie("username")
        print "[DEBUG] request to log out . . ."
        welcome_message = "Please input your login credentials :"
        self.render("login.html",message=welcome_message)

        
class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/login", LoginHandler),
            (r"/logout", LogoutHandler),
            (r"/api/request/rdbms",PostRequestHandlerRDBMS),
            (r"/api/request/kafka",PostRequestHandlerKAFKA),
            (r"/api/report", PostReportHandler),
            (r"/Ramanujan/", DefaultHandler),
            (r"/web/static/(.*)", tornado.web.StaticFileHandler, {"path":"web/static/"})
        ]
        tornado.web.Application.__init__(self,handlers,static_path="web/static",template_path="web/static",login_url="/login",cookie_secret="R7ALZk1rT4OOLE7TdDvuIkPZXq+YgEOznOrXb7RI5Ns=")    

def main():
    application = Application()
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(port)

    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    main()
