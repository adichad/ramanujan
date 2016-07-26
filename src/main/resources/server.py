#!/usr/env/python
"""
server instead of the normal spray routes
"""

import traceback
import math
import csv
import sys
import json
import ast
import time
import django
import sqlalchemy
from sqlalchemy import *
from sqlalchemy import func
import collections
from django.utils.encoding import smart_str, smart_unicode
import logging
import smtplib
import sendgrid
from datetime import *
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pickle
import logging
import csv
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

import MySQLdb
import os.path

import pandas as pd

from datetime import datetime	

define("port",default=9999,help="serve on the given port",type=int)
define("debug",default=False, help="running in the debug mode")

internalhost = "localhost"
internaldbname = "Ramanujan"
internalusername = "root"
internalpassword = "1"

defaultDateTimeStr = "0001-01-01 00:00:00"
defaultRunState = "idle"
NoExceptionsStr = "none"
NoNotesStr = "none"
noInput = "none"
Zook = "0"

db_port_map = {}
db_port_map['3306'] = "mysql"
db_port_map['5432'] = "postgres"
db_port_map['1433'] = "mssql"

def ss(str_):
	return "'"+str_+"'"

def getUserSpecType(colname,request):
	# SELECT HIVE SPECIFIC TYPES ONLY e.g string integer etc etc # default STRING no mumbo jumbo
	return "STRING"

class DefaultHandler(tornado.web.RequestHandler):
	def get(self):
		db = MySQLdb.connect(internalhost,internalusername,internalpassword,internaldbname)
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
		self.render("index.html",message=health_message,records=df_to_show)
		print "[MY DEBUG STATEMENTS] server up and running . . ."


class PostRequestHandler(tornado.web.RequestHandler):
	# the random error is a trouble, and 
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

			usertype = getUserSpecType(colname,request)
			try:
				internalcursor.execute("INSERT INTO VarTypeRecordsTable (tablename, colname, coltype, usertype) VALUES ("+','.join([ss(qualified_db_table_name),ss(colname),ss(coltype),ss(usertype)])+");")
				internaldb.commit()
			except:
				print "[DEBUG] some exception while inserting in VarTypeRecordsTable . . ."
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
		


class PostReportHandler(tornado.web.RequestHandler):
	def post(self):
		report = tornado.escape.json_decode(self.request.body)
		print "[MY DEBUG STATEMENTS] received report == "+str(report)
		
class Application(tornado.web.Application):
	def __init__(self):
		handlers = [
			(r"/api/request",PostRequestHandler),
			(r"/api/report", PostReportHandler),
			(r"/Ramanujan/", DefaultHandler),
			(r"/web/static/(.*)", tornado.web.StaticFileHandler, {"path":"web/static/"})
		]
		tornado.web.Application.__init__(self,handlers,static_path="web/static",template_path="web/static")	

def main():
	application = Application()
	http_server = tornado.httpserver.HTTPServer(application)
	http_server.listen(9999)

	tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
	main()
