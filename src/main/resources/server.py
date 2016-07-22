#!/usr/env/python
"""
server instead of the normal spray routes
"""

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

defaultDateTimeStr = "1000-01-01 00:00:00"
defaultRunState = "idle"
NoExceptionsStr = "none"
NoNotesStr = "none"

def ss(str_):
	return '"'+str_+'"'

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
			print "[DEBUG] no record entry in the Requests . . ."
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
		print "[DEBUG] server up and running . . ."


class PostRequestHandler(tornado.web.RequestHandler):
	# the random error is a trouble, and 
	def post(self):
		request = tornado.escape.json_decode(self.request.body)
		print "[DEBUG] received request == "+str(request)
		db = MySQLdb.connect(internalhost,internalusername,internalpassword,internaldbname)
		cursor = db.cursor()
		processDate = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
		requestStr = str(request)
		host = request["host"]
		port = request["port"]
		dbname = request["db"]
		dbtable = request["table"]
		lastStarted = defaultDateTimeStr
		lastEnded = defaultDateTimeStr
		runFrequency = request["runFrequency"]
		totalRuns = "0"
		successRuns = "0"
		failureRuns = "0"
		currentState = defaultRunState
		exceptions = NoExceptionsStr
		notes = NoNotesStr
		print "[DEBUG] the query == "+str("INSERT into Requests (processDate,request,host,port,dbname,dbtable,lastStarted,lastEnded,runFrequency,totalRuns,successRuns,failureRuns,currentState,exceptions,notes) VALUES ("+','.join([ss(processDate),ss(requestStr),ss(host),ss(port),ss(dbname),ss(dbtable),ss(lastStarted),ss(lastEnded),ss(runFrequency),ss(totalRuns),ss(successRuns),ss(failureRuns),ss(currentState),ss(exceptions),ss(notes)])+");")
		try:
			cursor.execute("INSERT into Requests (processDate,request,host,port,dbname,dbtable,lastStarted,lastEnded,runFrequency,totalRuns,successRuns,failureRuns,currentState,exceptions,notes) VALUES ("+','.join([ss(processDate),ss(requestStr),ss(host),ss(port),ss(dbname),ss(dbtable),ss(lastStarted),ss(lastEnded),ss(runFrequency),ss(totalRuns),ss(successRuns),ss(failureRuns),ss(currentState),ss(exceptions),ss(notes)])+");")
			db.commit()
		except:
			db.rollback()
		db.close()
		print "[DEBUG] inserted request . . ."




class PostReportHandler(tornado.web.RequestHandler):
	def post(self):
		report = tornado.escape.json_decode(self.request.body)
		print "[DEBUG] received report == "+str(report)
		
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
