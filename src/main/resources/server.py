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


import os.path

define("port",default=9999,help="serve on the given port",type=int)
define("debug",default=False, help="running in the debug mode")

class DefaultHandler(tornado.web.RequestHandler):
	def get(self):
		self.render("index.html")
		print "[DEBUG] server up and running . . ."


class PostRequestHandler(tornado.web.RequestHandler):
	def post(self):
		request = tornado.escape.json_decode(self.request.body)
		print "[DEBUG] received request == "+str(request)

class PostReportHandler(tornado.web.RequestHandler):
	def post(self):
		report = tornado.escape.json_decode(self.request.body)
		print "[DEBUG] received report == "+str(report)

class Application(tornado.web.Application):
	def __init__(self):
		handlers = [
			(r"/api/requests",PostRequestHandler),
			(r"api/report", PostReportHandler),
			(r"/Ramanujan/(.*)", tornado.web.StaticFileHandler, {"path":"web/static/"})
		]
		tornado.web.Application.__init__(self,handlers)

def main():
	application = Application()
	http_server = tornado.httpserver.HTTPServer(application)
	http_server.listen(9999)

	tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
	main()
