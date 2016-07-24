package com.askme.ramanujan.util

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.askme.ramanujan.Configurable
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization


class DataSource(val config: Config,val conntype: String, val host: String, val port: String,           
		val user: String, val password: String, val db: String, val table: String,          
		val bookmark: String, val bookmarkformat: String, val primarykey: String, val fullTableSchema: String,
		val hdfsPartitionCol: String,val druidMetrics: String,val druidDims: String, val schedulingFrequency: String) extends Configurable with Logging with Serializable{

	def appendType(s: String): String = {
		s+" STRING "
	}

	def getColAndType(): String = {
		val columns = fullTableSchema.split(",")
		val typecastedColumns = columns.map(appendType(_)).mkString(" , ")
		typecastedColumns
	}


	object Holder extends Serializable {
       @transient lazy val log = Logger.getLogger(getClass.getName)    
      }
  
      val oConfig = config // maybe this relays config to them all
	    var oconntype: String = conntype // e.g com.mysql.driver.MySQL
			var ohost: String = host // host ip
			var oport: String = port // port no.
			var ouser: String = user // user
			var opassword: String = password // password 
			var odb: String = db //  dbname
			var otable: String = table // tablename
			var obookmark: String = bookmark // bookmark column - e.g timestamp etc etc.
			var obookmarkformat: String = bookmarkformat // timestamp or date or incr. id identifier format - YYYY-mm-ddTHH:MM:SSZ
			var oprimarykey: String = primarykey // primarykey - e.g orderId / payment status / transaction id etc etc
			var ofullTableSchema: String = fullTableSchema // full schema of the table - we pull entire row for Kafka HDFS

			var internalConnection:Connection = null
			val internalHost = string("db.internal.url") // get the host from env confs - tables bookmark & status mostly
			val internalPort = string("db.internal.port") // get the port from env confs - tables bookmark & status mostly
			val internalDB = string("db.internal.dbname") // get the port from env confs - tables bookmark & status mostly
			val internalUser = string("db.internal.user") // get the user from env confs - tables bookmark & status mostly
			val internalPassword = string("db.internal.password") // get the password from env confs - tables bookmark & status mostly

			val hostURL: String = (string("db.conn.jdbc")+":"+string("db.type.mysql")+"://"+host+":"+port+"/"+db) // pertains to this very db+table : fully qualified name
			val internalURL: String = (string("db.conn.jdbc")+":"+string("db.type.mysql")+"://"+internalHost+":"+internalPort+"/"+internalDB) // the internal connection DB <status, bookmark, requests> etc
			// returns the //info things
			override def toString(): String = ("[//info] this data source is == "+hostURL) // to Strings : db specific
			def insertInitRow(json: String) = {
		    internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
		    val statement = internalConnection.createStatement()
		    val timeNow = Calendar.getInstance.getTime()
		    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
		    val timeNowStr = format.format(timeNow)
				debug("INSERT INTO "+string("db.internal.tables.requests.name")+" ("+string("db.internal.tables.requests.cols.processDate")+","+string("db.internal.tables.requests.cols.host")+","+string("db.internal.tables.requests.cols.dbname")+","+string("db.internal.tables.requests.cols.dbtable")+","+string("db.internal.tables.requests.cols.request")+","+string("db.internal.tables.requests.cols.status")+")" + "VALUES( '"+timeNowStr+"','"+host+"','"+db+"','"+table+"','"+json+"','"+string("db.internal.tables.requests.defs.defaultStatusVal")+"')")
		    val insertRequestQuery = "INSERT INTO "+string("db.internal.tables.requests.name")+" ("+string("db.internal.tables.requests.cols.processDate")+","+string("db.internal.tables.requests.cols.host")+","+string("db.internal.tables.requests.cols.dbname")+","+string("db.internal.tables.requests.cols.dbtable")+","+string("db.internal.tables.requests.cols.request")+","+string("db.internal.tables.requests.cols.status")+")" + "VALUES( '"+timeNowStr+"','"+host+"','"+db+"','"+table+"','"+json+"','"+string("db.internal.tables.requests.defs.defaultStatusVal")+"')" // insert '0' as status by default.
				//Holder.log.debug("[MY DEBUG STATEMENTS] the insertQuery == "+insertRequestQuery)
		    statement.executeUpdate(insertRequestQuery)
		    internalConnection.close()
		  }
			// Update insertInRunLogsPassed
			def insertInRunLogsPassed(hash: String) = {
				val format = new java.text.SimpleDateFormat(string("db.internal.tables.requests.defs.defaultDateFormat"))
				val currentDateDate = Calendar.getInstance().getTime()
				val currentDateStr = format.format(currentDateDate)
				internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
				val statement = internalConnection.createStatement()
				debug("INSERT INTO "+string("db.internal.tables.runninglogs.name")+" ("+string("db.internal.tables.runninglogs.cols.host")+","+string("db.internal.tables.runninglogs.cols.port")+","+string("db.internal.tables.runninglogs.cols.dbname")+","+string("db.internal.tables.runninglogs.cols.dbtable")+","+string("db.internal.tables.runninglogs.cols.runTimeStamp")+","+string("db.internal.tables.runninglogs.cols.hash")+","+string("db.internal.tables.runninglogs.cols.exceptions")+","+string("db.internal.tables.runninglogs.cols.notes")+") VALUES ('"+host+"','"+port+"','"+db+"','"+table+"','"+currentDateStr+"','"+hash+"','none','the last run passed')")
				val insertPassLogQuery = "INSERT INTO "+string("db.internal.tables.runninglogs.name")+" ("+string("db.internal.tables.runninglogs.cols.host")+","+string("db.internal.tables.runninglogs.cols.port")+","+string("db.internal.tables.runninglogs.cols.dbname")+","+string("db.internal.tables.runninglogs.cols.dbtable")+","+string("db.internal.tables.runninglogs.cols.runTimeStamp")+","+string("db.internal.tables.runninglogs.cols.hash")+","+string("db.internal.tables.runninglogs.cols.exceptions")+","+string("db.internal.tables.runninglogs.cols.notes")+") VALUES ('"+host+"','"+port+"','"+db+"','"+table+"','"+currentDateStr+"','"+hash+"','none','the last run passed')"
				statement.executeUpdate(insertPassLogQuery)
				internalConnection.close()
			}
			// Update insertInRequestsPassed
			def insertInRequestsPassed(hash: String) = {
				val format = new java.text.SimpleDateFormat(string("db.internal.tables.requests.defs.defaultDateFormat"))
				val currentDateDate = Calendar.getInstance().getTime()
				val currentDateStr = format.format(currentDateDate)
				internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
				val statement = internalConnection.createStatement()
				debug("UPDATE "+string("db.internal.tables.requests.name")+" SET "+string("db.internal.tables.requests.cols.success")+" = "+string("db.internal.tables.requests.cols.success")+" + 1 , "+string("db.internal.tables.requests.cols.currentState")+" = \""+string("db.internal.tables.requests.defs.defaultIdleState")+"\", "+string("db.internal.tables.requests.cols.lastEnded")+" = \""+currentDateStr+"\" where "+string("db.internal.tables.requests.cols.host")+" = \""+host+"\" and "+string("db.internal.tables.requests.cols.dbname")+" = \""+db+"\" and "+string("db.internal.tables.requests.cols.dbtable")+" = \""+table+"\"")
				val insertReqPassedQuery = "UPDATE "+string("db.internal.tables.requests.name")+" SET "+string("db.internal.tables.requests.cols.success")+" = "+string("db.internal.tables.requests.cols.success")+" + 1 , "+string("db.internal.tables.requests.cols.currentState")+" = \""+string("db.internal.tables.requests.defs.defaultIdleState")+"\", "+string("db.internal.tables.requests.cols.lastEnded")+" = \""+currentDateStr+"\" where "+string("db.internal.tables.requests.cols.host")+" = \""+host+"\" and "+string("db.internal.tables.requests.cols.dbname")+" = \""+db+"\" and "+string("db.internal.tables.requests.cols.dbtable")+" = \""+table+"\""
				statement.executeUpdate(insertReqPassedQuery)
				internalConnection.close()
			}
			// Update insertInRunLogsFailed
			def insertInRequestsFailed(hash: String, value: Exception) = {
				val strValue = value.toString()
				val format = new java.text.SimpleDateFormat(string("db.internal.tables.requests.defs.defaultDateFormat"))
				val currentDateDate = Calendar.getInstance().getTime()
				val currentDateStr = format.format(currentDateDate)
				internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
				val statement = internalConnection.createStatement()
				debug("UPDATE "+string("db.internal.tables.requests.name")+" SET "+string("db.internal.tables.requests.cols.exceptions")+" = "+strValue+" , "+string("db.internal.tables.requests.cols.failure")+" = "+string("db.internal.tables.requests.cols.failure")+" + 1 , "+string("db.internal.tables.requests.cols.currentState")+" = "+string("db.internal.tables.requests.defs.defaultIdleState")+" where "+string("db.internal.tables.requests.cols.host")+" = "+host+" and "+string("db.internal.tables.requests.cols.dbname")+" = "+db+" and "+string("db.internal.tables.requests.cols.dbtable")+" = "+table)
				val insertReqFailedQuery = "UPDATE "+string("db.internal.tables.requests.name")+" SET "+string("db.internal.tables.requests.cols.exceptions")+" = "+strValue+" , "+string("db.internal.tables.requests.cols.failure")+" = "+string("db.internal.tables.requests.cols.failure")+" + 1 , "+string("db.internal.tables.requests.cols.currentState")+" = "+string("db.internal.tables.requests.defs.defaultIdleState")+" where "+string("db.internal.tables.requests.cols.host")+" = "+host+" and "+string("db.internal.tables.requests.cols.dbname")+" = "+db+" and "+string("db.internal.tables.requests.cols.dbtable")+" = "+table
				statement.executeUpdate(insertReqFailedQuery)
				internalConnection.close()
			}

			def insertInRunLogsFailed(hash: String, value: Exception) = {
				val strValue = value.toString()
				val format = new java.text.SimpleDateFormat(string("db.internal.tables.requests.defs.defaultDateFormat"))
				val currentDateDate = Calendar.getInstance().getTime()
				val currentDateStr = format.format(currentDateDate)
				internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
				val statement = internalConnection.createStatement()
				debug("INSERT INTO "+string("db.internal.tables.runninglogs.name")+" ("+string("db.internal.tables.runninglogs.cols.host")+","+string("db.internal.tables.runninglogs.cols.port")+","+string("db.internal.tables.runninglogs.cols.dbname")+","+string("db.internal.tables.runninglogs.cols.dbtable")+","+string("db.internal.tables.runninglogs.cols.runTimeStamp")+","+string("db.internal.tables.runninglogs.cols.hash")+","+string("db.internal.tables.runninglogs.cols.exceptions")+","+string("db.internal.tables.runninglogs.cols.notes")+") VALUES ('"+host+"','"+port+"','"+db+"','"+table+"','"+currentDateStr+"','"+hash+"','"+strValue+"','the run has failed . . .')")
				val insertFailLogQuery = "INSERT INTO "+string("db.internal.tables.runninglogs.name")+" ("+string("db.internal.tables.runninglogs.cols.host")+","+string("db.internal.tables.runninglogs.cols.port")+","+string("db.internal.tables.runninglogs.cols.dbname")+","+string("db.internal.tables.runninglogs.cols.dbtable")+","+string("db.internal.tables.runninglogs.cols.runTimeStamp")+","+string("db.internal.tables.runninglogs.cols.hash")+","+string("db.internal.tables.runninglogs.cols.exceptions")+","+string("db.internal.tables.runninglogs.cols.notes")+") VALUES ('"+host+"','"+port+"','"+db+"','"+table+"','"+currentDateStr+"','"+hash+"','"+strValue+"','the run has failed . . .')"
				statement.executeUpdate(insertFailLogQuery)
				internalConnection.close()
			}
			def insertInRunLogsStarted(hash: String) = {
				val format = new java.text.SimpleDateFormat(string("db.internal.tables.requests.defs.defaultDateFormat"))
				val currentDateDate = Calendar.getInstance().getTime()
				val currentDateStr = format.format(currentDateDate)
				internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
				val statement = internalConnection.createStatement()
				debug("INSERT INTO "+string("db.internal.tables.runninglogs.name")+" ("+string("db.internal.tables.runninglogs.cols.host")+","+string("db.internal.tables.runninglogs.cols.port")+","+string("db.internal.tables.runninglogs.cols.dbname")+","+string("db.internal.tables.runninglogs.cols.dbtable")+","+string("db.internal.tables.runninglogs.cols.runTimeStamp")+","+string("db.internal.tables.runninglogs.cols.hash")+","+string("db.internal.tables.runninglogs.cols.exceptions")+","+string("db.internal.tables.runninglogs.cols.notes")+") VALUES ('"+host+"','"+port+"','"+db+"','"+table+"','"+currentDateStr+"','"+hash+"','none','started the run')")
				val insertStartLogQuery = "INSERT INTO "+string("db.internal.tables.runninglogs.name")+" ("+string("db.internal.tables.runninglogs.cols.host")+","+string("db.internal.tables.runninglogs.cols.port")+","+string("db.internal.tables.runninglogs.cols.dbname")+","+string("db.internal.tables.runninglogs.cols.dbtable")+","+string("db.internal.tables.runninglogs.cols.runTimeStamp")+","+string("db.internal.tables.runninglogs.cols.hash")+","+string("db.internal.tables.runninglogs.cols.exceptions")+","+string("db.internal.tables.runninglogs.cols.notes")+") VALUES ('"+host+"','"+port+"','"+db+"','"+table+"','"+currentDateStr+"','"+hash+"','none','started the run')"
				statement.executeUpdate(insertStartLogQuery)
				internalConnection.close()
			}
			// Update the current bookmark
			def updateBookMark(currBookMark: String) = {
			  internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
				val statement = internalConnection.createStatement()
				debug("INSERT INTO `"+string("db.internal.tables.bookmarks.name")+"`(`"+string("db.internal.tables.bookmarks.cols.dbtablekey")+"`,"+string("db.internal.tables.bookmarks.cols.bookmarkId")+") VALUES( '"+db+"_"+table+"', '"+currBookMark+"')")
				val insertBkMrkQuery = "INSERT INTO `"+string("db.internal.tables.bookmarks.name")+"`(`"+string("db.internal.tables.bookmarks.cols.dbtablekey")+"`,"+string("db.internal.tables.bookmarks.cols.bookmarkId")+") VALUES( '"+db+"_"+table+"', '"+currBookMark+"')"
						statement.executeUpdate(insertBkMrkQuery)
			  internalConnection.close()
			}
			// Spark # 1 job stands over.
			def upsert(primarykey: String, bookmark: String) = {
			  internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
				val statement = internalConnection.createStatement()
						val upsertQuery = "INSERT INTO `"+string("db.internal.tables.status.name")+"`("+string("db.internal.tables.status.cols.dbname")+","+string("db.internal.tables.status.cols.table")+",`"+string("db.internal.tables.status.cols.primarykey")+"`,"+string("db.internal.tables.status.cols.sourceId")+","+string("db.internal.tables.status.cols.kafkaTargetId")+string("db.internal.tables.status.cols.hdfsTargetId")+") VALUES( '"+db+"','"+table+"','"+primarykey+"', '"+bookmark+"',"+string("db.internal.tables.status.defs.defaultTargetId")+string("db.internal.tables.status.defs.defaultTargetId")+") ON DUPLICATE KEY UPDATE `"+string("db.internal.tables.status.cols.sourceId")+"` = '"+bookmark+"'"        
						val resultSet = statement.executeQuery(upsertQuery)
				internalConnection.close()
			}
			// get the previous bookmark, from bookmark table
			def getPrevBookMark() = { // this strictly returns the actual bookmark, e.g the timestamp
			      debug("[MY DEBUG STATEMENTS] [SQL] [BookMarks] entered to obtain the previous bookmark . . . == "+toString())
			      internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
				    debug("[MY DEBUG STATEMENTS] [SQL] [BookMarks] got the internal connection . . . == "+toString())
			      val statement = internalConnection.createStatement()
			      debug("[MY DEBUG STATEMENTS] [SQL] [BookMarks] executing the statement . . . == "+toString())
						val prevOffsetFetchQuery = "select "+string("db.internal.tables.bookmarks.cols.dbtablekey")+" as "+string("db.internal.tables.bookmarks.cols.dbtablekey")+",max("+string("db.internal.tables.bookmarks.cols.id")+") as "+string("db.internal.tables.bookmarks.cols.id")+" from "+string("db.internal.tables.bookmarks.name")+" where "+string("db.internal.tables.bookmarks.cols.dbtablekey")+" in (\""+odb+"_"+otable+"\")"
						debug("[MY DEBUG STATEMENTS] [SQL] [BookMarks] prev offset fetch query . . . == "+prevOffsetFetchQuery)
						var resultSet = statement.executeQuery(prevOffsetFetchQuery) // handle first time cases also
						if(resultSet.next()){
						      debug("[MY DEBUG STATEMENTS] [SQL] [BookMarks] [previous offset] this db_table  has a row . . . == "+toString())
							    val key: String = resultSet.getString(string("db.internal.tables.bookmarks.cols.dbtablekey"))
									val id: String = resultSet.getString(string("db.internal.tables.bookmarks.cols.id"))
									val prevBookMarkFetchQuery = "select "+string("db.internal.tables.bookmarks.cols.bookmarkId")+" from "+string("db.internal.tables.bookmarks.name")+" where "+string("db.internal.tables.bookmarks.cols.dbtablekey")+" in (\""+ key +"\") and "+string("db.internal.tables.bookmarks.cols.id")+" in ("+ id +")"
									debug("[MY DEBUG STATEMENTS] [SQL] [BookMarks] prev bookmark fetch query . . . == "+prevOffsetFetchQuery)
							    val statement1 = internalConnection.createStatement()
									resultSet = statement1.executeQuery(prevBookMarkFetchQuery)
									if(resultSet.next()){
									 debug("[MY DEBUG STATEMENTS] [SQL] [BookMarks] [previous bookmark] this db_table  has a row . . . == "+toString())
										val bookmark = resultSet.getString(string("db.internal.tables.bookmarks.cols.bookmarkId"))
										    debug("[MY DEBUG STATEMENTS] [SQL] [BookMarks] [previous bookmark] the previous bookmark returned == "+bookmark)
												bookmark
									}
									else {
									      debug("[MY DEBUG STATEMENTS] [SQL] [BookMarks] [previous] [NULL] the default one . . . == "+string("db.internal.tables.bookmarks.defs.defaultBookMarkValue"))
									      string("db.internal.tables.bookmarks.defs.defaultBookMarkValue")
								  }
						}
						else {
						  debug("[MY DEBUG STATEMENTS] [SQL] [BookMarks] [previous] [NOTNULL] the default one . . . == "+string("db.internal.tables.bookmarks.defs.defaultBookMarkValue"))
						  string("db.internal.tables.bookmarks.defs.defaultBookMarkValue")
						}
			}
			// get the current bookmark, from source table
			def getCurrBookMark() = {
			  
			  val conf = sparkConf("spark")
		    val sc = SparkContext.getOrCreate(conf) // ideally this is what one must do - getOrCreate
		    val sqlContext = new SQLContext(sc)
			  
			  debug("[MY DEBUG STATEMENTS] [SQL] [BookMarks] [current bookmark] getting it . . . ")
				if(conntype.toLowerCase().contains(string("db.type.mysql").toLowerCase())){// lowercase comparisons always
				  debug("[MY DEBUG STATEMENTS] [SQL] [BOOKMARKS] [current bookmark] entered mysql types db/table . . .")
				  debug("[MY DEBUG STATEMENTS] [SQL] [BOOKMARKS] [current bookmark] getting current bookmark / DF / . . .")
				  debug("[MY DEBUG STATEMENTS] [SQL] [SPARKDF query check] == (select max("+bookmark+") as "+bookmark+" from "+table+" ) tmp")
				  val conf = sparkConf("spark")
				  
					val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
							Map(
									"driver" -> conntype,
									"url" -> (string("db.conn.jdbc")+":"+string("db.type.mysql")+"://"+host+":"+port+"/"+db+"?user="+user+"&password="+password),
									"dbtable" -> ("(select max("+bookmark+") as "+bookmark+" from "+table+" ) tmp") // now this is table specific so bookmark column must be passed!
									)
							).load()
							debug("[MY DEBUG STATEMENTS] [SQL] [BOOKMARKS] [current bookmark] got the current bookmark . . .")
						  if(jdbcDF.rdd.isEmpty()){
						    debug("[MY DEBUG STATEMENTS] [SQL] [BOOKMARKS] [current bookmark] the db of affected keys was all empty . . .")
						    debug("[MY DEBUG STATEMENTS] [SQL] [BOOKMARKS] [current bookmark] returning the default value == "+string("db.internal.tables.bookmarks.defs.defaultBookMarkValue"))
						    string("db.internal.tables.bookmarks.defs.defaultBookMarkValue")
						  }
						  else{
						    val currBookMark = jdbcDF.first().get(jdbcDF.first().fieldIndex(bookmark)).toString()
						    debug("[MY DEBUG STATEMENTS] [SQL] [BOOKMARKS] [current bookmark] returning the current value == "+currBookMark)
						    currBookMark
						  }
				}
				else if(conntype.toLowerCase().contains(string("db.type.postgres").toLowerCase())){
					debug("[MY DEBUG STATEMENTS] [SQL] [BOOKMARKS] [current bookmark] entered postgres types db/table . . .")
				  debug("[MY DEBUG STATEMENTS] [SQL] [BOOKMARKS] [current bookmark] getting current bookmark / DF / . . .")
				  debug("[MY DEBUG STATEMENTS] [SQL] [SPARKDF query check] == (select max("+bookmark+") as "+bookmark+" from "+table+" ) tmp")
				  val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
							Map(
									"driver" -> conntype, // "org.postgresql.Driver"
									"url" -> (string("db.conn.jdbc")+":"+string("db.type.postgres")+"://"+host+":"+port+"/"+db+"?user="+user+"&password="+password),
									"dbtable" -> ("(select max("+bookmark+") as "+bookmark+" from "+table+" ) tmp") // now this is table specific so bookmark column must be passed!
									)
							).load()
							debug("[MY DEBUG STATEMENTS] [SQL] [BOOKMARKS] [current bookmark] got the current bookmark . . .")
							if(jdbcDF.rdd.isEmpty()){
							  debug("[MY DEBUG STATEMENTS] [SQL] [BOOKMARKS] [current bookmark] the db of affected keys was all empty . . .")
						    debug("[MY DEBUG STATEMENTS] [SQL] [BOOKMARKS] [current bookmark] returning the default value == "+string("db.internal.tables.bookmarks.defs.defaultBookMarkValue"))
						    string("db.internal.tables.bookmarks.defs.defaultBookMarkValue")
						  }
						  else{
						    val currBookMark = jdbcDF.first().getString(jdbcDF.first().fieldIndex(bookmark))
						    debug("[MY DEBUG STATEMENTS] [SQL] [BOOKMARKS] [current bookmark] returning the current value == "+currBookMark)
						    currBookMark
						  }
				}
				else if(conntype.toLowerCase().contains(string("db.type.sqlserver").toLowerCase())){
				  debug("[MY DEBUG STATEMENTS] [SQL] [BOOKMARKS] [current bookmark] entered sqlserver types db/table . . .")
				  debug("[MY DEBUG STATEMENTS] [SQL] [BOOKMARKS] [current bookmark] getting current bookmark / DF / . . .")
				  debug("[MY DEBUG STATEMENTS] [SQL] [SPARKDF query check] == (select max("+bookmark+") as "+bookmark+" from "+table+" ) tmp")
					val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
							Map(
									"driver" -> conntype, // "com.microsoft.sqlserver.jdbc.SQLServerDriver"
									"url" -> (string("db.conn.jdbc")+":"+string("db.type.sqlserver")+"://"+host+":"+port+";database="+db+";user="+user+";password="+password),
									"dbtable" -> ("(select max("+bookmark+") as "+bookmark+" from "+table+" ) tmp") // now this is table specific so bookmark column must be passed!
									)
							).load()
							debug("[MY DEBUG STATEMENTS] [SQL] [BOOKMARKS] [current bookmark] got the current bookmark . . .")
							if(jdbcDF.rdd.isEmpty()){
							  debug("[MY DEBUG STATEMENTS] [SQL] [BOOKMARKS] [current bookmark] the db of affected keys was all empty . . .")
						    debug("[MY DEBUG STATEMENTS] [SQL] [BOOKMARKS] [current bookmark] returning the default value == "+string("db.internal.tables.bookmarks.defs.defaultBookMarkValue"))
						    string("db.internal.tables.bookmarks.defs.defaultBookMarkValue")
						  }
						  else{
						    val currBookMark = jdbcDF.first().getString(jdbcDF.first().fieldIndex(bookmark))
						    debug("[MY DEBUG STATEMENTS] [SQL] [BOOKMARKS] [current bookmark] returning the current value == "+currBookMark)
						    currBookMark
						  }
				}
				else{
					// no supported DB type
				  debug("[MY DEBUG STATEMENTS] [SQL] [BOOKMARKS] [current bookmark] entered some unsupported DB Driver / type . . .")
					string("db.internal.tables.bookmarks.defs.defaultBookMarkValue")
				}
			}
			def getAffectedPKs(prevBookMark: String,currBookMark: String) = {
			  
			  val conf = sparkConf("spark")
			  val sc = SparkContext.getOrCreate(conf)
			  val sqlContext = new SQLContext(sc)
			  
			  debug("[MY DEBUG STATEMENTS] [SQL] [AFFECTED PKs] getting it . . . ")
				if(conntype.toLowerCase().contains(string("db.type.mysql").toLowerCase())){
				  debug("[MY DEBUG STATEMENTS] [SQL] [AFFECTED PKs] entered mysql types db/table . . .")
				  debug("[MY DEBUG STATEMENTS] [SQL] [AFFECTED PKs] getting affected PKs / DF / . . .")
				  debug("[MY DEBUG STATEMENTS] [SQL] [AFFECTED PKs] [QUERY] === (select "+fullTableSchema+" from "+table+" where "+bookmark+" >= \""+prevBookMark+"\" and "+bookmark+" <= \""+currBookMark+"\" group by "+primarykey+") overtmp")
				  val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
							Map(
									"driver" -> conntype,
									"url" -> (string("db.conn.jdbc")+":"+string("db.type.mysql")+"://"+host+":"+port+"/"+db+"?user="+user+"&password="+password),
									"dbtable" -> ("(select "+fullTableSchema+" from "+table+" where "+bookmark+" >= \""+prevBookMark+"\" and "+bookmark+" <= \""+currBookMark+"\" group by "+primarykey+") overtmp")
									//"dbtable" -> ("(select \""+db+"\" as "+string("db.internal.tables.status.cols.dbname")+", \""+table+"\" as "+string("db.internal.tables.status.cols.dbtable")+", "+primarykey+" as "+string("db.internal.tables.status.cols.primarykey")+", max("+bookmark+") as "+string("db.internal.tables.status.cols.sourceId")+",\""+string("db.internal.tables.status.defs.defaultTargetId")+"\" as "+string("db.internal.tables.status.cols.kafkaTargetId")+",\""+string("db.internal.tables.status.defs.defaultTargetId")+"\" as "+string("db.internal.tables.status.cols.hdfsTargetId")+", concat(\'"+db+"_"+table+"_\',"+primarykey+") as "+string("db.internal.tables.status.cols.qualifiedName")+" from "+table+" where "+bookmark+" >= \""+prevBookMark+"\" and "+bookmark+" <= \""+currBookMark+"\" group by "+primarykey+") overtmp") // table specific
									)
							).load()
							jdbcDF
				}
				else if(conntype.toLowerCase().contains(string("db.type.postgres").toLowerCase())){
				  debug("[MY DEBUG STATEMENTS] [SQL] [AFFECTED PKs] entered postgres types db/table . . .")
				  debug("[MY DEBUG STATEMENTS] [SQL] [AFFECTED PKs] getting affected PKs / DF / . . .")
				  debug("[MY DEBUG STATEMENTS] [SQL] [AFFECTED PKs] [QUERY] === (select "+fullTableSchema+" from "+table+" where "+bookmark+" >= \""+prevBookMark+"\" and "+bookmark+" <= \""+currBookMark+"\" group by "+primarykey+") overtmp")
					val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
							Map(
									"driver" -> conntype, // "org.postgresql.Driver"
									"url" -> (string("db.conn.jdbc")+":"+string("db.type.postgres")+"://"+host+":"+port+"/"+db+"?user="+user+"&password="+password),
									"dbtable" -> ("(select "+fullTableSchema+" from "+table+" where "+bookmark+" >= \""+prevBookMark+"\" and "+bookmark+" <= \""+currBookMark+"\" group by "+primarykey+") overtmp")
									//"dbtable" -> ("(select \""+db+"\" as "+string("db.internal.tables.status.cols.dbname")+", \""+table+"\" as "+string("db.internal.tables.status.cols.dbtable")+", "+primarykey+" as "+string("db.internal.tables.status.cols.primarykey")+", max("+bookmark+") as "+string("db.internal.tables.status.cols.sourceId")+",\""+string("db.internal.tables.status.defs.defaultTargetId")+"\" as "+string("db.internal.tables.status.cols.kafkaTargetId")+",\""+string("db.internal.tables.status.defs.defaultTargetId")+"\" as "+string("db.internal.tables.status.cols.hdfsTargetId")+", concat(\'"+db+"_"+table+"_\',"+primarykey+") as "+string("db.internal.tables.status.cols.qualifiedName")+" from "+table+" where "+bookmark+" >= \""+prevBookMark+"\" and "+bookmark+" <= \""+currBookMark+"\" group by "+primarykey+") overtmp") // table specific
									)
							).load()
							jdbcDF
				}
				else if(conntype.toLowerCase().contains(string("db.type.sqlserver").toLowerCase())){
				  debug("[MY DEBUG STATEMENTS] [SQL] [AFFECTED PKs] entered sqlserver types db/table . . .")
				  debug("[MY DEBUG STATEMENTS] [SQL] [AFFECTED PKs] getting affected PKs / DF / . . .")
					debug("[MY DEBUG STATEMENTS] [SQL] [AFFECTED PKs] [QUERY] === (select "+fullTableSchema+" from "+table+" where "+bookmark+" >= \""+prevBookMark+"\" and "+bookmark+" <= \""+currBookMark+"\" group by "+primarykey+") overtmp")
				  val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
							Map(
									"driver" -> conntype, // "com.microsoft.sqlserver.jdbc.SQLServerDriver"
									"url" -> (string("db.conn.jdbc")+":"+string("db.type.sqlserver")+"://"+host+":"+port+";database="+db+";user="+user+";password="+password),
									"dbtable" -> ("(select "+fullTableSchema+" from "+table+" where "+bookmark+" >= \""+prevBookMark+"\" and "+bookmark+" <= \""+currBookMark+"\" group by "+primarykey+") overtmp")
									//"dbtable" -> ("(select \""+db+"\" as "+string("db.internal.tables.status.cols.dbname")+", \""+table+"\" as "+string("db.internal.tables.status.cols.dbtable")+", "+primarykey+" as "+string("db.internal.tables.status.cols.primarykey")+", max("+bookmark+") as "+string("db.internal.tables.status.cols.sourceId")+",\""+string("db.internal.tables.status.defs.defaultTargetId")+"\" as "+string("db.internal.tables.status.cols.kafkaTargetId")+",\""+string("db.internal.tables.status.defs.defaultTargetId")+"\" as "+string("db.internal.tables.status.cols.hdfsTargetId")+", concat(\'"+db+"_"+table+"_\',"+primarykey+") as "+string("db.internal.tables.status.cols.qualifiedName")+" from "+table+" where "+bookmark+" >= \""+prevBookMark+"\" and "+bookmark+" <= \""+currBookMark+"\" group by "+primarykey+") overtmp") // table specific
									)
							).load()
							jdbcDF
				}
				else{
					// no supported DB type
				  debug("[MY DEBUG STATEMENTS] [SQL] [AFFECTED PKs] entered some unsupported db / table . . .")
				  debug("[MY DEBUG STATEMENTS] [SQL] [AFFECTED PKs] returning an empty / DF / . . .")
					sqlContext.emptyDataFrame // just return one empty data frame, no rows, no columns
				}
			}
			def getPKs4UpdationKafka(conf: SparkConf,sqlContext: SQLContext) = { // get the primary keys for insertion, as well as their timestamps/ bookmarks
				val table_to_query = table; val db_to_query = db
				Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [PKs] [KAFKA] for == "+db_to_query+"_"+table_to_query)
				Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] [QUERY]  (select "+string("db.internal.tables.status.cols.primarykey")+","+string("db.internal.tables.status.cols.sourceId")+" from "+string("db.internal.tables.status.name")+" where "+string("db.internal.tables.status.cols.dbname")+" in (\""+db_to_query+"\") and "+string("db.internal.tables.status.cols.dbtable")+" in (\""+table_to_query+"\") and "+string("db.internal.tables.status.cols.sourceId")+" > "+string("db.internal.tables.status.cols.kafkaTargetId")+") kafka_tmp")
				val sc = SparkContext.getOrCreate(conf)
						val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
								Map(
										"driver" -> string("db.internal.driver"),
										"url" -> (internalURL+"?user="+internalUser+"&password="+internalPassword),
										"dbtable" -> ("(select "+string("db.internal.tables.status.cols.primarykey")+","+string("db.internal.tables.status.cols.sourceId")+" from "+string("db.internal.tables.status.name")+" where "+string("db.internal.tables.status.cols.dbname")+" in (\""+db_to_query+"\") and "+string("db.internal.tables.status.cols.dbtable")+" in (\""+table_to_query+"\") and "+string("db.internal.tables.status.cols.sourceId")+" > "+string("db.internal.tables.status.cols.kafkaTargetId")+") kafka_tmp")
										)
								).load()
								if(jdbcDF.rdd.isEmpty()){
								  Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [PKs] [KAFKA] an empty DF of PKs for == "+db_to_query+"_"+table_to_query)
								  //Holder.log.info("no persistences done to the kafka sinks whatsoever . . . for == "+db_to_query+"_"+table_to_query)
								}
								else{
								  Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [PKs] [KAFKA] going to persist into == "+db_to_query+"_"+table_to_query)
  								jdbcDF.map { x => {
  								                    val conf = sparkConf("spark");
  								                    val sc=SparkContext.getOrCreate(conf);
  								                    val sqlContext = new SQLContext(sc);
  								                    fetchFromFactTableAndSinkKafka(x,sqlContext)
  								                  } 
  								           } // kafka sink only
								}
				
								// No piggyMerge in here.
			}
			def getPKs4UpdationHDFS(conf: SparkConf,sqlContext: SQLContext) = { // get the primary keys for insertion, as well as their timestamps/ bookmarks
				val table_to_query = table; val db_to_query = db
				Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [PKs] [HDFS] for == "+db_to_query+"_"+table_to_query)
				val sc = SparkContext.getOrCreate(conf)
						val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
								Map(
										"driver" -> string("db.internal.driver"),
										"url" -> (internalURL+"?user="+internalUser+"&password="+internalPassword),
										"dbtable" -> ("select "+string("db.internal.tables.status.cols.primarykey")+","+string("db.internal.tables.status.cols.sourceId")+" from "+string("db.internal.tables.status.name")+" where "+string("db.internal.tables.status.cols.dbname")+" = "+db_to_query+" and "+string("db.internal.tables.status.cols.dbtable")+" = "+table_to_query+" and "+string("db.internal.tables.status.cols.sourceId")+" > "+string("db.internal.tables.status.cols.hdfsTargetId"))
										)
								).load()
								if(jdbcDF.rdd.isEmpty()){
								  Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [PKs] [HDFS] an empty DF of PKs for == "+db_to_query+"_"+table_to_query)
								  //Holder.log.info("no persistences done to the sinks whatsoever . . . for == "+db_to_query+"_"+table_to_query)
								}
								else{
								  Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [PKs] [HDFS] going to persist into == "+db_to_query+"_"+table_to_query)
  								jdbcDF.map { x => fetchFromFactTableAndSinkHDFS(x,sqlContext) } // hdfs append
								}
								Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [PKs] [HDFS] [PiggyMerge] going to persist into == "+db_to_query+"_"+table_to_query)
								piggyMerge(sc,sqlContext,db_to_query,table_to_query) // take the two tables, and merge them
			}

			def fetchFromFactTableAndSinkKafka(x: Row,sqlContext: SQLContext) : String = { // had to explicitly do that
			  Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] entered the row-by-row sink . . .")
				val pk = x.getString(0) // get pk from the row
				val bkmk = x.getString(1) // get bookmark from row
				Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] doing it for == "+pk)
				
				val props = new Properties()
				props.put("metadata.broker.list", string("sinks.kafka.brokers"))
				props.put("serializer.class", string("sinks.kafka.serializer"))
			  props.put("group.id", string("sinks.kafka.groupname"))
			  props.put("producer.type", string("sinks.kafka.producer.type"))
			  
			  val config = new ProducerConfig(props)
			  val producer = new Producer[String,String](config)
				
				if(conntype.contains(string("db.type.mysql"))){ // selecting entire rows - insertion into kafka as json and hdfs as ORC
				  Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] db entered was == mysql")
					val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
							Map(
									"driver" -> conntype,
									"url" -> (string("db.conn.jdbc")+":"+string("db.type.mysql")+"://"+host+":"+port+"/"+db+"?user="+user+"&password="+password),
									"dbtable" -> ("select "+ofullTableSchema+" from "+table+" where "+primarykey+" = "+pk+" and "+bookmark+" = "+bkmk) // table Specific
									)
							).load()
					val row_to_insert: Row = jdbcDF.first()
					Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] got the row . . .")
					var rowMap:Map[String,String] = Map()
					val fullTableSchemaArr = ofullTableSchema.split(",")
					for(i <- 0 until fullTableSchemaArr.length) {
					  rowMap += (fullTableSchemaArr(i) -> row_to_insert.getString(i))  
					} // contructing the json
					implicit val formats = Serialization.formats(NoTypeHints)
					val json_to_insert = Serialization.write(rowMap) // to be relayed to kafka
					val cleanStr = preprocess(json_to_insert) // for all kinds of preprocessing - left empty
					Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] the clean string to insert into Kafka == "+cleanStr)
					producer.send(new KeyedMessage[String,String](("[TOPIC] "+db+"_"+table),db,cleanStr)) // topic + key + message
					Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] published the clean string . . .")
					// KAFKA Over
					updateTargetIdsBackKafka(db,table,pk,bkmk)
					"[STREAMING] [KAFKA] updations EXITTED successfully == "+db+"_and_"+table+"_and_"+pk+"_and_"+bkmk
				}
				else if(conntype.contains(string("db.type.postgres"))){
				  Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] db entered was == postgres")
					val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
							Map(
									"driver" -> conntype, // "org.postgresql.Driver"
									"url" -> (string("db.conn.jdbc")+":"+string("db.type.postgres")+"://"+host+":"+port+"/"+db+"?user="+user+"&password="+password),
									"dbtable" -> ("select "+ofullTableSchema+" from "+table+" where "+primarykey+" = "+pk+" and "+bookmark+" = "+bkmk) // table Specific
									)
							).load()
							val row_to_insert: Row = jdbcDF.first()
							Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] got the row . . .")
							var rowMap:Map[String,String] = Map()
							val fullTableSchemaArr = ofullTableSchema.split(",")
							for(i <- 0 until fullTableSchemaArr.length) {
					      rowMap += (fullTableSchemaArr(i) -> row_to_insert.getString(i))  // rather apply preprocessing here
					    } // constructing the json
					    implicit val formats = Serialization.formats(NoTypeHints)
					    val json_to_insert = Serialization.write(rowMap) // to be relayed to kafka
					    val cleanStr = preprocess(json_to_insert) // for all kinds of preprocessing - left empty
					    Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] the clean string to insert into Kafka == "+cleanStr)
					    producer.send(new KeyedMessage[String,String](("[TOPIC] "+db+"_"+table),db,cleanStr)) // topic + key + message
					    Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] published the clean string . . .")
					    // KAFKA Over
					    updateTargetIdsBackKafka(db,table,pk,bkmk)
					    "[STREAMING] [KAFKA] updations EXITTED successfully == "+db+"_and_"+table+"_and_"+pk+"_and_"+bkmk
				}
				else if(conntype.contains(string("db.type.sqlserver"))){
				  Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] db entered was == sqlserver")
					val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
							Map(
									"driver" -> conntype, // "com.microsoft.sqlserver.jdbc.SQLServerDriver"
									"url" -> (string("db.conn.jdbc")+":"+string("db.type.sqlserver")+host+":"+port+";database="+db+";user="+user+";password="+password),
									"dbtable" -> ("select "+fullTableSchema+" from "+table+" where "+primarykey+" = "+pk+" and "+bookmark+" = "+bkmk) // table Specific
									)
							).load()
							val row_to_insert: Row = jdbcDF.first()
							Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] got the row . . .")
							var rowMap:Map[String,String] = Map()
							val fullTableSchemaArr = ofullTableSchema.split(",")
							for(i <- 0 until fullTableSchemaArr.length) {
					      rowMap += (fullTableSchemaArr(i) -> row_to_insert.getString(i))  
					    }
							implicit val formats = Serialization.formats(NoTypeHints)
							val json_to_insert = Serialization.write(rowMap) // to be relayed to kafka
							val cleanStr = preprocess(json_to_insert) // for all kinds of preprocessing - left empty
							Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] the clean string to insert into Kafka == "+cleanStr)
							producer.send(new KeyedMessage[String,String](("[TOPIC] "+db+"_"+table),db,cleanStr)) // topic + key + message
							Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] published the clean string . . .")
							// KAFKA Over
							updateTargetIdsBackKafka(db,table,pk,bkmk)
							"[STREAMING] [KAFKA] updations EXITTED successfully == "+db+"_and_"+table+"_and_"+pk+"_and_"+bkmk
				}
				else{
				  Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] nothing to publish . . .")
				  // no need to send it to either of kafka or hdfs
					// no supported DB type - return a random string with fullTableSchema
				  val fullTableSchemaArr = ofullTableSchema.split(",")
				  var rowMap:Map[String,String] = Map()
				  for(i <- 0 until fullTableSchemaArr.length) {
					      rowMap += (fullTableSchemaArr(i) -> "nothing")  
					}
				  implicit val formats = Serialization.formats(NoTypeHints)
				  val json_to_insert = Serialization.write(rowMap) // to be relayed to kafka
				  val cleanStr = preprocess(json_to_insert) // for all kinds of preprocessing - left empty
				  // KAFKA Over
				  updateTargetIdsBackKafka(db,table,pk,bkmk)
				  "updations EXITTED successfully"
				}
			}
			
			def fetchFromFactTableAndSinkHDFS(x: Row,sqlContext: SQLContext) : String = { // had to explicitly do that
			  Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] entered hdfs persistence function . . .")
				val pk = x.getString(0) // get pk from the row
				val bkmk = x.getString(1) // get bookmark from row
				Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] doing it for == "+pk)
				if(conntype.contains(string("db.type.mysql"))){ // selecting entire rows - insertion into kafka as json and hdfs as ORC
				  Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] dbtype == mysql")
					val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
							Map(
									"driver" -> conntype,
									"url" -> (string("db.conn.jdbc")+":"+string("db.type.mysql")+"://"+host+":"+port+"/"+db+"?user="+user+"&password="+password),
									"dbtable" -> ("select "+ofullTableSchema+" from "+table+" where "+primarykey+" = "+pk+" and "+bookmark+" = "+bkmk) // table Specific
									)
							).load()
					// put the row into parent HDFS location + bookmarks wise year/month/date
					Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] inserting into the hdfs the entire (jdbc)DF")
					jdbcDF.write.partitionBy(bookmark).mode(SaveMode.Append).orc(formPartitionPath(string("sinks.hdfs.prefixPath"),db,table,bkmk))
					updateTargetIdsBackHDFS(db,table,pk,bkmk)
					"[STREAMING] [HDFS] updations EXITTED successfully == "+db+"_and_"+table+"_and_"+pk+"_and_"+bkmk
				}
				else if(conntype.contains(string("db.type.postgres"))){
				  Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] dbtype == postgres")
					val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
							Map(
									"driver" -> conntype, // "org.postgresql.Driver"
									"url" -> (string("db.conn.jdbc")+":"+string("db.type.postgres")+"://"+host+":"+port+"/"+db+"?user="+user+"&password="+password),
									"dbtable" -> ("select "+ofullTableSchema+" from "+table+" where "+primarykey+" = "+pk+" and "+bookmark+" = "+bkmk) // table Specific
									)
							).load()
					    // put the row into parent HDFS location + bookmarks wise year/month/date
					    Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] inserting into the hdfs the entire (jdbc)DF")
							jdbcDF.write.partitionBy(bookmark).mode(SaveMode.Append).orc(formPartitionPath(string("sinks.hdfs.prefixPath"),db,table,bkmk))
					    updateTargetIdsBackHDFS(db,table,pk,bkmk)
					    "updations EXITTED successfully == "+db+"_and_"+table+"_and_"+pk+"_and_"+bkmk
				}
				else if(conntype.contains(string("db.type.sqlserver"))){
				  Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] dbtype == sqlserver")
					val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
							Map(
									"driver" -> conntype, // "com.microsoft.sqlserver.jdbc.SQLServerDriver"
									"url" -> (string("db.conn.jdbc")+":"+string("db.type.sqlserver")+host+":"+port+";database="+db+";user="+user+";password="+password),
									"dbtable" -> ("select "+fullTableSchema+" from "+table+" where "+primarykey+" = "+pk+" and "+bookmark+" = "+bkmk) // table Specific
									)
							).load()
							// put the row into parent HDFS location + bookmarks wise year/month/date
							Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] inserting into the hdfs the entire (jdbc)DF")
							jdbcDF.write.partitionBy(bookmark).mode(SaveMode.Append).orc(formPartitionPath(string("sinks.hdfs.prefixPath"),db,table,bkmk))
							updateTargetIdsBackHDFS(db,table,pk,bkmk)
							"updations EXITTED successfully == "+db+"_and_"+table+"_and_"+pk+"_and_"+bkmk
				}
				else{ // no need to send it to either of kafka or hdfs
					// no supported DB type - return a random string with fullTableSchema
				  // KAFKA Over
				  Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] some unsupported db type . . .")
				  updateTargetIdsBackHDFS(db,table,pk,bkmk)
				  "updations EXITTED successfully"
				}
			}

  def preprocess(fetchFromFactTableString: String) = { // for all kinds of preprocessing - left empty
      Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] preprocessing func called . . .")
		  fetchFromFactTableString
		}

  def updateTargetIdsBackKafka(db: String, table: String, pk: String, bkmrk: String) = {
      Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] update the targets back . . .")
      internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
		  val statement = internalConnection.createStatement()
			val updateTargetQuery = "UPDATE "+string("db.internal.tables.status.name")+" SET "+string("db.internal.tables.status.cols.kafkaTargetId")+"="+string("db.internal.tables.status.cols.sourceId")+" where "+string("db.internal.tables.status.cols.primarykey")+"="+pk+" and "+(string("db.internal.tables.status.cols.sourceId"))+"="+bkmrk+";"
		  statement.executeQuery(updateTargetQuery) // perfectly fine if another upsert might have had happened in the meanwhile
		  Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] updated == "+pk+"_"+bkmrk)
		  internalConnection.close()	
  }
  def updateTargetIdsBackHDFS(db: String, table: String, pk: String, bkmrk: String) = {
      Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] update the targets back . . .")
      internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
		  val statement = internalConnection.createStatement()
			val updateTargetQuery = "UPDATE "+string("db.internal.tables.status.name")+" SET "+string("db.internal.tables.status.cols.hdfsTargetId")+"="+string("db.internal.tables.status.cols.sourceId")+" where "+string("db.internal.tables.status.cols.primarykey")+"="+pk+" and "+(string("db.internal.tables.status.cols.sourceId"))+"="+bkmrk+";"
		  statement.executeQuery(updateTargetQuery) // perfectly fine if another upsert might have had happened in the meanwhile
		  Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] updated == "+pk+"_"+bkmrk)
		  internalConnection.close()	
  }

  def formPartitionPath(prefix: String, db: String, table: String,bookmark: String) = {
      Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] form some goddamn partition path . . .")
		  prefix+db+"_"+table+"/dt="+bookmark.substring(0, int("sinks.hdfs.partitionEnd")) // not using the bookmark because of no partition,or infact
		}

  def piggyMerge(sc: SparkContext,sqlContext: SQLContext, db_to_query: String, table_to_query: String) = { // upsert HDFS work-around
		  // pig like load, join, generate -> store
      /*
       * one fallacy is that we will have to load all the partitions, because a very old sourceId might be just getting updated suxessfully
       * hdfsMAIN <- mainHDFS_URL+db_to_query+table_to_query
       * hdfsCHANGES <- mainHDFS_URL_db_to_query+table_to_query
       * merge hdfsMAIN + hdfsCHANGES --> update/overwrite the hdfsMAIN
       * join them on a key
       * overwrite hdfsMAIN location - try partitioning as year/month/date
       */
      Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] piggy Merge was called"+db_to_query+"_"+table_to_query)
      val fullTableSchema = StructType(ofullTableSchema.split(',').map { x => StructField(x,StringType,true) }) // schema
      
      val hdfsMAINdf = sqlContext.read.format("orc").load(getTable("main",db_to_query,table_to_query)).toDF(ofullTableSchema) // all partitions
      Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] hdfs Main got it . . .")
      val hdfsCHANGESdf = sqlContext.read.format("orc").load(getTable("changes",db_to_query,table_to_query)).toDF(ofullTableSchema) // all partition
      Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] hdfs Changes got it . . .")
      val hdfs1df = hdfsMAINdf.join(hdfsCHANGESdf, hdfsMAINdf(primarykey) !== hdfsCHANGESdf(primarykey)) // get the non affected ones ! Costly
      Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] hdfs1df got it . . .")
      val dfUnion = hdfs1df.unionAll(hdfsCHANGESdf)
      Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] dfUnion Main got it . . .")
      // pig like flow - uncommon records + new editions/changes
      // now this dfUnion overrides the hdfsMAINdf location - either bookmark partition or map it to date time etc. - even bookmark is fine
      Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] upserting . . .")
      dfUnion.write.mode(SaveMode.Overwrite).partitionBy(bookmark).format("orc").save(getTable("main", db_to_query, table_to_query)) // partition by needed ?!?
	    Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] upserted . . .")
  }

    def getTable(arg: String, db_to_query: String, table_to_query: String) = {
		  // placeholder -> return the main and changes tables only
      Holder.log.debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] getting the table . . .")
      arg+db_to_query+table_to_query
		}

}