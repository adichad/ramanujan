package com.askme.ramanujan.util

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.Calendar

import com.askme.ramanujan.Configurable
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/*
SQL data types :
CHARACTER [(length)] or CHAR [(length)]
VARCHAR (length)
BOOLEAN
SMALLINT
INTEGER or INT
DECIMAL [(p[,s])] or DEC [(p[,s])]
NUMERIC [(p[,s])]
REAL
FLOAT(p)
DOUBLE PRECISION
DATE
TIME
TIMESTAMP
CLOB [(length)] or CHARACTER LARGE OBJECT [(length)] or CHAR LARGE OBJECT [(length)]
BLOB [(length)] or BINARY LARGE OBJECT [(length)]

Parquet supported data types :
	BOOLEAN, INT32, INT64, INT96, FLOAT, DOUBLE, BYTE_ARRAY

Spark data types :
	BinaryType, BooleanType, ByteType, 	DateType, DoubleType, 	FloatType, 	IntegerType, LongType, 	NullType, 	ShortType, 	StringType, 	TimestampType
 */

class DataSource(val config: Config,val conntype: String, val host: String,val port: String,
								 val user: String, val password: String, val db: String, val table: String,val alias: String,
								 val bookmark: String, val primarykey: String, val fullTableSchema: String,
								 val numOfPartitions: String,val toDruid: Boolean,val druidMetrics: String,val druidDims: String, val schedulingFrequency: String,val treatment: String) extends Configurable with Logging with Serializable{

	// skip  / report / fail
	val longToDouble = udf(f = (col: String, treatment: String, hash: String) => {
		if (treatment.toLowerCase() == string("judgement.error.handling.skip") || treatment.toLowerCase() == string("judgement.error.handling.report")) {
			try {
				col.toDouble
			} catch {
				case e: Throwable => {
					if (treatment.toLowerCase() == string("judgement.error.handling.skip")) {
						null.asInstanceOf[Double]
					}
					else if (treatment.toLowerCase() == string("judgement.error.handling.report")) {
						debug("[MY DEBUG STATEMENTS] [EXCEPTION] [USER TYPE CONVERSIONS] reporting the exception == " + e.printStackTrace())
						val exceptions = e.toString().substring(0,math.min(e.toString().length(),int("db.internal.tables.requests.defs.excStrEnd"))).replaceAll("[^a-zA-Z]", "")
						reportExceptionRunningLogs(hash, exceptions)
						updateInRequestsException(exceptions)
						null.asInstanceOf[Double]
					}
					else {
						col.toDouble
					}
				}
			}
		}
		else {
			col.toDouble
		}
	} )

	val longToInt = udf((col: String, treatment: String, hash: String) => {
		if (treatment.toLowerCase() == string("judgement.error.handling.skip") || treatment.toLowerCase() == string("judgement.error.handling.report")) {
			try {
				col.toInt
			} catch {
				case e: Throwable => {
					if (treatment.toLowerCase() == string("judgement.error.handling.skip")) {
						null.asInstanceOf[Int]
					}
					else if (treatment.toLowerCase() == string("judgement.error.handling.report")) {
						debug("[MY DEBUG STATEMENTS] [EXCEPTION] [USER TYPE CONVERSIONS] reporting the exception == " + e.printStackTrace())
						val exceptions = e.toString().substring(0,math.min(e.toString().length(),int("db.internal.tables.requests.defs.excStrEnd"))).replaceAll("[^a-zA-Z]", "")
						reportExceptionRunningLogs(hash, exceptions)
						updateInRequestsException(exceptions)
						null.asInstanceOf[Int]
					}
					else {
						col.toInt
					}
				}
			}
		}
		else{
			col.toInt
		}
	} )

	val longToString = udf((col: String, treatment: String, hash: String) => {
		if (treatment.toLowerCase() == string("judgement.error.handling.skip") || treatment.toLowerCase() == string("judgement.error.handling.report")) {
			try {
				col.toString
			} catch {
				case e: Throwable => {
					if (treatment.toLowerCase() == string("judgement.error.handling.skip")) {
						null.asInstanceOf[String]
					}
					else if (treatment.toLowerCase() == string("judgement.error.handling.report")) {
						debug("[MY DEBUG STATEMENTS] [EXCEPTION] [USER TYPE CONVERSIONS] reporting the exception == " + e.printStackTrace())
						val exceptions = e.toString().substring(0,math.min(e.toString().length(),int("db.internal.tables.requests.defs.excStrEnd"))).replaceAll("[^a-zA-Z]", "")
						reportExceptionRunningLogs(hash, exceptions)
						updateInRequestsException(exceptions)
						null.asInstanceOf[String]
					}
					else {
						col.toString
					}
				}
			}
		}
		else{
			col.toString
		}
	} )

	val stringToInt = udf((col: String, treatment: String, hash: String) => {
		if (treatment.toLowerCase() == string("judgement.error.handling.skip") || treatment.toLowerCase() == string("judgement.error.handling.report")) {
			try {
				col.toInt
			} catch {
				case e: Throwable => {
					if (treatment.toLowerCase() == string("judgement.error.handling.skip")) {
						null.asInstanceOf[Int]
					}
					else if (treatment.toLowerCase() == string("judgement.error.handling.report")) {
						debug("[MY DEBUG STATEMENTS] [EXCEPTION] [USER TYPE CONVERSIONS] reporting the exception == " + e.printStackTrace())
						val exceptions = e.toString().substring(0,math.min(e.toString().length(),int("db.internal.tables.requests.defs.excStrEnd"))).replaceAll("[^a-zA-Z]", "")
						reportExceptionRunningLogs(hash, exceptions)
						updateInRequestsException(exceptions)
						null.asInstanceOf[Int]
					}
					else {
						col.toInt
					}
				}
			}
		}
		else{
			col.toInt
		}
	} )

	val stringToLong = udf((col: String, treatment: String, hash: String) => {
		if (treatment.toLowerCase() == string("judgement.error.handling.skip") || treatment.toLowerCase() == string("judgement.error.handling.report")) {
			try {
				col.toLong
			} catch {
				case e: Throwable => {
					if (treatment.toLowerCase() == string("judgement.error.handling.skip")) {
						null.asInstanceOf[Long]
					}
					else if (treatment.toLowerCase() == string("judgement.error.handling.report")) {
						debug("[MY DEBUG STATEMENTS] [EXCEPTION] [USER TYPE CONVERSIONS] reporting the exception == " + e.printStackTrace())
						val exceptions = e.toString().substring(0,math.min(e.toString().length(),int("db.internal.tables.requests.defs.excStrEnd"))).replaceAll("[^a-zA-Z]", "")
						reportExceptionRunningLogs(hash, exceptions)
						updateInRequestsException(exceptions)
						null.asInstanceOf[Long]
					}
					else {
						col.toLong
					}
				}
			}
		}
		else{
			col.toLong
		}
	} )

	val stringToDouble = udf((col: String, treatment: String, hash: String) => {
		if (treatment.toLowerCase() == string("judgement.error.handling.skip") || treatment.toLowerCase() == string("judgement.error.handling.report")) {
			try {
				col.toDouble
			} catch {
				case e: Throwable => {
					if (treatment.toLowerCase() == string("judgement.error.handling.skip")) {
						null.asInstanceOf[Double]
					}
					else if (treatment.toLowerCase() == string("judgement.error.handling.report")) {
						debug("[MY DEBUG STATEMENTS] [EXCEPTION] [USER TYPE CONVERSIONS] reporting the exception == " + e.printStackTrace())
						val exceptions = e.toString().substring(0,math.min(e.toString().length(),int("db.internal.tables.requests.defs.excStrEnd"))).replaceAll("[^a-zA-Z]", "")
						reportExceptionRunningLogs(hash, exceptions)
						updateInRequestsException(exceptions)
						null.asInstanceOf[Double]
					}
					else {
						col.toDouble
					}
				}
			}
		}
		else{
			col.toDouble
		}
	} )

	val intToLong = udf((col: String, treatment: String, hash: String) => {
		if (treatment.toLowerCase() == string("judgement.error.handling.skip") || treatment.toLowerCase() == string("judgement.error.handling.report")) {
			try {
				col.toLong
			} catch {
				case e: Throwable => {
					if (treatment.toLowerCase() == string("judgement.error.handling.skip")) {
						null.asInstanceOf[Long]
					}
					else if (treatment.toLowerCase() == string("judgement.error.handling.report")) {
						debug("[MY DEBUG STATEMENTS] [EXCEPTION] [USER TYPE CONVERSIONS] reporting the exception == " + e.printStackTrace())
						val exceptions = e.toString().substring(0,math.min(e.toString().length(),int("db.internal.tables.requests.defs.excStrEnd"))).replaceAll("[^a-zA-Z]", "")
						reportExceptionRunningLogs(hash, exceptions)
						updateInRequestsException(exceptions)
						null.asInstanceOf[Long]
					}
					else {
						col.toLong
					}
				}
			}
		}
		else{
			col.toLong
		}
	} )

	val intToString = udf((col: String, treatment: String, hash: String) => {
		if (treatment.toLowerCase() == string("judgement.error.handling.skip") || treatment.toLowerCase() == string("judgement.error.handling.report")) {
			try {
				col.toString
			} catch {
				case e: Throwable => {
					if (treatment.toLowerCase() == string("judgement.error.handling.skip")) {
						null.asInstanceOf[String]
					}
					else if (treatment.toLowerCase() == string("judgement.error.handling.report")) {
						debug("[MY DEBUG STATEMENTS] [EXCEPTION] [USER TYPE CONVERSIONS] reporting the exception == " + e.printStackTrace())
						val exceptions = e.toString().substring(0,math.min(e.toString().length(),int("db.internal.tables.requests.defs.excStrEnd"))).replaceAll("[^a-zA-Z]", "")
						reportExceptionRunningLogs(hash, exceptions)
						updateInRequestsException(exceptions)
						null.asInstanceOf[String]
					}
					else {
						col.toString
					}
				}
			}
		}
		else{
			col.toString
		}
	} )

	val intToDouble = udf((col: String, treatment: String, hash: String) => {
		if (treatment.toLowerCase() == string("judgement.error.handling.skip") || treatment.toLowerCase() == string("judgement.error.handling.report")) {
			try {
				col.toDouble
			} catch {
				case e: Throwable => {
					if (treatment.toLowerCase() == string("judgement.error.handling.skip")) {
						null.asInstanceOf[Double]
					}
					else if (treatment.toLowerCase() == string("judgement.error.handling.report")) {
						debug("[MY DEBUG STATEMENTS] [EXCEPTION] [USER TYPE CONVERSIONS] reporting the exception == " + e.printStackTrace())
						val exceptions = e.toString().substring(0,math.min(e.toString().length(),int("db.internal.tables.requests.defs.excStrEnd"))).replaceAll("[^a-zA-Z]", "")
						reportExceptionRunningLogs(hash, exceptions)
						updateInRequestsException(exceptions)
						null.asInstanceOf[Double]
					}
					else {
						col.toDouble
					}
				}
			}
		}
		else{
			col.toDouble
		}
	} )

	val doubleToInt = udf((col: String, treatment: String, hash: String) => {
		if (treatment.toLowerCase() == string("judgement.error.handling.skip") || treatment.toLowerCase() == string("judgement.error.handling.report")) {
			try {
				col.toInt
			} catch {
				case e: Throwable => {
					if (treatment.toLowerCase() == string("judgement.error.handling.skip")) {
						null.asInstanceOf[Int]
					}
					else if (treatment.toLowerCase() == string("judgement.error.handling.report")) {
						debug("[MY DEBUG STATEMENTS] [EXCEPTION] [USER TYPE CONVERSIONS] reporting the exception == " + e.printStackTrace())
						val exceptions = e.toString().substring(0,math.min(e.toString().length(),int("db.internal.tables.requests.defs.excStrEnd"))).replaceAll("[^a-zA-Z]", "")
						reportExceptionRunningLogs(hash, exceptions)
						updateInRequestsException(exceptions)
						null.asInstanceOf[Int]
					}
					else {
						col.toInt
					}
				}
			}
		}
		else{
			col.toInt
		}
	} )

	val doubleToLong = udf((col: String, treatment: String, hash: String) => {
		if (treatment.toLowerCase() == string("judgement.error.handling.skip") || treatment.toLowerCase() == string("judgement.error.handling.report")) {
			try {
				col.toLong
			} catch {
				case e: Throwable => {
					if (treatment.toLowerCase() == string("judgement.error.handling.skip")) {
						null.asInstanceOf[Long]
					}
					else if (treatment.toLowerCase() == string("judgement.error.handling.report")) {
						debug("[MY DEBUG STATEMENTS] [EXCEPTION] [USER TYPE CONVERSIONS] reporting the exception == " + e.printStackTrace())
						val exceptions = e.toString().substring(0,math.min(e.toString().length(),int("db.internal.tables.requests.defs.excStrEnd"))).replaceAll("[^a-zA-Z]", "")
						reportExceptionRunningLogs(hash, exceptions)
						updateInRequestsException(exceptions)
						null.asInstanceOf[Long]
					}
					else {
						col.toLong
					}
				}
			}
		}
		else{
			col.toLong
		}
	} )

	val doubleToString = udf((col: String, treatment: String, hash: String) => {
		if (treatment.toLowerCase() == string("judgement.error.handling.skip") || treatment.toLowerCase() == string("judgement.error.handling.report")) {
			try {
				col.toString
			} catch {
				case e: Throwable => {
					if (treatment.toLowerCase() == string("judgement.error.handling.skip")) {
						null.asInstanceOf[String]
					}
					else if (treatment.toLowerCase() == string("judgement.error.handling.report")) {
						debug("[MY DEBUG STATEMENTS] [EXCEPTION] [USER TYPE CONVERSIONS] reporting the exception == " + e.printStackTrace())
						val exceptions = e.toString().substring(0,math.min(e.toString().length(),int("db.internal.tables.requests.defs.excStrEnd"))).replaceAll("[^a-zA-Z]", "")
						reportExceptionRunningLogs(hash, exceptions)
						updateInRequestsException(exceptions)
						null.asInstanceOf[String]
					}
					else {
						col.toString
					}
				}
			}
		}
		else{
			col.toString
		}
	} )

	def convertTargetTypes(PKsAffectedDFSource: DataFrame, hash: String): DataFrame = {
		var df = PKsAffectedDFSource
		val dfSchema: StructType = PKsAffectedDFSource.schema
		val dfColLen = dfSchema.length

		for(i <- 0 until dfColLen){
			val schema: StructField = dfSchema(i)
			val varname: String = schema.name.toString
			val vartype: String = schema.dataType.toString

			debug("[MY DEBUG STATEMENTS] SCHEMA ISSUES BEFORE ### source == "+alias+"_"+db+"_"+table+" =and= varname == "+varname+" =and= vartype == "+vartype)

			if(vartype.toLowerCase.contains("db.formats.spark.datatypes.booleantype"))
				df = df.withColumn(varname,df(varname).cast(BooleanType)) // redundant but dont know.
			else if(vartype.toLowerCase.contains("db.formats.spark.datatypes.decimaltype"))
				df = df.withColumn(varname,df(varname).cast(DoubleType))
			else if(vartype.toLowerCase.contains("db.formats.spark.datatypes.shorttype"))
				df = df.withColumn(varname,df(varname).cast(IntegerType))
			else
				df = df.withColumn(varname,df(varname).cast(StringType))
		}

		internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword)

		val statement = internalConnection.createStatement()
		val qualifiedTableName = alias+"_"+db+"_"+table
		val getUserColTypesQuery = "SELECT "+string("db.internal.tables.varTypeRecordsTable.cols.tablename")+", "+string("db.internal.tables.varTypeRecordsTable.cols.colname")+", "+string("db.internal.tables.varTypeRecordsTable.cols.coltype")+", "+string("db.internal.tables.varTypeRecordsTable.cols.usertype")+" from "+string("db.internal.tables.varTypeRecordsTable.name")+" where "+string("db.internal.tables.varTypeRecordsTable.cols.tablename")+" = \""+qualifiedTableName+"\";"
		val resultSet = statement.executeQuery(getUserColTypesQuery)
		while ( resultSet.next() ) {
			val colname = resultSet.getString(string("db.internal.tables.varTypeRecordsTable.cols.colname"))
			val coltype = resultSet.getString(string("db.internal.tables.varTypeRecordsTable.cols.coltype"))
			val usertype = resultSet.getString(string("db.internal.tables.varTypeRecordsTable.cols.usertype"))
			debug("[MY DEBUG STATEMENTS] SCHEMA ISSUES ### source == "+alias+"_"+db+"_"+table+" =and= colname == "+colname+" =and= coltype == "+coltype+" =and= usertype == "+usertype)
			if(!(usertype.toLowerCase() == string("sinks.hdfs.nullUserType"))) {
						if (usertype.toLowerCase() == "db.formats.hive.datatypes.bigint")
							df = df.withColumn(colname, df(colname).cast(LongType))
						else if (usertype.toLowerCase() == "db.formats.hive.datatypes.int")
							df = df.withColumn(colname, df(colname).cast(IntegerType))
						else if (usertype.toLowerCase() == "db.formats.hive.datatypes.float")
							df = df.withColumn(colname, df(colname).cast(DoubleType))
						else if (usertype.toLowerCase() == "db.formats.hive.datatypes.double")
							df = df.withColumn(colname, df(colname).cast(DoubleType))
						else
							df = df.withColumn(colname, df(colname).cast(StringType))
			}
		}
		val dfSchemaAfter: StructType = df.schema
		val dfColLenAfter = dfSchema.length

		for(i <- 0 until dfColLen){
			val schema: StructField = dfSchema(i)
			val varname: String = schema.name.toString
			val vartype: String = schema.dataType.toString

			debug("[MY DEBUG STATEMENTS] SCHEMA ISSUES AFTER ### source == "+alias+"_"+db+"_"+table+" =and= varname == "+varname+" =and= vartype == "+vartype)
		}
		internalConnection.close()
		df
	}

	def sparkToHiveDataTypeMapping(vartype: String): String = {
		if(vartype.toLowerCase().contains("decimaltype")){
			"Double"
		}
		else {
			val typeConversionsSparktoHIVEMap = Map("BinaryType" -> "BINARY", "BooleanType" -> "BOOLEAN", "ByteType" -> "STRING", "DateType" -> "DATE", "DoubleType" -> "DOUBLE", "FloatType" -> "FLOAT", "IntegerType" -> "INT", "LongType" -> "BIGINT", "NullType" -> "STRING", "ShortType" -> "INT", "StringType" -> "STRING", "TimestampType" -> "TIMESTAMP")
			typeConversionsSparktoHIVEMap.getOrElse(vartype, "STRING")
		}
	}

	def appendTypeUser(colname: String, usertype: String): String = {
		colname+" "+usertype
	}
	def appendTypeSchema(colname: String,map: Map[String,String]): String = {
		colname+" "+map.getOrElse(colname,"String")
	}

	def getColAndType(dfSchema: StructType): String = {
		val columns = fullTableSchema.split(",")

		var sparkSchemaColsMap : Map[String,String] = Map()
		val dfColLen = dfSchema.length

		for(i <- 0 until dfColLen){
			val schema = dfSchema(i)
			val varname = schema.name.toString
			val vartype = schema.dataType.toString
			sparkSchemaColsMap += (varname -> sparkToHiveDataTypeMapping(vartype))
		}

		var hiveTableschema = scala.collection.mutable.MutableList[String]()

		internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword)
		val statement = internalConnection.createStatement()
		val qualifiedTableName = alias+"_"+db+"_"+table
		val getUserColTypesQuery = "SELECT "+string("db.internal.tables.varTypeRecordsTable.cols.tablename")+", "+string("db.internal.tables.varTypeRecordsTable.cols.colname")+", "+string("db.internal.tables.varTypeRecordsTable.cols.coltype")+", "+string("db.internal.tables.varTypeRecordsTable.cols.usertype")+" from "+string("db.internal.tables.varTypeRecordsTable.name")+" where "+string("db.internal.tables.varTypeRecordsTable.cols.tablename")+" = \""+qualifiedTableName+"\";"
		val resultSet = statement.executeQuery(getUserColTypesQuery)
		while ( resultSet.next() ) {
			val colname = resultSet.getString(string("db.internal.tables.varTypeRecordsTable.cols.colname"))
			val coltype = resultSet.getString(string("db.internal.tables.varTypeRecordsTable.cols.coltype"))
			val usertype = resultSet.getString(string("db.internal.tables.varTypeRecordsTable.cols.usertype"))
			if(!(usertype.toLowerCase() == string("sinks.hdfs.nullUserType"))){
				hiveTableschema += appendTypeUser(colname,usertype)
			}
			else {
				hiveTableschema += appendTypeSchema(colname,sparkSchemaColsMap)
			}
		}
		internalConnection.close()
		//val typecastedColumns = columns.map(appendType(_,coltype,userTypeColsMap)).mkString(" , ")
		debug("[MY DEBUG STATEMENTS] [GET COL AND TYPE] hive table schema BE LIKE == " + hiveTableschema.mkString(" , "))
		hiveTableschema.mkString(" , ")
	}

	val oConfig = config // maybe this relays config to them all
	var oconntype: String = conntype // e.g com.mysql.driver.MySQL
	var ohost: String = host // host ip
	var oalias: String = alias
	var oport: String = port // port no.
	var ouser: String = user // user
	var opassword: String = password // password
	var odb: String = db //  dbname
	var otable: String = table // tablename
	var obookmark: String = bookmark // bookmark column - e.g timestamp etc etc.
	var oprimarykey: String = primarykey // primarykey - e.g orderId / payment status / transaction id etc etc
	var ofullTableSchema: String = fullTableSchema // full schema of the table - we pull entire row for Kafka HDFS

	var internalConnection:Connection = null
	val internalHost = string("db.internal.url") // get the host from env confs - tables bookmark & status mostly
	val internalPort = string("db.internal.port") // get the port from env confs - tables bookmark & status mostly
	val internalDB = string("db.internal.dbname") // get the port from env confs - tables bookmark & status mostly
	val internalUser = string("db.internal.user") // get the user from env confs - tables bookmark & status mostly
	val internalPassword = string("db.internal.password") // get the password from env confs - tables bookmark & status mostly

	val hostURL: String = (string("db.conn.jdbc")+":"+string("db.type.mysql")+"://"+host+":"+port+"/"+db+"?zeroDateTimeBehavior=convertToNull") // pertains to this very db+table : fully qualified name
	val internalURL: String = (string("db.conn.jdbc")+":"+string("db.type.mysql")+"://"+internalHost+":"+internalPort+"/"+internalDB)//+"?zeroDateTimeBehavior=convertToNull") // the internal connection DB <status, bookmark, requests> etc
	// returns the //info things
	override def toString(): String = (" this data source is == "+hostURL) // to Strings : db specific
	def insertInitRow(json: String) = {
		internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
		val statement = internalConnection.createStatement()
		val timeNow = Calendar.getInstance.getTime()
		val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
		val timeNowStr = format.format(timeNow)
		debug("INSERT INTO "+string("db.internal.tables.requests.name")+" ("+string("db.internal.tables.requests.cols.processDate")+","+string("db.internal.tables.requests.cols.host")+","+string("db.internal.tables.requests.cols.dbname")+","+string("db.internal.tables.requests.cols.dbtable")+","+string("db.internal.tables.requests.cols.request")+","+string("db.internal.tables.requests.cols.status")+")" + "VALUES( '"+timeNowStr+"','"+host+"','"+db+"','"+table+"','"+json+"','"+string("db.internal.tables.requests.defs.defaultStatusVal")+"')")
		val insertRequestQuery = "INSERT INTO "+string("db.internal.tables.requests.name")+" ("+string("db.internal.tables.requests.cols.processDate")+","+string("db.internal.tables.requests.cols.host")+","+string("db.internal.tables.requests.cols.dbname")+","+string("db.internal.tables.requests.cols.dbtable")+","+string("db.internal.tables.requests.cols.request")+","+string("db.internal.tables.requests.cols.status")+")" + "VALUES( '"+timeNowStr+"','"+host+"','"+db+"','"+table+"','"+json+"','"+string("db.internal.tables.requests.defs.defaultStatusVal")+"')" // insert '0' as status by default.
		//debug("[MY DEBUG STATEMENTS] the insertQuery == "+insertRequestQuery)
		statement.executeUpdate(insertRequestQuery)
		internalConnection.close()
	}

	def updateBookMark(currBookMark: String) = {
		internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
		val statement = internalConnection.createStatement()
		debug("INSERT INTO `"+string("db.internal.tables.bookmarks.name")+"` (`"+string("db.internal.tables.bookmarks.cols.dbtablekey")+"`,"+string("db.internal.tables.bookmarks.cols.bookmarkId")+") VALUES( '"+alias+"_"+db+"_"+table+"', '"+currBookMark+"')")
		val insertBkMrkQuery = "INSERT INTO `"+string("db.internal.tables.bookmarks.name")+"` (`"+string("db.internal.tables.bookmarks.cols.dbtablekey")+"`,"+string("db.internal.tables.bookmarks.cols.bookmarkId")+") VALUES( '"+db+"_"+table+"', '"+currBookMark+"')"
		statement.executeUpdate(insertBkMrkQuery)
		internalConnection.close()
	}

	def reportExceptionRunningLogs(hash: String, e: String) = {
		val format = new java.text.SimpleDateFormat(string("db.internal.tables.requests.defs.defaultDateFormat"))
		val currentDateDate = Calendar.getInstance().getTime()
		val currentDateStr = format.format(currentDateDate)
		internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
		val statement = internalConnection.createStatement()
		val updateRunLogsExceptionQuery = "UPDATE "+string("db.internal.tables.runninglogs.name")+" SET "+string("db.internal.tables.runninglogs.cols.exceptions")+" = "+e+" WHERE "+string("db.internal.tables.runninglogs.cols.hash")+" = "+hash
		statement.executeUpdate(updateRunLogsExceptionQuery)
		internalConnection.close()
	}

	def insertInRunLogsPassed(hash: String) = {
		val format = new java.text.SimpleDateFormat(string("db.internal.tables.requests.defs.defaultDateFormat"))
		val currentDateDate = Calendar.getInstance().getTime()
		val currentDateStr = format.format(currentDateDate)
		internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
		val statement = internalConnection.createStatement()
		val insertPassLogQuery = "INSERT INTO `"+string("db.internal.tables.runninglogs.name")+"` (`"+string("db.internal.tables.runninglogs.cols.host")+"`,`"+string("db.internal.tables.runninglogs.cols.port")+"`,`"+string("db.internal.tables.runninglogs.cols.dbname")+"`,`"+string("db.internal.tables.runninglogs.cols.dbtable")+"`,`"+string("db.internal.tables.runninglogs.cols.runTimeStamp")+"`,`"+string("db.internal.tables.runninglogs.cols.hash")+"`,`"+string("db.internal.tables.runninglogs.cols.exceptions")+"`,`"+string("db.internal.tables.runninglogs.cols.notes")+"`) VALUES ('"+host+"','"+port+"','"+db+"','"+table+"','"+currentDateStr+"','"+hash+"','none','the last run passed')"
		statement.executeUpdate(insertPassLogQuery)
		internalConnection.close()
	}

	def updateInRequestsException(e: String) = {
		val format = new java.text.SimpleDateFormat(string("db.internal.tables.requests.defs.defaultDateFormat"))
		val currentDateDate = Calendar.getInstance().getTime()
		val currentDateStr = format.format(currentDateDate)
		val statement = internalConnection.createStatement()
		val updateRequestsExceptionQuery = "UPDATE "+string("db.internal.tables.requests.name")+" SET "+string("db.internal.tables.requests.cols.exceptions")+" = \""+e+"\" where "+string("db.internal.tables.requests.cols.host")+" = \""+host+"\" and "+string("db.internal.tables.requests.cols.port")+" = \""+port+"\" and "+string("db.internal.tables.requests.cols.dbname")+" = \""+db+"\" and "+string("db.internal.tables.requests.cols.dbtable")+" = \""+table+"\""
		statement.executeUpdate(updateRequestsExceptionQuery)
		internalConnection.close()
	}

	def updateInRequestsPassed(hash: String) = {
		val format = new java.text.SimpleDateFormat(string("db.internal.tables.requests.defs.defaultDateFormat"))
		val currentDateDate = Calendar.getInstance().getTime()
		val currentDateStr = format.format(currentDateDate)
		internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
		val statement = internalConnection.createStatement()
		val insertReqPassedQuery = "UPDATE "+string("db.internal.tables.requests.name")+" SET "+string("db.internal.tables.requests.cols.success")+" = "+string("db.internal.tables.requests.cols.success")+" + 1 , "+string("db.internal.tables.requests.cols.currentState")+" = \""+string("db.internal.tables.requests.defs.defaultIdleState")+"\", "+string("db.internal.tables.requests.cols.lastEnded")+" = \""+currentDateStr+"\" where "+string("db.internal.tables.requests.cols.host")+" = \""+host+"\" and "+string("db.internal.tables.requests.cols.port")+" = \""+port+"\" and "+string("db.internal.tables.requests.cols.dbname")+" = \""+db+"\" and "+string("db.internal.tables.requests.cols.dbtable")+" = \""+table+"\""
		statement.executeUpdate(insertReqPassedQuery)
		internalConnection.close()
	}
	def updateInRequestsFailed(hash: String, value: Exception) = {
		val strValue = value.toString().substring(0,math.min(value.toString().length(),int("db.internal.tables.requests.defs.excStrEnd")))
		val format = new java.text.SimpleDateFormat(string("db.internal.tables.requests.defs.defaultDateFormat"))
		val currentDateDate = Calendar.getInstance().getTime()
		val currentDateStr = format.format(currentDateDate)
		internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
		val statement = internalConnection.createStatement()
		val insertReqFailedQuery = "UPDATE "+string("db.internal.tables.requests.name")+" SET "+string("db.internal.tables.requests.cols.exceptions")+" = \""+strValue.replaceAll("[^a-zA-Z]", "")+"\" , "+string("db.internal.tables.requests.cols.failure")+" = "+string("db.internal.tables.requests.cols.failure")+" + 1 , "+string("db.internal.tables.requests.cols.currentState")+" = \""+string("db.internal.tables.requests.defs.defaultIdleState")+"\" where "+string("db.internal.tables.requests.cols.host")+" = \""+host+"\" and "+string("db.internal.tables.requests.cols.port")+" = \""+port+"\" and "+string("db.internal.tables.requests.cols.dbname")+" = \""+db+"\" and "+string("db.internal.tables.requests.cols.dbtable")+" = \""+table+"\""
		statement.executeUpdate(insertReqFailedQuery)
		internalConnection.close()
	}

	def insertInRunLogsFailed(hash: String, value: Exception) = {
		val strValue = value.toString().substring(0,math.min(value.toString().length(),int("db.internal.tables.requests.defs.excStrEnd")))
		val format = new java.text.SimpleDateFormat(string("db.internal.tables.requests.defs.defaultDateFormat"))
		val currentDateDate = Calendar.getInstance().getTime()
		val currentDateStr = format.format(currentDateDate)
		internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
		val statement = internalConnection.createStatement()
		val insertFailLogQuery = "INSERT INTO `"+string("db.internal.tables.runninglogs.name")+"` (`"+string("db.internal.tables.runninglogs.cols.host")+"`,`"+string("db.internal.tables.runninglogs.cols.port")+"`,`"+string("db.internal.tables.runninglogs.cols.dbname")+"`,`"+string("db.internal.tables.runninglogs.cols.dbtable")+"`,`"+string("db.internal.tables.runninglogs.cols.runTimeStamp")+"`,`"+string("db.internal.tables.runninglogs.cols.hash")+"`,`"+string("db.internal.tables.runninglogs.cols.exceptions")+"`,`"+string("db.internal.tables.runninglogs.cols.notes")+"`) VALUES ('"+host+"','"+port+"','"+db+"','"+table+"','"+currentDateStr+"','"+hash+"','"+strValue.replaceAll("[^a-zA-Z]", "")+"','the run has failed . . .')"
		debug("[MY DEBUG STATEMENTS] INSERT LOG FAIL QUERY == "+insertFailLogQuery)
		statement.executeUpdate(insertFailLogQuery)
		internalConnection.close()
	}

	def insertInRunLogsStarted(hash: String) = {
		val format = new java.text.SimpleDateFormat(string("db.internal.tables.requests.defs.defaultDateFormat"))
		val currentDateDate = Calendar.getInstance().getTime()
		val currentDateStr = format.format(currentDateDate)
		internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
		val statement = internalConnection.createStatement()
		val insertStartLogQuery = "INSERT INTO `"+string("db.internal.tables.runninglogs.name")+"` (`"+string("db.internal.tables.runninglogs.cols.host")+"`,`"+string("db.internal.tables.runninglogs.cols.port")+"`,`"+string("db.internal.tables.runninglogs.cols.dbname")+"`,`"+string("db.internal.tables.runninglogs.cols.dbtable")+"`,`"+string("db.internal.tables.runninglogs.cols.runTimeStamp")+"`,`"+string("db.internal.tables.runninglogs.cols.hash")+"`,`"+string("db.internal.tables.runninglogs.cols.exceptions")+"`,`"+string("db.internal.tables.runninglogs.cols.notes")+"`) VALUES ('"+host+"','"+port+"','"+db+"','"+table+"','"+currentDateStr+"','"+hash+"','none','started the run')"
		statement.executeUpdate(insertStartLogQuery)
		internalConnection.close()
	}

	def getPrevBookMark() = { // this strictly returns the actual bookmark, e.g the timestamp
		internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
		val statement = internalConnection.createStatement()
		val prevOffsetFetchQuery = "select "+string("db.internal.tables.bookmarks.cols.dbtablekey")+" as "+string("db.internal.tables.bookmarks.cols.dbtablekey")+",max("+string("db.internal.tables.bookmarks.cols.id")+") as "+string("db.internal.tables.bookmarks.cols.id")+" from "+string("db.internal.tables.bookmarks.name")+" where "+string("db.internal.tables.bookmarks.cols.dbtablekey")+" in (\""+odb+"_"+otable+"\")"
		var resultSet = statement.executeQuery(prevOffsetFetchQuery) // handle first time cases also
		if(resultSet.next()){
			val key: String = resultSet.getString(string("db.internal.tables.bookmarks.cols.dbtablekey"))
			val id: String = resultSet.getString(string("db.internal.tables.bookmarks.cols.id"))
			val prevBookMarkFetchQuery = "select "+string("db.internal.tables.bookmarks.cols.bookmarkId")+" from "+string("db.internal.tables.bookmarks.name")+" where "+string("db.internal.tables.bookmarks.cols.dbtablekey")+" in (\""+ key +"\") and "+string("db.internal.tables.bookmarks.cols.id")+" in ("+ id +")"
			debug("[MY DEBUG STATEMENTS] [SQL] [BookMarks] prev bookmark fetch query . . . == "+prevOffsetFetchQuery)
			val prevBookMarkstatement = internalConnection.createStatement()
			resultSet = prevBookMarkstatement.executeQuery(prevBookMarkFetchQuery)
			if(resultSet.next()){
				val bookmark = resultSet.getString(string("db.internal.tables.bookmarks.cols.bookmarkId"))
				internalConnection.close()
				bookmark
			}
			else {
				internalConnection.close()
					string("db.internal.tables.bookmarks.defs.defaultIdBookMarkValue")
			}
		}
		else {
			internalConnection.close()
			debug("[MY DEBUG STATEMENTS] [SQL] [BookMarks] [previous] [NOTNULL] the default one . . . == " + string("db.internal.tables.bookmarks.defs.defaultIdBookMarkValue"))
			string("db.internal.tables.bookmarks.defs.defaultIdBookMarkValue")
		}
	}

	def getCurrBookMark(hash: String) = {
		val conf = sparkConf("spark")
		val sc = SparkContext.getOrCreate(conf)
		val sqlContext = new SQLContext(sc)

		debug("[MY DEBUG STATEMENTS] [SQL] [SPARKDF query check] [CURRENT BOOKMARK] == (select max("+bookmark+") as "+bookmark+" from "+table+" ) tmp"+alias.replaceAll("[^a-zA-Z]", "")+"_"+db.replaceAll("[^a-zA-Z]", "")+"_"+table.replaceAll("[^a-zA-Z]", ""))
		val getCurrentBookMarkQuery = "(select max("+bookmark+") as "+bookmark+" from "+table+" ) tmp"+alias.replaceAll("[^a-zA-Z]", "")+"_"+db.replaceAll("[^a-zA-Z]", "")+"_"+table.replaceAll("[^a-zA-Z]", "")

		debug("[MY DEBUG STATEMENTS] [SQL] [BookMarks] [current bookmark] getting current bookmark . . . ")
		if(conntype.toLowerCase().contains(string("db.type.mysql").toLowerCase())){// lowercase comparisons always
		val conf = sparkConf("spark")

			val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
				Map(
					"driver" -> conntype,
					"url" -> (string("db.conn.jdbc")+":"+string("db.type.mysql")+"://"+host+":"+port+"/"+db+"?user="+user+"&password="+password),
					"dbtable" -> getCurrentBookMarkQuery
				)
			).load()
			debug("[MY DEBUG STATEMENTS] [SQL] [BOOKMARKS] [current bookmark] got the current bookmark . . .")
			if(jdbcDF.rdd.isEmpty()){
				debug("[MY DEBUG STATEMENTS] [SQL] [BOOKMARKS] [current bookmark] the db of affected keys was all empty . . .")
				debug("[MY DEBUG STATEMENTS] [SQL] [BOOKMARKS] [current bookmark] returning the default value == "+string("db.internal.tables.bookmarks.defs.defaultIdBookMarkValue"))
				val exceptions = "current bookmark not found  | dataframe was empty"
				reportExceptionRunningLogs(hash,exceptions)
				updateInRequestsException(exceptions)
				string("db.internal.tables.bookmarks.defs.defaultIdBookMarkValue")
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
					"dbtable" -> getCurrentBookMarkQuery
				)
			).load()
			debug("[MY DEBUG STATEMENTS] [SQL] [BOOKMARKS] [current bookmark] got the current bookmark . . .")
			if(jdbcDF.rdd.isEmpty()){
				debug("[MY DEBUG STATEMENTS] [SQL] [BOOKMARKS] [current bookmark] the db of affected keys was all empty . . .")
				debug("[MY DEBUG STATEMENTS] [SQL] [BOOKMARKS] [current bookmark] returning the default value == "+string("db.internal.tables.bookmarks.defs.defaultIdBookMarkValue"))
				string("db.internal.tables.bookmarks.defs.defaultIdBookMarkValue")
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
				debug("[MY DEBUG STATEMENTS] [SQL] [BOOKMARKS] [current bookmark] returning the default value == "+string("db.internal.tables.bookmarks.defs.defaultIdBookMarkValue"))
				string("db.internal.tables.bookmarks.defs.defaultIdBookMarkValue")
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
			string("db.internal.tables.bookmarks.defs.defaultIdBookMarkValue")
		}
	}
	def getAffectedPKs(prevBookMark: String,currBookMark: String, hash: String) = {
		val conf = sparkConf("spark")
		val sc = SparkContext.getOrCreate(conf)
		val sqlContext = new SQLContext(sc)

		val getAffectedPKsSparkQuery = "(select "+fullTableSchema+" from "+table+" where "+bookmark+" >= \""+prevBookMark+"\" and "+bookmark+" <= \""+currBookMark+"\") overtmp_"+alias.replaceAll("[^a-zA-Z]", "")+"_"+db.replaceAll("[^a-zA-Z]", "")+"_"+table.replaceAll("[^a-zA-Z]", "")

		debug("[MY DEBUG STATEMENTS] [SQL] [AFFECTED PKs] getting affected PKs . . . ")
		if(conntype.toLowerCase().contains(string("db.type.mysql").toLowerCase())){
			val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
				Map(
					"driver" -> conntype,
					"url" -> (string("db.conn.jdbc")+":"+string("db.type.mysql")+"://"+host+":"+port+"/"+db+"?zeroDateTimeBehavior=convertToNull&user="+user+"&password="+password),
					"dbtable" -> (getAffectedPKsSparkQuery)
				)
			).load()
			jdbcDF
		}
		else if(conntype.toLowerCase().contains(string("db.type.postgres").toLowerCase())){
			val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
				Map(
					"driver" -> conntype,
					"url" -> (string("db.conn.jdbc")+":"+string("db.type.postgres")+"://"+host+":"+port+"/"+db+"?zeroDateTimeBehavior=convertToNull&user="+user+"&password="+password),
					"dbtable" -> (getAffectedPKsSparkQuery)
				)
			).load()
			jdbcDF
		}
		else if(conntype.toLowerCase().contains(string("db.type.sqlserver").toLowerCase())){
			val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
				Map(
					"driver" -> conntype,
					"url" -> (string("db.conn.jdbc")+":"+string("db.type.sqlserver")+"://"+host+":"+port+"/"+db+"?zeroDateTimeBehavior=convertToNull&user="+user+"&password="+password),
					"dbtable" -> (getAffectedPKsSparkQuery)
				)
			).load()
			jdbcDF
		}
		else{
			debug("[MY DEBUG STATEMENTS] [SQL] [AFFECTED PKs] entered some unsupported db / table . . .")
			debug("[MY DEBUG STATEMENTS] [SQL] [AFFECTED PKs] returning an empty / DF / . . .")
			val exceptions = "the db connection type provided is not supported . . ."
			reportExceptionRunningLogs(hash,exceptions)
			updateInRequestsException(exceptions)
			sqlContext.emptyDataFrame
		}
	}
	/*
	def upsert(primarykey: String, bookmark: String) = {
		internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
		val statement = internalConnection.createStatement()
		val upsertQuery = "INSERT INTO `"+string("db.internal.tables.status.name")+"` ("+string("db.internal.tables.status.cols.dbname")+","+string("db.internal.tables.status.cols.table")+",`"+string("db.internal.tables.status.cols.primarykey")+"`,"+string("db.internal.tables.status.cols.sourceId")+","+string("db.internal.tables.status.cols.kafkaTargetId")+string("db.internal.tables.status.cols.hdfsTargetId")+") VALUES( '"+db+"','"+table+"','"+primarykey+"', '"+bookmark+"',"+string("db.internal.tables.status.defs.defaultTargetId")+string("db.internal.tables.status.defs.defaultTargetId")+") ON DUPLICATE KEY UPDATE `"+string("db.internal.tables.status.cols.sourceId")+"` = '"+bookmark+"'"
		val resultSet = statement.executeQuery(upsertQuery)
		internalConnection.close()
	}
  def getPKs4UpdationKafka(conf: SparkConf,sqlContext: SQLContext) = { // get the primary keys for insertion, as well as their timestamps/ bookmarks
  val table_to_query = table; val db_to_query = db
    debug("[MY DEBUG STATEMENTS] [STREAMING] [PKs] [KAFKA] for == "+db_to_query+"_"+table_to_query)
    debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] [QUERY]  (select "+string("db.internal.tables.status.cols.primarykey")+","+string("db.internal.tables.status.cols.sourceId")+" from "+string("db.internal.tables.status.name")+" where "+string("db.internal.tables.status.cols.dbname")+" in (\""+db_to_query+"\") and "+string("db.internal.tables.status.cols.dbtable")+" in (\""+table_to_query+"\") and "+string("db.internal.tables.status.cols.sourceId")+" > "+string("db.internal.tables.status.cols.kafkaTargetId")+") kafka_tmp")
    val sc = SparkContext.getOrCreate(conf)
    val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
      Map(
        "driver" -> string("db.internal.driver"),
        "url" -> (internalURL+"?user="+internalUser+"&password="+internalPassword),
        "dbtable" -> ("(select "+string("db.internal.tables.status.cols.primarykey")+","+string("db.internal.tables.status.cols.sourceId")+" from "+string("db.internal.tables.status.name")+" where "+string("db.internal.tables.status.cols.dbname")+" in (\""+db_to_query+"\") and "+string("db.internal.tables.status.cols.dbtable")+" in (\""+table_to_query+"\") and "+string("db.internal.tables.status.cols.sourceId")+" > "+string("db.internal.tables.status.cols.kafkaTargetId")+") kafka_tmp")
      )
    ).load()
    if(jdbcDF.rdd.isEmpty()){
      debug("[MY DEBUG STATEMENTS] [STREAMING] [PKs] [KAFKA] an empty DF of PKs for == "+db_to_query+"_"+table_to_query)
      //info("no persistences done to the kafka sinks whatsoever . . . for == "+db_to_query+"_"+table_to_query)
    }
    else{
      debug("[MY DEBUG STATEMENTS] [STREAMING] [PKs] [KAFKA] going to persist into == "+db_to_query+"_"+table_to_query)
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
    debug("[MY DEBUG STATEMENTS] [STREAMING] [PKs] [HDFS] for == "+db_to_query+"_"+table_to_query)
    val sc = SparkContext.getOrCreate(conf)
    val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
      Map(
        "driver" -> string("db.internal.driver"),
        "url" -> (internalURL+"?user="+internalUser+"&password="+internalPassword),
        "dbtable" -> ("select "+string("db.internal.tables.status.cols.primarykey")+","+string("db.internal.tables.status.cols.sourceId")+" from "+string("db.internal.tables.status.name")+" where "+string("db.internal.tables.status.cols.dbname")+" = "+db_to_query+" and "+string("db.internal.tables.status.cols.dbtable")+" = "+table_to_query+" and "+string("db.internal.tables.status.cols.sourceId")+" > "+string("db.internal.tables.status.cols.hdfsTargetId"))
      )
    ).load()
    if(jdbcDF.rdd.isEmpty()){
      debug("[MY DEBUG STATEMENTS] [STREAMING] [PKs] [HDFS] an empty DF of PKs for == "+db_to_query+"_"+table_to_query)
      //info("no persistences done to the sinks whatsoever . . . for == "+db_to_query+"_"+table_to_query)
    }
    else{
      debug("[MY DEBUG STATEMENTS] [STREAMING] [PKs] [HDFS] going to persist into == "+db_to_query+"_"+table_to_query)
      jdbcDF.map { x => fetchFromFactTableAndSinkHDFS(x,sqlContext) } // hdfs append
    }
    debug("[MY DEBUG STATEMENTS] [STREAMING] [PKs] [HDFS] [PiggyMerge] going to persist into == "+db_to_query+"_"+table_to_query)
    piggyMerge(sc,sqlContext,db_to_query,table_to_query) // take the two tables, and merge them
  }

  def fetchFromFactTableAndSinkKafka(x: Row,sqlContext: SQLContext) : String = { // had to explicitly do that
  debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] entered the row-by-row sink . . .")
  val pk = x.getString(0) // get pk from the row
  val bkmk = x.getString(1) // get bookmark from row
  debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] doing it for == "+pk)

  val props = new Properties()
  props.put("metadata.broker.list", string("sinks.kafka.brokers"))
  props.put("serializer.class", string("sinks.kafka.serializer"))
  props.put("group.id", string("sinks.kafka.groupname"))
  props.put("producer.type", string("sinks.kafka.producer.type"))

  val config = new ProducerConfig(props)
  val producer = new Producer[String,String](config)

  if(conntype.contains(string("db.type.mysql"))){ // selecting entire rows - insertion into kafka as json and hdfs as ORC
    debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] db entered was == mysql")
    val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
      Map(
        "driver" -> conntype,
        "url" -> (string("db.conn.jdbc")+":"+string("db.type.mysql")+"://"+host+":"+port+"/"+db+"?user="+user+"&password="+password),
        "dbtable" -> ("select "+ofullTableSchema+" from "+table+" where "+primarykey+" = "+pk+" and "+bookmark+" = "+bkmk) // table Specific
      )
    ).load()
    val row_to_insert: Row = jdbcDF.first()
    debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] got the row . . .")
    var rowMap:Map[String,String] = Map()
    val fullTableSchemaArr = ofullTableSchema.split(",")
    for(i <- 0 until fullTableSchemaArr.length) {
      rowMap += (fullTableSchemaArr(i) -> row_to_insert.getString(i))
    } // contructing the json
    implicit val formats = Serialization.formats(NoTypeHints)
    val json_to_insert = Serialization.write(rowMap) // to be relayed to kafka
    val cleanStr = preprocess(json_to_insert) // for all kinds of preprocessing - left empty
    debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] the clean string to insert into Kafka == "+cleanStr)
    producer.send(new KeyedMessage[String,String](("[TOPIC] "+db+"_"+table),db,cleanStr)) // topic + key + message
    debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] published the clean string . . .")
    // KAFKA Over
    updateTargetIdsBackKafka(db,table,pk,bkmk)
    "[STREAMING] [KAFKA] updations EXITTED successfully == "+db+"_and_"+table+"_and_"+pk+"_and_"+bkmk
  }
  else if(conntype.contains(string("db.type.postgres"))){
    debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] db entered was == postgres")
    val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
      Map(
        "driver" -> conntype, // "org.postgresql.Driver"
        "url" -> (string("db.conn.jdbc")+":"+string("db.type.postgres")+"://"+host+":"+port+"/"+db+"?user="+user+"&password="+password),
        "dbtable" -> ("select "+ofullTableSchema+" from "+table+" where "+primarykey+" = "+pk+" and "+bookmark+" = "+bkmk) // table Specific
      )
    ).load()
    val row_to_insert: Row = jdbcDF.first()
    debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] got the row . . .")
    var rowMap:Map[String,String] = Map()
    val fullTableSchemaArr = ofullTableSchema.split(",")
    for(i <- 0 until fullTableSchemaArr.length) {
      rowMap += (fullTableSchemaArr(i) -> row_to_insert.getString(i))  // rather apply preprocessing here
    } // constructing the json
    implicit val formats = Serialization.formats(NoTypeHints)
    val json_to_insert = Serialization.write(rowMap) // to be relayed to kafka
    val cleanStr = preprocess(json_to_insert) // for all kinds of preprocessing - left empty
    debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] the clean string to insert into Kafka == "+cleanStr)
    producer.send(new KeyedMessage[String,String](("[TOPIC] "+db+"_"+table),db,cleanStr)) // topic + key + message
    debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] published the clean string . . .")
    // KAFKA Over
    updateTargetIdsBackKafka(db,table,pk,bkmk)
    "[STREAMING] [KAFKA] updations EXITTED successfully == "+db+"_and_"+table+"_and_"+pk+"_and_"+bkmk
  }
  else if(conntype.contains(string("db.type.sqlserver"))){
    debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] db entered was == sqlserver")
    val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
      Map(
        "driver" -> conntype, // "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        "url" -> (string("db.conn.jdbc")+":"+string("db.type.sqlserver")+host+":"+port+";database="+db+";user="+user+";password="+password),
        "dbtable" -> ("select "+fullTableSchema+" from "+table+" where "+primarykey+" = "+pk+" and "+bookmark+" = "+bkmk) // table Specific
      )
    ).load()
    val row_to_insert: Row = jdbcDF.first()
    debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] got the row . . .")
    var rowMap:Map[String,String] = Map()
    val fullTableSchemaArr = ofullTableSchema.split(",")
    for(i <- 0 until fullTableSchemaArr.length) {
      rowMap += (fullTableSchemaArr(i) -> row_to_insert.getString(i))
    }
    implicit val formats = Serialization.formats(NoTypeHints)
    val json_to_insert = Serialization.write(rowMap) // to be relayed to kafka
    val cleanStr = preprocess(json_to_insert) // for all kinds of preprocessing - left empty
    debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] the clean string to insert into Kafka == "+cleanStr)
    producer.send(new KeyedMessage[String,String](("[TOPIC] "+db+"_"+table),db,cleanStr)) // topic + key + message
    debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] published the clean string . . .")
    // KAFKA Over
    updateTargetIdsBackKafka(db,table,pk,bkmk)
    "[STREAMING] [KAFKA] updations EXITTED successfully == "+db+"_and_"+table+"_and_"+pk+"_and_"+bkmk
  }
  else{
    debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] nothing to publish . . .")
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
  debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] entered hdfs persistence function . . .")
  val pk = x.getString(0) // get pk from the row
  val bkmk = x.getString(1) // get bookmark from row
  debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] doing it for == "+pk)
  if(conntype.contains(string("db.type.mysql"))){ // selecting entire rows - insertion into kafka as json and hdfs as ORC
    debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] dbtype == mysql")
    val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
      Map(
        "driver" -> conntype,
        "url" -> (string("db.conn.jdbc")+":"+string("db.type.mysql")+"://"+host+":"+port+"/"+db+"?user="+user+"&password="+password),
        "dbtable" -> ("select "+ofullTableSchema+" from "+table+" where "+primarykey+" = "+pk+" and "+bookmark+" = "+bkmk) // table Specific
      )
    ).load()
    // put the row into parent HDFS location + bookmarks wise year/month/date
    debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] inserting into the hdfs the entire (jdbc)DF")
    jdbcDF.write.partitionBy(bookmark).mode(SaveMode.Append).orc(formPartitionPath(string("sinks.hdfs.prefixPath"),db,table,bkmk))
    updateTargetIdsBackHDFS(db,table,pk,bkmk)
    "[STREAMING] [HDFS] updations EXITTED successfully == "+db+"_and_"+table+"_and_"+pk+"_and_"+bkmk
  }
  else if(conntype.contains(string("db.type.postgres"))){
    debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] dbtype == postgres")
    val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
      Map(
        "driver" -> conntype, // "org.postgresql.Driver"
        "url" -> (string("db.conn.jdbc")+":"+string("db.type.postgres")+"://"+host+":"+port+"/"+db+"?user="+user+"&password="+password),
        "dbtable" -> ("select "+ofullTableSchema+" from "+table+" where "+primarykey+" = "+pk+" and "+bookmark+" = "+bkmk) // table Specific
      )
    ).load()
    // put the row into parent HDFS location + bookmarks wise year/month/date
    debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] inserting into the hdfs the entire (jdbc)DF")
    jdbcDF.write.partitionBy(bookmark).mode(SaveMode.Append).orc(formPartitionPath(string("sinks.hdfs.prefixPath"),db,table,bkmk))
    updateTargetIdsBackHDFS(db,table,pk,bkmk)
    "updations EXITTED successfully == "+db+"_and_"+table+"_and_"+pk+"_and_"+bkmk
  }
  else if(conntype.contains(string("db.type.sqlserver"))){
    debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] dbtype == sqlserver")
    val jdbcDF = sqlContext.read.format(string("db.conn.jdbc")).options(
      Map(
        "driver" -> conntype, // "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        "url" -> (string("db.conn.jdbc")+":"+string("db.type.sqlserver")+host+":"+port+";database="+db+";user="+user+";password="+password),
        "dbtable" -> ("select "+fullTableSchema+" from "+table+" where "+primarykey+" = "+pk+" and "+bookmark+" = "+bkmk) // table Specific
      )
    ).load()
    // put the row into parent HDFS location + bookmarks wise year/month/date
    debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] inserting into the hdfs the entire (jdbc)DF")
    jdbcDF.write.partitionBy(bookmark).mode(SaveMode.Append).orc(formPartitionPath(string("sinks.hdfs.prefixPath"),db,table,bkmk))
    updateTargetIdsBackHDFS(db,table,pk,bkmk)
    "updations EXITTED successfully == "+db+"_and_"+table+"_and_"+pk+"_and_"+bkmk
  }
  else{ // no need to send it to either of kafka or hdfs
    // no supported DB type - return a random string with fullTableSchema
    // KAFKA Over
    debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] some unsupported db type . . .")
    updateTargetIdsBackHDFS(db,table,pk,bkmk)
    "updations EXITTED successfully"
  }
  }

  def preprocess(fetchFromFactTableString: String) = { // for all kinds of preprocessing - left empty
  debug("[MY DEBUG STATEMENTS] [STREAMING] preprocessing func called . . .")
  fetchFromFactTableString
  }

  def updateTargetIdsBackKafka(db: String, table: String, pk: String, bkmrk: String) = {
  debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] update the targets back . . .")
  internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
  val statement = internalConnection.createStatement()
  val updateTargetQuery = "UPDATE "+string("db.internal.tables.status.name")+" SET "+string("db.internal.tables.status.cols.kafkaTargetId")+"="+string("db.internal.tables.status.cols.sourceId")+" where "+string("db.internal.tables.status.cols.primarykey")+"="+pk+" and "+(string("db.internal.tables.status.cols.sourceId"))+"="+bkmrk+";"
  statement.executeQuery(updateTargetQuery) // perfectly fine if another upsert might have had happened in the meanwhile
  debug("[MY DEBUG STATEMENTS] [STREAMING] [KAFKA] updated == "+pk+"_"+bkmrk)
  internalConnection.close()
  }
  def updateTargetIdsBackHDFS(db: String, table: String, pk: String, bkmrk: String) = {
  debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] update the targets back . . .")
  internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
  val statement = internalConnection.createStatement()
  val updateTargetQuery = "UPDATE "+string("db.internal.tables.status.name")+" SET "+string("db.internal.tables.status.cols.hdfsTargetId")+"="+string("db.internal.tables.status.cols.sourceId")+" where "+string("db.internal.tables.status.cols.primarykey")+"="+pk+" and "+(string("db.internal.tables.status.cols.sourceId"))+"="+bkmrk+";"
  statement.executeQuery(updateTargetQuery) // perfectly fine if another upsert might have had happened in the meanwhile
  debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] updated == "+pk+"_"+bkmrk)
  internalConnection.close()
  }

  def formPartitionPath(prefix: String, db: String, table: String,bookmark: String) = {
  debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] form some goddamn partition path . . .")
  prefix+db+"_"+table+"/dt="+bookmark.substring(0, math.min(bookmark.length(),int("sinks.hdfs.partitionEnd"))) // not using the bookmark because of no partition,or infact
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
  debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] piggy Merge was called"+db_to_query+"_"+table_to_query)
  val fullTableSchema = StructType(ofullTableSchema.split(',').map { x => StructField(x,StringType,true) }) // schema

  val hdfsMAINdf = sqlContext.read.format("orc").load(getTable("main",db_to_query,table_to_query)).toDF(ofullTableSchema) // all partitions
  debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] hdfs Main got it . . .")
  val hdfsCHANGESdf = sqlContext.read.format("orc").load(getTable("changes",db_to_query,table_to_query)).toDF(ofullTableSchema) // all partition
  debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] hdfs Changes got it . . .")
  val hdfs1df = hdfsMAINdf.join(hdfsCHANGESdf, hdfsMAINdf(primarykey) !== hdfsCHANGESdf(primarykey)) // get the non affected ones ! Costly
  debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] hdfs1df got it . . .")
  val dfUnion = hdfs1df.unionAll(hdfsCHANGESdf)
  debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] dfUnion Main got it . . .")
  debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] upserting . . .")
  dfUnion.write.mode(SaveMode.Overwrite).partitionBy(bookmark).format("orc").save(getTable("main", db_to_query, table_to_query)) // partition by needed ?!?
  debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] upserted . . .")
  }

  def getTable(arg: String, db_to_query: String, table_to_query: String) = {
  debug("[MY DEBUG STATEMENTS] [STREAMING] [HDFS] getting the table . . .")
  arg+db_to_query+table_to_query
  }
  */
}