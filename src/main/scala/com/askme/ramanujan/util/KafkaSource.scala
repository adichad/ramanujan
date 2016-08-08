package com.askme.ramanujan.util

import java.sql.{Connection, DriverManager}
import java.util.Calendar

import com.askme.ramanujan.Configurable
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

/**
  * Created by Sumit on 31/07/16.
  */
class KafkaSource(val config: Config, val cluster: String, val topic: String, val alias: String, val groupName: String, val reset_offset_on_start: String, val auto_offset_reset: String, val bookmark: String, val bookmarkformat: String, val primaryKey: String, val hdfsPartitionCol: String, val druidMetrics: String, val druidDims: String, val treatment: String) extends Configurable with Logging with Serializable{

  def sparkToHiveDataTypeMapping(vartype: String): Any = {
    val typeConversionsSparktoHIVEMap = Map("BinaryType" -> "BINARY", "BooleanType" -> "BOOLEAN", "ByteType" -> "STRING", "DateType" -> "DATE", "DoubleType" -> "DOUBLE", "FloatType" -> "FLOAT", "IntegerType" -> "INT", "LongType" -> "BIGINT", "NullType" -> "STRING", "ShortType" -> "INT", "StringType" -> "STRING", "TimestampType" -> "TIMESTAMP")
    typeConversionsSparktoHIVEMap.get(vartype)
  }

  def appendType(varname: String, vartype: String): String = {
    varname+" "+sparkToHiveDataTypeMapping(vartype)
  }

  def getColAndType(dfSchema: StructType): String = {
    var hiveTableschema = scala.collection.mutable.MutableList[String]()
    val dfColLen = dfSchema.length
    var schemaDefinition = ""
    for(i <- 0 until dfColLen){
      val schema = dfSchema(i)
      val varname = schema.name.toString
      val vartype = schema.dataType.toString
      hiveTableschema += appendType(varname,vartype)
    }
    hiveTableschema.mkString(" , ")
  }

  def updateInKafkaRequestsFailed(hash: String, value: Exception) = {
    val strValue = value.toString().substring(0,math.min(value.toString().length(),int("db.internal.tables.requests.defs.excStrEnd")))
    val format = new java.text.SimpleDateFormat(string("db.internal.tables.requests.defs.defaultDateFormat"))
    val currentDateDate = Calendar.getInstance().getTime()
    val currentDateStr = format.format(currentDateDate)
    internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
    val statement = internalConnection.createStatement()
    val insertReqFailedQuery = "UPDATE "+string("db.internal.tables.kafkaRequests.name")+" SET "+string("db.internal.tables.kafkaRequests.cols.exceptions")+" = \""+strValue.replaceAll("[^a-zA-Z]", "")+"\" , "+string("db.internal.tables.kafkaRequests.cols.currentState")+" = \""+string("db.internal.tables.kafkaRequests.defs.defaultIdleState")+"\" where "+string("db.internal.tables.r=kafkaRequests.cols.cluster")+" = \""+cluster+"\" and "+string("db.internal.tables.kafkaRequests.cols.topic")+" = \""+topic+"\" and "+string("db.internal.tables.kafkaRequests.cols.alias")+" = \""+alias+"\" and "+string("db.internal.tables.kafkaRequests.cols.groupName")+" = \""+groupName+"\""
    statement.executeUpdate(insertReqFailedQuery)
    internalConnection.close()
  }

  def insertInKafkaRunLogsFailed(hash: String, value: Exception) = {
    val strValue = value.toString().substring(0,math.min(value.toString().length(),int("db.internal.tables.requests.defs.excStrEnd")))
    val format = new java.text.SimpleDateFormat(string("db.internal.tables.requests.defs.defaultDateFormat"))
    val currentDateDate = Calendar.getInstance().getTime()
    val currentDateStr = format.format(currentDateDate)
    internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
    val statement = internalConnection.createStatement()
    val insertFailLogQuery = "INSERT INTO `"+string("db.internal.tables.kafkaRunninglogs.name")+"` (`"+string("db.internal.tables.kafkaRunninglogs.cols.cluster")+"`,`"+string("db.internal.tables.kafkaRunninglogs.cols.topic")+"`,`"+string("db.internal.tables.kafkaRunninglogs.cols.alias")+"`,`"+string("db.internal.tables.kafkaRunninglogs.cols.groupName")+"`,`"+string("db.internal.tables.kafkaRunninglogs.cols.runTimeStamp")+"`,`"+string("db.internal.tables.kafkaRunninglogs.cols.hash")+"`,`"+string("db.internal.tables.kafkaRunninglogs.cols.exceptions")+"`,`"+string("db.internal.tables.kafkaRunninglogs.cols.notes")+"`) VALUES ('"+cluster+"','"+topic+"','"+alias+"','"+groupName+"','"+currentDateStr+"','"+hash+"','"+strValue.replaceAll("[^a-zA-Z]", "")+"','the run has failed . . .')"
    debug("[MY DEBUG STATEMENTS] INSERT LOG FAIL QUERY == "+insertFailLogQuery)
    statement.executeUpdate(insertFailLogQuery)
    internalConnection.close()
  }

  def updateInKafkaRequestsPassed(hash: String) = {
    val format = new java.text.SimpleDateFormat(string("db.internal.tables.requests.defs.defaultDateFormat"))
    val currentDateDate = Calendar.getInstance().getTime()
    val currentDateStr = format.format(currentDateDate)
    internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
    val statement = internalConnection.createStatement()
    val insertReqPassedQuery = "UPDATE "+string("db.internal.tables.kafkaRequests.name")+" SET "+string("db.internal.tables.kafkaRequests.cols.currentState")+" = \""+string("db.internal.tables.kafkaRequests.defs.defaultIdleState")+"\" where "+string("db.internal.tables.kafkaRequests.cols.cluster")+" = \""+cluster+"\" and "+string("db.internal.tables.kafkaRequests.cols.topic")+" = \""+topic+"\" and "+string("db.internal.tables.kafkaRequests.cols.alias")+" = \""+alias+"\" and "+string("db.internal.tables.kafkaRequests.cols.groupName")+" = \""+groupName+"\""
    statement.executeUpdate(insertReqPassedQuery)
    internalConnection.close()
  }

  def insertInKafkaRunLogsPassed(hash: String) = {
    val format = new java.text.SimpleDateFormat(string("db.internal.tables.requests.defs.defaultDateFormat"))
    val currentDateDate = Calendar.getInstance().getTime()
    val currentDateStr = format.format(currentDateDate)
    internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword) // getting internal DB connection : jdbc:mysql://localhost:3306/<db>
    val statement = internalConnection.createStatement()
    val insertPassLogQuery = "INSERT INTO `"+string("db.internal.tables.kafkaRunninglogs.name")+"` (`"+string("db.internal.tables.kafkaRunninglogs.cols.cluster")+"`,`"+string("db.internal.tables.kafkaRunninglogs.cols.topic")+"`,`"+string("db.internal.tables.kafkaRunninglogs.cols.alias")+"`,`"+string("db.internal.tables.kafkaRunninglogs.cols.groupName")+"`,`"+string("db.internal.tables.kafkaRunninglogs.cols.runTimeStamp")+"`,`"+string("db.internal.tables.kafkaRunninglogs.cols.hash")+"`,`"+string("db.internal.tables.kafkaRunninglogs.cols.exceptions")+"`,`"+string("db.internal.tables.kafkaRunninglogs.cols.notes")+"`) VALUES ('"+cluster+"','"+topic+"','"+alias+"','"+groupName+"','"+currentDateStr+"','"+hash+"','none','the last run passed')"
    statement.executeUpdate(insertPassLogQuery)
    internalConnection.close()
  }


  val mapToString = udf((col: String, treatment: String) => {
    if(treatment.toLowerCase() == "skip" || treatment.toLowerCase() == "report"){
      try{
        col.toString
      } catch{
        case e: Throwable => {
          if(treatment.toLowerCase() == "skip"){
            null.asInstanceOf[String]
          }
          else{
            debug("[MY DEBUG STATEMENTS] [EXCEPTION] [USER TYPE CONVERSIONS] reporting the exception == "+e.printStackTrace())
            null.asInstanceOf[String]
          }
        }
      }
    }
    else{
      col.toString
    }
  })

  val mapToDouble = udf((col: String, treatment: String) => {
    if(treatment.toLowerCase() == "skip" || treatment.toLowerCase() == "report"){
      try{
        col.toDouble
      } catch{
        case e: Throwable => {
          if(treatment.toLowerCase() == "skip"){
            null.asInstanceOf[Double]
          }
          else{
            debug("[MY DEBUG STATEMENTS] [EXCEPTION] [USER TYPE CONVERSIONS] reporting the exception == "+e.printStackTrace())
            null.asInstanceOf[Double]
          }
        }
      }
    }
    else{
      col.toDouble
    }
  })

  val mapToLong = udf((col: String, treatment: String) => {
    if(treatment.toLowerCase() == "skip" || treatment.toLowerCase() == "report"){
      try{
        col.toLong
      } catch{
        case e: Throwable => {
          if(treatment.toLowerCase() == "skip"){
            null.asInstanceOf[Long]
          }
          else{
            debug("[MY DEBUG STATEMENTS] [EXCEPTION] [USER TYPE CONVERSIONS] reporting the exception == "+e.printStackTrace())
            null.asInstanceOf[Long]
          }
        }
      }
    }
    else{
      col.toLong
    }
  })

  val mapToInt = udf((col: String, treatment: String) => {
    if(treatment.toLowerCase() == "skip" || treatment.toLowerCase() == "report"){
      try{
        col.toInt
      } catch{
        case e: Throwable => {
          if(treatment.toLowerCase() == "skip"){
            null.asInstanceOf[Int]
          }
          else{
            debug("[MY DEBUG STATEMENTS] [EXCEPTION] [USER TYPE CONVERSIONS] reporting the exception == "+e.printStackTrace())
            null.asInstanceOf[Int]
          }
        }
      }
    }
    else{
      col.toInt
    }
  })

  def convertTargetTypes(rddData: DataFrame): DataFrame = {
    var df = rddData
    internalConnection = DriverManager.getConnection(internalURL, internalUser, internalPassword)
    val statement = internalConnection.createStatement()
    val qualifiedTopicName = ocluster+"_"+otopic+"_"+oalias
    val getUserColTypesQuery = "SELECT "+string("db.internal.tables.kafkaVarTypeRecordsTable.cols.topicname")+", "+string("db.internal.tables.varTypeRecordsTable.cols.kafkaColname")+", "+string("db.internal.tables.varTypeRecordsTable.cols.kafkaUsertype")+" from "+string("db.internal.tables.kafkaVarTypeRecordsTable.name")+" where "+string("db.internal.tables.kafkaVarTypeRecordsTable.cols.topicname")+" = \""+qualifiedTopicName+"\";"
    val resultSet = statement.executeQuery(getUserColTypesQuery)
    while ( resultSet.next() ) {
      val kafkaColname = resultSet.getString(string("db.internal.tables.kafkaVarTypeRecordsTable.cols.kafkaColname"))
      val kafkaUsertype = resultSet.getString(string("db.internal.tables.varTypeRecordsTable.cols.kafkaUsertype"))
      if(kafkaUsertype.toLowerCase() == "bigint"){
        df = df.withColumn(kafkaColname, mapToLong(df(kafkaColname),lit(treatment)))
      }
      else if(kafkaUsertype.toLowerCase() == "int"){
        df = df.withColumn(kafkaColname, mapToInt(df(kafkaColname),lit(treatment)))
      }
      else if(kafkaUsertype.toLowerCase() == "double"){
        df = df.withColumn(kafkaColname, mapToDouble(df(kafkaColname),lit(treatment)))
      }
      else{
        df = df.withColumn(kafkaColname, mapToString(df(kafkaColname),lit(treatment)))
      }
    }
    df
  }


  val oConfig = config
  val ocluster = cluster
  val otopic = topic
  val oalias = alias
  val ogroupName = groupName
  val oreset_offset_on_start = reset_offset_on_start
  val oauto_offset_reset = auto_offset_reset
  val obookmark = bookmark
  val obookmarkformat = bookmarkformat
  val oprimaryKey = primaryKey

  var internalConnection:Connection = null
  val internalHost = string("db.internal.url") // get the host from env confs - tables bookmark & status mostly
  val internalPort = string("db.internal.port") // get the port from env confs - tables bookmark & status mostly
  val internalDB = string("db.internal.dbname") // get the port from env confs - tables bookmark & status mostly
  val internalUser = string("db.internal.user") // get the user from env confs - tables bookmark & status mostly
  val internalPassword = string("db.internal.password") // get the password from env confs - tables bookmark & status mostly

  val internalURL: String = (string("db.conn.jdbc")+":"+string("db.type.mysql")+"://"+internalHost+":"+internalPort+"/"+internalDB)//+"?zeroDateTimeBehavior=convertToNull") // the internal connection DB <status, bookmark, requests> etc
  val clusterTopicURL: String = ocluster+"_"+otopic+"_"+oalias

  override def toString(): String = (" this data source is == "+clusterTopicURL)
}
