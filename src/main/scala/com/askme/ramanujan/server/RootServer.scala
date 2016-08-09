package com.askme.ramanujan.server
import java.sql.DriverManager
import java.util.Calendar
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import com.askme.ramanujan.Configurable
import com.askme.ramanujan.actors.{KafkaWorkerActor, Listener, WorkerActor}
import com.askme.ramanujan.util.{DataSource, KafkaSource}
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.apache.spark.SparkContext
import org.apache.spark.scheduler._
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import spray.json.{DefaultJsonProtocol, _}

class RootServer(val config: Config) extends Configurable with Server with Logging with Serializable {

	val conf = sparkConf("spark")
	val sc: SparkContext = SparkContext.getOrCreate(conf) // ideally this is what one must do - getOrCreate

	val customSparkListener: CustomSparkListener = new CustomSparkListener()
	sc.addSparkListener(customSparkListener)

	val ssc = new StreamingContext(sc, Seconds(int("streaming.kafka.batchDuration")))

	// one sqlContext - to pass on
	val sqlContext = new SQLContext(sc)

	//create an actorSystem
	info("creating the actor system == "+string("actorSystem.name"))
	private implicit val pipelineSystem = ActorSystem(string("actorSystem.name"))

	// the listener, an option sys log class basically - one listener only
	info("creating the listener actor == "+string("actorSystem.actors.listener"))
	val listener = pipelineSystem.actorOf(Props(classOf[Listener],config), name = string("actorSystem.actors.listener")) // one listener

	//		val indexService = string("sinks.druid.overlord")//"overlord" // Your overlord's druid.service, with slashes replaced by colons.
	//		val firehosePattern = string("sinks.druid.firehose")//"druid:firehose:%s" // Make up a service pattern, include %s somewhere in it.
	//		val discoveryPath = string("sinks.druid.discovery")//"/druid/discovery" // Your overlord's druid.discovery.curator.path.
	//
	//		val curator = CuratorFrameworkFactory.builder().connectString(string("sinks.hdfs.zookeeper.url"))
	//		.retryPolicy(new ExponentialBackoffRetry(1000, 20, 30000))
	//		.build()
	//		curator.start()

	// the master class
	//info("creating the master actor == "+string("actorSystem.actors.master"))
	//val requestHandlerRef = pipelineSystem.actorOf(Props(classOf[RequestHandler],config,conf,listener,sqlContext), name = string("actorSystem.actors.master"))
	// take a table in - completely?

	val internalHost = string("db.internal.url") // get the host from env confs - tables bookmark & status mostly
	val internalPort = string("db.internal.port") // get the port from env confs - tables bookmark & status mostly
	val internalDB = string("db.internal.dbname") // get the port from env confs - tables bookmark & status mostly
	val internalUser = string("db.internal.user") // get the user from env confs - tables bookmark & status mostly
	val internalPassword = string("db.internal.password") // get the password from env confs - tables bookmark & status mostly

	val internalURL: String = string("db.conn.jdbc")+":"+string("db.conn.use")+"://"+internalHost+":"+internalPort+"/"+internalDB+"?zeroDateTimeBehavior=convertToNull" // the internal connection DB <status, bookmark, requests> etc

	def transform(request: String) = {
		val request_ = request.replaceAll("'","\"") // not needed anymore
		case class RequestObject(host: String, alias: String, port: String, user: String, password: String, db: String, table: String, bookmark: String, bookmarkformat: String, primaryKey: String, conntype: String,fullTableSchema: String,numOfPartitions: String,druidMetrics: String,druidDims: String,runFrequency: String,treatment: String)

		object RequestJsonProtocol extends DefaultJsonProtocol {
			implicit val RequestFormat = jsonFormat17(RequestObject)
		}
		import RequestJsonProtocol._
		val jsonValue = request_.parseJson //parequest_.parseJson

		debug("[MY DEBUG STATEMENTS] [SQL] [transform json value] == "+jsonValue)

		val requestObject = jsonValue.convertTo[RequestObject]

		val host = requestObject.host // e.g 10.0.16.98
		val alias = requestObject.alias
		val port = requestObject.port // e.g 3306
		val user = requestObject.user // e.g jyotishree
		val password = requestObject.password // e.g getit1234
		val db = requestObject.db // e.g payments
		val table = requestObject.table // e.g txn_tab
		val bookmark = requestObject.bookmark // bookmark key, e.g timestamp
		val bookmarkformat = requestObject.bookmarkformat // format of bookmark e.g dd-mm-YYYYTHH:MM:SSZ
		val primaryKey = requestObject.primaryKey // primaryKey
		val conntype = requestObject.conntype // e.g com.sql.MySQL.driver
		val fullTableSchema = requestObject.fullTableSchema // fully qualified table schema
		val numOfPartitions = requestObject.numOfPartitions
		val druidMetrics = requestObject.druidMetrics
		val druidDims = requestObject.druidDims
		val runFrequency = requestObject.runFrequency
		val treatment = requestObject.treatment

		val dataSource = new DataSource(config,conntype,host,alias,port,user,password,db,table,bookmark,bookmarkformat,primaryKey,fullTableSchema,numOfPartitions,druidMetrics,druidDims,runFrequency,treatment)
		dataSource
	}

	def transformKafka(request: String) = {
		val request_ = request.replaceAll("'","\"")
		case class RequestObject(cluster: String, topic: String, alias: String, groupName: String, reset_offset_on_start: String, auto_offset_reset: String, bookmark: String, bookmarkformat: String, primaryKey: String, partitionCol: String,druidMetrics: String,druidDims: String,treatment: String)

		object RequestJsonProtocol extends DefaultJsonProtocol {
			implicit val RequestFormat = jsonFormat13(RequestObject)
		}

		import RequestJsonProtocol._
		val jsonValue = request_.parseJson //parequest_.parseJson

		debug("[MY DEBUG STATEMENTS] [KAFKA] [transform json value] == "+jsonValue)

		val requestObject = jsonValue.convertTo[RequestObject]

		val cluster = requestObject.cluster // e.g 10.0.16.98
		val topic = requestObject.topic
		val alias = requestObject.alias // e.g 3306
		val groupName = requestObject.groupName // e.g jyotishree
		val reset_offset_on_start = requestObject.reset_offset_on_start // e.g getit1234
		val auto_offset_reset = requestObject.auto_offset_reset // e.g payments
		val bookmark = requestObject.bookmark // e.g txn_tab
		val bookmarkformat = requestObject.bookmarkformat // bookmark key, e.g timestamp
		val primaryKey = requestObject.primaryKey // format of bookmark e.g dd-mm-YYYYTHH:MM:SSZ
		val partitionCol = requestObject.partitionCol // primaryKey
		val druidMetrics = requestObject.druidMetrics // e.g com.sql.MySQL.driver
		val druidDims = requestObject.druidDims // fully qualified table schema
		val treatment = requestObject.treatment

		val kafkaSource = new KafkaSource(config,cluster,topic,alias,groupName,reset_offset_on_start,auto_offset_reset,bookmark,bookmarkformat,primaryKey,partitionCol,druidMetrics,druidDims,treatment)
		kafkaSource
	}

	def getMins(freqScalar: String, freqUnitMeasure: String): Int = {
		if(freqUnitMeasure.toLowerCase().contains(string("db.internal.tables.requests.defs.min"))){
			freqScalar.toInt
		}
		else if(freqUnitMeasure.toLowerCase().contains(string("db.internal.tables.requests.defs.hour"))){
			freqScalar.toInt * int("db.internal.tables.requests.defs.hourToMin") // ok if hardcoded
		}
		else if(freqUnitMeasure.toLowerCase().contains(string("db.internal.tables.requests.defs.day"))){
			freqScalar.toInt * int("db.internal.tables.requests.defs.dayToMin") // ok if hardcoded
		}
		else  if(freqUnitMeasure.toLowerCase().contains(string("db.internal.tables.requests.defs.week"))){
			freqScalar.toInt * int("db.internal.tables.requests.defs.weekToMin") // ok if hardcoded
		}
		else {
			freqScalar.toInt * int("db.internal.tables.requests.defs.monthToMin")
		}
	}

	def randomString(length: Int) = scala.util.Random.alphanumeric.take(length).mkString

	def startASpin(host: String,port: String,dbname: String,dbtable: String,currentDateStr: String,runStartMessage: String,request: String) = {
		val hash = randomString(10)
		val connection = DriverManager.getConnection(internalURL, internalUser, internalPassword)
		val insertRecRunningLOGS = "INSERT INTO `"+string("db.internal.tables.runninglogs.name")+"` (`"+string("db.internal.tables.runninglogs.cols.host")+"`,`"+string("db.internal.tables.runninglogs.cols.port")+"`,`"+string("db.internal.tables.runninglogs.cols.dbname")+"`,`"+string("db.internal.tables.runninglogs.cols.dbtable")+"`,`"+string("db.internal.tables.runninglogs.cols.runTimeStamp")+"`,`"+string("db.internal.tables.runninglogs.cols.hash")+"`,`"+string("db.internal.tables.runninglogs.cols.exceptions")+"`,`"+string("db.internal.tables.runninglogs.cols.notes")+"`) VALUES( '"+host+"','"+port+"','"+dbname+"','"+dbtable+"','"+currentDateStr+"','"+hash+"','none','"+runStartMessage+"')"
		val insertRecRunningStatement = connection.createStatement()
		insertRecRunningStatement.executeUpdate(insertRecRunningLOGS)
		val updateRequestsRunningQuery = "UPDATE "+string("db.internal.tables.requests.name")+" SET "+string("db.internal.tables.requests.cols.lastStarted")+" = \""+currentDateStr+"\" , "+string("db.internal.tables.requests.cols.totalRuns")+" = "+string("db.internal.tables.requests.cols.totalRuns")+" + 1 , "+string("db.internal.tables.requests.cols.currentState")+" = \""+string("db.internal.tables.requests.defs.defaultRunningState")+"\" where "+string("db.internal.tables.requests.cols.host")+" = \""+host+"\" and "+string("db.internal.tables.requests.cols.port")+" = \""+port+"\" and "+string("db.internal.tables.requests.cols.dbname")+" = \""+dbname+"\" and "+string("db.internal.tables.requests.cols.dbtable")+" = \""+dbtable+"\""
		val updateRequestsRunningStatement = connection.createStatement()
		updateRequestsRunningStatement.executeUpdate(updateRequestsRunningQuery)
		connection.close()
		val dataSource = transform(request)

		val workerActor = pipelineSystem.actorOf(Props(classOf[WorkerActor],config))
		workerActor ! new TableMessage(listener,dataSource,hash)
		//val druidActor = pipelineSystem.actorOf(Props(classOf[DruidActor],config))
		//druidActor ! new DruidMessage(listener,dataSource,hash)
	}

	def startAStream(cluster: String, topic: String, alias: String, groupName: String, currentDateStr: String, runStreamingMessage: String, request: String): Unit = {
		val hash = randomString(10)
		val connection = DriverManager.getConnection(internalURL, internalUser, internalPassword)
		val insertkafkaRecRunningLOGS = "INSERT INTO `"+string("db.internal.tables.kafkaRunninglogs.name")+"` (`"+string("db.internal.tables.kafkaRunninglogs.cols.cluster")+"`,`"+string("db.internal.tables.kafkaRunninglogs.cols.topic")+"`,`"+string("db.internal.tables.kafkaRunninglogs.cols.alias")+"`,`"+string("db.internal.tables.kafkaRunninglogs.cols.groupName")+"`,`"+string("db.internal.tables.kafkaRunninglogs.cols.runTimeStamp")+"`,`"+string("db.internal.tables.kafkaRunninglogs.cols.hash")+"`,`"+string("db.internal.tables.runninglogs.cols.exceptions")+"`,`"+string("db.internal.tables.runninglogs.cols.notes")+"`) VALUES( '"+cluster+"','"+topic+"','"+alias+"','"+groupName+"','"+currentDateStr+"','"+hash+"','none','"+runStreamingMessage+"')"
		val insertkafkaRecRunningStatement = connection.createStatement()
		insertkafkaRecRunningStatement.executeUpdate(insertkafkaRecRunningLOGS)
		Thread sleep 1000
		val updatekafkaRequestsRunningQuery = "UPDATE "+string("db.internal.tables.kafkaRequests.name")+" SET "+string("db.internal.tables.kafkaRequests.cols.lastStarted")+" = \""+currentDateStr+"\" , "+string("db.internal.tables.kafkaRequests.cols.currentState")+" = \""+string("db.internal.tables.kafkaRequests.defs.defaultRunningState")+"\" where "+string("db.internal.tables.kafkaRequests.cols.cluster")+" = \""+cluster+"\" and "+string("db.internal.tables.kafkaRequests.cols.topic")+" = \""+topic+"\" and "+string("db.internal.tables.kafkaRequests.cols.groupName")+" = \""+groupName+"\" and "+string("db.internal.tables.kafkaRequests.cols.alias")+" = \""+alias+"\""
		val updatekafkaRequestsRunningstatement = connection.createStatement()
		updatekafkaRequestsRunningstatement.executeUpdate(updatekafkaRequestsRunningQuery)

		val kafkaSource = transformKafka(request)

		val kafkaWorkerActor = pipelineSystem.actorOf(Props(classOf[KafkaWorkerActor],config))
		kafkaWorkerActor ! new KafkaMessage(listener,kafkaSource,hash)
		//val druidActor = pipelineSystem.actorOf(Props(classOf[DruidActor],config))
		//druidActor ! new DruidMessage(listener,dataSource,hash)
		connection.close()
	}
	/* commenting kafka streaming - the approach needs to be changed
	val kafkaStatement = connection.createStatement()

	val getAllKafkaRequestsQuery = "SELECT * FROM "+string("db.internal.tables.kafkaRequests.name")
	val kafkaResultSet = kafkaStatement.executeQuery(getAllKafkaRequestsQuery)

	while(kafkaResultSet.next()){
		// metadata
		val processDate = kafkaResultSet.getString(string("db.internal.tables.kafkaRequests.cols.processDate"))
		val request = kafkaResultSet.getString(string("db.internal.tables.kafkaRequests.cols.request"))
		val cluster = kafkaResultSet.getString(string("db.internal.tables.kafkaRequests.cols.cluster"))
		val topic = kafkaResultSet.getString(string("db.internal.tables.kafkaRequests.cols.topic"))
		val alias = kafkaResultSet.getString(string("db.internal.tables.kafkaRequests.cols.alias"))
		val groupName = kafkaResultSet.getString(string("db.internal.tables.kafkaRequests.cols.groupName"))
		// health
		val currentState = kafkaResultSet.getString(string("db.internal.tables.kafkaRequests.cols.currentState"))
		val exceptions = kafkaResultSet.getString(string("db.internal.tables.kafkaRequests.cols.exceptions"))
		val notes = kafkaResultSet.getString(string("db.internal.tables.kafkaRequests.cols.notes"))

		val format = new java.text.SimpleDateFormat(string("db.internal.tables.kafkaRequests.defs.defaultDateFormat"))
		val currentDateDate = Calendar.getInstance().getTime()
		val currentDateStr = format.format(currentDateDate)

		val runStreamingMessage = "starting the first Run Ever . . ."
		startAStream(cluster,topic,alias,groupName,currentDateStr,runStreamingMessage,request)
	}
	*/

	while(true) {
		val connection = DriverManager.getConnection(internalURL, internalUser, internalPassword)
		val statement = connection.createStatement()

		val getAllRequestsQuery = "SELECT * FROM "+string("db.internal.tables.requests.name")
		val resultSet = statement.executeQuery(getAllRequestsQuery)

		while(resultSet.next()){
			// metadata
			val request = resultSet.getString(string("db.internal.tables.requests.cols.request"))
			val host = resultSet.getString(string("db.internal.tables.requests.cols.host"))
			val port = resultSet.getString(string("db.internal.tables.requests.cols.port"))
			val dbname = resultSet.getString(string("db.internal.tables.requests.cols.dbname"))
			val dbtable = resultSet.getString(string("db.internal.tables.requests.cols.dbtable"))
			// health
			val lastStarted = resultSet.getString(string("db.internal.tables.requests.cols.lastStarted"))
			val lastEnded = resultSet.getString(string("db.internal.tables.requests.cols.lastEnded"))
			val runFrequency = resultSet.getString(string("db.internal.tables.requests.cols.runFrequency"))
			val totalRuns = resultSet.getString(string("db.internal.tables.requests.cols.totalRuns"))
			val successRuns = resultSet.getString(string("db.internal.tables.requests.cols.success"))
			val failureRuns = resultSet.getString(string("db.internal.tables.requests.cols.failure"))
			val currentState = resultSet.getString(string("db.internal.tables.requests.cols.currentState"))
			val exceptions = resultSet.getString(string("db.internal.tables.requests.cols.exceptions"))
			val notes = resultSet.getString(string("db.internal.tables.requests.cols.notes"))

			val format = new java.text.SimpleDateFormat(string("db.internal.tables.requests.defs.defaultDateFormat"))
			val lastStartedDate = format.parse(lastStarted)
			val lastEndedDate = format.parse(lastEnded)

			val currentDateDate = Calendar.getInstance().getTime()
			val currentDateStr = format.format(currentDateDate)

			if(totalRuns.toInt == 0){ // start it. the first time
			val runStartMessage = "starting the first Run Ever . . ."
				startASpin(host,port,dbname,dbtable,currentDateStr,runStartMessage,request)
			}
			else{
				val diffInMinsEndedStarted = TimeUnit.MINUTES.convert(lastEndedDate.getTime() - lastStartedDate.getTime(),TimeUnit.MILLISECONDS)
				if(diffInMinsEndedStarted < 0){ // it must be either still running or failed !
					if(currentState == string("db.internal.tables.requests.defs.defaultRunningState")){
					}
					else{
						val diffInMinsCurrentStarted = TimeUnit.MINUTES.convert(currentDateDate.getTime() - lastStartedDate.getTime(),TimeUnit.MILLISECONDS)
						var freqInMins = 0
						try {
							val schedulingFrequencyParts = runFrequency.trim().split(" ")

							val freqScalar = schedulingFrequencyParts(0)
							val freqUnitMeasure = schedulingFrequencyParts(1)
							freqInMins = getMins(freqScalar, freqUnitMeasure)
						}
						catch{
							case _ => {
								freqInMins = int("db.internal.tables.requests.def.defaultWaitingTime")
							}
						}
						var runStartMessage = ""
						if(diffInMinsCurrentStarted > freqInMins){
							runStartMessage = "starting the Next Run | previous NOT OK . . ."
						}
						else{
							runStartMessage = "starting the Next Run before next scheduled time | previous NOT OK . . ."
						}
						startASpin(host,port,dbname,dbtable,currentDateStr,runStartMessage,request)
					}
				}
				else {
					val diffInMinsCurrentStarted = TimeUnit.MINUTES.convert(currentDateDate.getTime() - lastStartedDate.getTime(),TimeUnit.MILLISECONDS)
					val schedulingFrequencyParts = runFrequency.trim().split(" ")

					val freqScalar = schedulingFrequencyParts(0).trim()
					val freqUnitMeasure = schedulingFrequencyParts(1).trim()
					val freqInMins = getMins(freqScalar,freqUnitMeasure)

					if(diffInMinsCurrentStarted > freqInMins){
						val runStartMessage = "starting the Next Run | previous OK . . ."
						startASpin(host,port,dbname,dbtable,currentDateStr,runStartMessage,request)
					}
					else{
					}
				}
			}
		}
		connection.close()
	}

	class CustomSparkListener extends SparkListener {
		override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
			debug(s"application ended at time : ${applicationEnd.time}")
		}
		override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit ={
			debug(s"[SPARK LISTENER DEBUGS] application Start app attempt id : ${applicationStart.appAttemptId}")
			debug(s"[SPARK LISTENER DEBUGS] application Start app id : ${applicationStart.appId}")
			debug(s"[SPARK LISTENER DEBUGS] application start app name : ${applicationStart.appName}")
			debug(s"[SPARK LISTENER DEBUGS] applicaton start driver logs : ${applicationStart.driverLogs}")
			debug(s"[SPARK LISTENER DEBUGS] application start spark user : ${applicationStart.sparkUser}")
			debug(s"[SPARK LISTENER DEBUGS] application start time : ${applicationStart.time}")
		}
		override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
			debug(s"[SPARK LISTENER DEBUGS] ${executorAdded.executorId}")
			debug(s"[SPARK LISTENER DEBUGS] ${executorAdded.executorInfo}")
			debug(s"[SPARK LISTENER DEBUGS] ${executorAdded.time}")
		}
		override  def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
			debug(s"[SPARK LISTENER DEBUGS] the executor removed Id : ${executorRemoved.executorId}")
			debug(s"[SPARK LISTENER DEBUGS] the executor removed reason : ${executorRemoved.reason}")
			debug(s"[SPARK LISTENER DEBUGS] the executor temoved at time : ${executorRemoved.time}")
		}

		override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
			debug(s"[SPARK LISTENER DEBUGS] job End id : ${jobEnd.jobId}")
			debug(s"[SPARK LISTENER DEBUGS] job End job Result : ${jobEnd.jobResult}")
			debug(s"[SPARK LISTENER DEBUGS] job End time : ${jobEnd.time}")
		}
		override def onJobStart(jobStart: SparkListenerJobStart) {
			debug(s"[SPARK LISTENER DEBUGS] Job started with properties ${jobStart.properties}")
			debug(s"[SPARK LISTENER DEBUGS] Job started with time ${jobStart.time}")
			debug(s"[SPARK LISTENER DEBUGS] Job started with job id ${jobStart.jobId.toString}")
			debug(s"[SPARK LISTENER DEBUGS] Job started with stage ids ${jobStart.stageIds.toString()}")
			debug(s"[SPARK LISTENER DEBUGS] Job started with stages ${jobStart.stageInfos.size} : $jobStart")
		}

		override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
			debug(s"[SPARK LISTENER DEBUGS] Stage ${stageCompleted.stageInfo.stageId} completed with ${stageCompleted.stageInfo.numTasks} tasks.")
			debug(s"[SPARK LISTENER DEBUGS] Stage details : ${stageCompleted.stageInfo.details.toString}")
			debug(s"[SPARK LISTENER DEBUGS] Stage completion time : ${stageCompleted.stageInfo.completionTime}")
			debug(s"[SPARK LISTENER DEBUGS] Stage details : ${stageCompleted.stageInfo.rddInfos.toString()}")
		}
		override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
			debug(s"[SPARK LISTENER DEBUGS] Stage properties : ${stageSubmitted.properties}")
			debug(s"[SPARK LISTENER DEBUGS] Stage rddInfos : ${stageSubmitted.stageInfo.rddInfos.toString()}")
			debug(s"[SPARK LISTENER DEBUGS] Stage submission Time : ${stageSubmitted.stageInfo.submissionTime}")
			debug(s"[SPARK LISTENER DEBUGS] Stage submission details : ${stageSubmitted.stageInfo.details.toString()}")
		}
		override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
			debug(s"[SPARK LISTENER DEBUGS] task ended reason : ${taskEnd.reason}")
			debug(s"[SPARK LISTENER DEBUGS] task type : ${taskEnd.taskType}")
			debug(s"[SPARK LISTENER DEBUGS] task Metrics : ${taskEnd.taskMetrics}")
			debug(s"[SPARK LISTENER DEBUGS] task Info : ${taskEnd.taskInfo}")
			debug(s"[SPARK LISTENER DEBUGS] task stage Id : ${taskEnd.stageId}")
			debug(s"[SPARK LISTENER DEBUGS] task stage attempt Id : ${taskEnd.stageAttemptId}")
			debug(s"[SPARK LISTENER DEBUGS] task ended reason : ${taskEnd.reason}")
		}
		override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
			debug(s"[SPARK LISTENER DEBUGS] stage Attempt id : ${taskStart.stageAttemptId}")
			debug(s"[SPARK LISTENER DEBUGS] stage Id : ${taskStart.stageId}")
			debug(s"[SPARK LISTENER DEBUGS] task Info : ${taskStart.taskInfo}")
		}
		override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = {
			debug(s"[SPARK LISTENER DEBUGS] the unpersist RDD id : ${unpersistRDD.rddId}")
		}
	}

}

sealed trait PipeMessage
case class TableMessage(listener: ActorRef, dataSource: DataSource, hash: String) extends PipeMessage {require(!dataSource.table.isEmpty(), "table field cannot be empty");require(!dataSource.db.isEmpty(), "db field cannot be empty")}
case class KafkaMessage(listener: ActorRef, kafkaSource: KafkaSource, hash: String) extends PipeMessage {require(!kafkaSource.topic.isEmpty(), "topic field cannot be empty");require(!kafkaSource.cluster.isEmpty(), "cluster field cannot be empty")}
case class DruidMessage(listener: ActorRef, dataSource: DataSource, hash: String) extends PipeMessage {require(!dataSource.table.isEmpty(), "table field cannot be empty");require(!dataSource.db.isEmpty(), "db field cannot be empty")}
case class WorkerDone(dataSource: DataSource) extends PipeMessage
case class WorkerExecuting(dataSource: DataSource) extends PipeMessage