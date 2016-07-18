package com.askme.ramanujan.actors

import java.util.Properties
import java.util.concurrent.Executors

import akka.actor.Actor
import com.askme.ramanujan.Configurable
import com.askme.ramanujan.server.{TableMessage, WorkerDone, WorkerExecuting}
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by Sumit on 18/07/16.
  */
class WorkerActor(val config: Config) extends Actor with Configurable with Logging {
  val conf = sparkConf("spark")
  val sc = SparkContext.getOrCreate(conf)
  val sqlContext = new SQLContext(sc)
  override def receive = {
    case TableMessage(listener,dataSource) => {
      import scala.concurrent._
      val numberOfCPUs = sys.runtime.availableProcessors()
      val threadPool = Executors.newFixedThreadPool(numberOfCPUs)
      implicit val ec = ExecutionContext.fromExecutorService(threadPool)
      Future {
        var prevBookMark = dataSource.getPrevBookMark()
        debug("[DEBUG] the previous bookmark == "+prevBookMark)
        var currBookMark = dataSource.getCurrBookMark()
        debug("[DEBUG] the current bookmark == "+currBookMark)
        var PKsAffectedDF: DataFrame = dataSource.getAffectedPKs(prevBookMark,currBookMark)
        debug("[DEBUG] the count of PKs returned == "+PKsAffectedDF.count())
        if(PKsAffectedDF.rdd.isEmpty()){
          info("[SQL] no records to upsert in the internal Status Table == for bookmarks : "+prevBookMark+" ==and== "+currBookMark+" for table == "+dataSource.db+"_"+dataSource.table)
        }
        def shortenTS: (String => String) = (ts: String) => {
          "dt="+ts.replaceAll(" ", "").substring(0, 10) // 10 digit partition size
        }
        import org.apache.spark.sql.functions._
        val partitionColFunc = udf(shortenTS)
        val PKsAffectedDF_partition = PKsAffectedDF.withColumn("TSpartitionKey", partitionColFunc(col(dataSource.bookmark)))

        import sqlContext.implicits._
        val keys = PKsAffectedDF_partition.select("TSpartitionKey").distinct.collect.flatMap(_.toSeq)
        val byPartitionArray = keys.map(key => PKsAffectedDF_partition.where($"TSpartitionKey" <=> key))

        val dbname = dataSource.db
        val dbtable = dataSource.table

        val servers = string("sinks.kafka.bootstrap.servers")
        val brokers = string("sinks.kafka.metadata.brokers.list")
        val group = string("sinks.kafka.group.id")
        val producertype = string("sinks.kafka.producer.type")
        val keyserial = string("sinks.kafka.key.serializer")
        val value = string("sinks.kafka.value.serializer")
        val topicCreateEnable = string("sinks.kafka.auto.create.topics.enable")

        val PKsAffectedDF_json = PKsAffectedDF_partition.toJSON

        PKsAffectedDF_json.foreachPartition { partitionOfRecords => {
          var props = new Properties()

          props.put("bootstrap.servers", servers) // "kafka01.production.askmebazaar.com:9092,kafka02.production.askmebazaar.com:9092,kafka03.production.askmebazaar.com:9092")
          //props.put("metadata.broker.list", "localhost:9092")//"kafka01.production.askmebazaar.com:2181,kafka02.production.askmebazaar.com:2181,kafka03.production.askmebazaar.com:2181")
          props.put("group.id", "ramanujan") //"ramanujan")
          props.put("producer.type", "async") //"async")
          props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer") //"org.apache.kafka.common.serialization.StringSerializer")
          props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer") //"org.apache.kafka.common.serialization.StringSerializer")
          props.put("request.required.acks", "1")
          props.put("auto.create.topics.enable", "true")
          //props.put("block.on.buffer.full","false")
          //props.put("advertised.host.name","localhost")

          val producer = new KafkaProducer[String, String](props)
          partitionOfRecords.foreach {
            case x: String => {
              val message = new ProducerRecord[String, String]("TOPIC_" + dbname + "_" + dbtable, dbname, x)
              producer.send(message).get()
            }
          }
          producer.close()
        }
        }

        for(i <- 0 to (keys.length - 1)) {
          val key_ = keys(i).toString()
          val df_ = byPartitionArray(i)
          // hdfs://<host>:<port>/user/home/cloudera/cm_api.py <host> is Hadoop NameNode host and the <port> port number of Hadoop NameNode, 50070
          // s3a://<<ACCESS_KEY>>:<<SECRET_KEY>>@<<BUCKET>>/<<FOLDER>>/<<FILE>>
          // http://www.infoobjects.com/different-ways-of-setting-aws-credentials-in-spark/ https://www.supergloo.com/fieldnotes/apache-spark-amazon-s3-examples-of-text-files/
          val hdfspath = "hdfs://" + string("sinks.hdfs.url") + ":" + string("sinks.hdfs.port") + "/" + dbname + "_" + dbtable + "/" + key_
          val hconf = sc.hadoopConfiguration
          val hdfs = org.apache.hadoop.fs.FileSystem.get(hconf)
          val partitionExistsBefore = hdfs.exists(new org.apache.hadoop.fs.Path(hdfspath))
          hconf.set("fs.default.name", "hdfs://" + string("sinks.hdfs.url") + ":" + string("sinks.hdfs.port"))

          if (partitionExistsBefore) {
            try {
              debug("[DEBUG] [HDFS] [DUMP] The path was present before == " + hdfspath)
              val partitionb4df = sqlContext.read.parquet(hdfspath).toDF((dataSource.fullTableSchema + ",TSpartitionKey").split(','): _*) //(dataSource.fullTableSchema)
              //val partitionb4df = sc.textFile(hdfspath+"part-r-").toDF((dataSource.fullTableSchema+",TSpartitionKey").split(',') : _*) //("hdfs://quickstart.cloudera:8020/user/cloudera/README.md")

              val partitionPopulationRecordsDB = partitionb4df.unionAll(df_)
              partitionPopulationRecordsDB.write.format("json").mode("overwrite").save(hdfspath)
              //hdfs.delete(new org.apache.hadoop.fs.Path(hdfspath), true)
              //partitionPopulationRecordsDB.write.mode(SaveMode.ErrorIfExists).parquet(hdfspath)
            } catch {
              case _: Throwable => debug("[DEBUG] [HDFS] [UPSERT] [PROBLEM] " + key_)
            }
          }
          else {
            debug("[DEBUG] [HDFS] [DUMP] The path was present before == " + hdfspath)
            df_.write.format("json").mode("overwrite").save(hdfspath) //.save(hdfspath)
          }
          debug("[DEBUG] updating the current bookmark for this db + table into BOOKMARKS table . . .")
          dataSource.updateBookMark(currBookMark)


        }

      }(ExecutionContext.Implicits.global) onSuccess {
        case _ => {
          listener ! new WorkerDone(dataSource)
        }

      }
      listener ! new WorkerExecuting(dataSource)
    }
  }
}
