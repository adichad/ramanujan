package com.askme.ramanujan.actors

import java.sql.DriverManager
import java.util.concurrent.Executors

import akka.actor.Actor
import com.askme.ramanujan.Configurable
import com.askme.ramanujan.server.TableMessage
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by Sumit on 18/07/16.
  */
class WorkerActor(val config: Config) extends Actor with Configurable with Logging {
  val conf = sparkConf("spark")
  val sc = SparkContext.getOrCreate(conf)
  val sqlContext = new SQLContext(sc)

  override def receive = {

    case TableMessage(listener, dataSource, hash) => {

      listener ! "Starting to sink the following == " + dataSource.toString()

      val hiveDriver = "org.apache.hive.jdbc.HiveDriver"
      Class.forName(hiveDriver)
      val hiveCon = DriverManager.getConnection("jdbc:hive2://localhost:10000/default")
      //debug("[MY DEBUG STATEMENTS] ESTABLISHED HIVE CONNECTION . . .")

      import scala.concurrent._
      val numberOfCPUs = sys.runtime.availableProcessors()
      val threadPool = Executors.newFixedThreadPool(numberOfCPUs)
      implicit val ec = ExecutionContext.fromExecutorService(threadPool)
      listener ! "throwing the datasource == " + dataSource.toString() + " to a future . . ."
      //Future {
      //try{
      //dataSource.insertInRunLogsStarted(hash)
      debug("[MY DEBUG STATEMENTS] [START ENTRY] inserted into Running Logs . . .")
      val prevBookMark = dataSource.getPrevBookMark()
      debug("[MY DEBUG STATEMENTS] {{" + hash + "}} the previous bookmark == " + prevBookMark)
      val currBookMark = dataSource.getCurrBookMark()
      debug("[MY DEBUG STATEMENTS] {{" + hash + "}} the current bookmark == " + currBookMark)
      val PKsAffectedDF: DataFrame = dataSource.getAffectedPKs(prevBookMark, currBookMark)
      val affectedPKsCount: Long = PKsAffectedDF.count()
      debug("[MY DEBUG STATEMENTS] {{" + hash + "}} the count of PKs returned == " + affectedPKsCount)
      val servers = string("sinks.kafka.bootstrap.servers")
      val brokers = string("sinks.kafka.metadata.brokers.list")
      val group = string("sinks.kafka.group.id")
      val producertype = string("sinks.kafka.producer.type")
      val keyserial = string("sinks.kafka.key.serializer")
      val valueserial = string("sinks.kafka.value.serializer")
      val topicCreateEnable = string("sinks.kafka.auto.create.topics.enable")
      if (affectedPKsCount == 0) {
        info("[SQL] {{" + hash + "}} no records to upsert in the internal Status Table == for bookmarks : " + prevBookMark + " ==and== " + currBookMark + " for table == " + dataSource.db + "_" + dataSource.table + " @host@ == " + dataSource.host)
      }
//      def shortenTS(ts: String,partKey: String): String = {
//        partKey + ts.replaceAll(" ", "").substring(0, 10) // 10 digit partition size
//      }

//      def lowerCase(colName: String, partKey: String): String = {
//        try {
//          "partitioned_on_"+partKey+"="+colName.toLowerCase
//        } catch {
//          case _: Throwable => {
//            "partitioned_on_"+partKey+"=defaultNullPartition"
//          }
//        }
//        // 10 digit partition size
//      }
      if (dataSource.hdfsPartitionCol.isEmpty()) {
        debug("[MY DEBUG STATEMENTS] [PICKING UP ### DEFAULT PARTITION ONLY] == " + dataSource.bookmark)
        val partitionColFunc = udf((ts: String,partKey: String) => {
          "partitioned_on_"+partKey + "='"+ts.replaceAll(" ", "_").substring(0, 10)+"'" // 10 digit partition size
        })
        val PKsAffectedDF_partition = PKsAffectedDF.withColumn("TSpartitionKey", partitionColFunc(col(dataSource.bookmark),lit("dt")))
        debug("[MY DEBUG STATEMENTS] atleast @@@PKsAffectedDF_partition@@@ formed . . .")
        import sqlContext.implicits._
        val keys = PKsAffectedDF_partition.select("TSpartitionKey").distinct.collect.flatMap(_.toSeq)
        debug("[MY DEBUG STATEMENTS] keys distinct for the partitions obtained . . .")
        val byPartitionArray = keys.map(key => PKsAffectedDF_partition.where($"TSpartitionKey" <=> key))
        debug("[MY DEBUG STATEMENTS] byPartitionArray formed . . .")
        val dbname = dataSource.db
        val dbtable = dataSource.table

        val PKsAffectedDF_json = PKsAffectedDF_partition.toJSON
        debug("[MY DEBUG STATEMENTS] PKsAffecteddf_JSON ==== json conversions . . .")
        /*
        PKsAffectedDF_json.foreachPartition {
          partitionOfRecords => {
            val props = new Properties()

            props.put("bootstrap.servers", "kafka01.production.askmebazaar.com:9092,kafka02.production.askmebazaar.com:9092,kafka03.production.askmebazaar.com:9092")
            //props.put("metadata.broker.list", "localhost:9092")//"kafka01.production.askmebazaar.com:2181,kafka02.production.askmebazaar.com:2181,kafka03.production.askmebazaar.com:2181")
            props.put("group.id", "ramanujan")
            props.put("producer.type", "async")
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
            props.put("request.required.acks", "1")
            props.put("auto.create.topics.enable", topicCreateEnable)
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
        */
        for (i <- 0 to (keys.length - 1)) {
          val key_ = keys(i).toString()
          val df_ = byPartitionArray(i)
          // hdfs://<host>:<port>/user/home/cloudera/cm_api.py <host> is Hadoop NameNode host and the <port> port number of Hadoop NameNode, 50070
          // s3a://<<ACCESS_KEY>>:<<SECRET_KEY>>@<<BUCKET>>/<<FOLDER>>/<<FILE>>
          // http://www.infoobjects.com/different-ways-of-setting-aws-credentials-in-spark/ https://www.supergloo.com/fieldnotes/apache-spark-amazon-s3-examples-of-text-files/
          val hdfspath = dbname + "_" + dbtable + "/" + key_
          val hconf = sc.hadoopConfiguration
          //hconf.addResource(new Path("/usr/local/hadoop/libexec/etc/hadoop/core-site.xml"))
          //hconf.addResource(new Path("/usr/local/hadoop/libexec/etc/hadoop/hdfs-site.xml"))
          //hconf.addResource(new Path("/usr/local/hadoop/libexec/etc/hadoop/yarn-site.xml"))
          //hconf.addResource(new Path("/usr/local/hadoop/libexec/etc/hadoop/mapred-site.xml"))
          hconf.set("fs.default.name", "hdfs://localhost:9000");
          val hdfs = org.apache.hadoop.fs.FileSystem.get(hconf)
          val partitionExistsBefore = hdfs.exists(new org.apache.hadoop.fs.Path(hdfspath))
          //hconf.set("fs.default.name", "hdfs://" + string("sinks.hdfs.url") + ":" + string("sinks.hdfs.port"))

          if (partitionExistsBefore) {
            debug("[MY DEBUG STATEMENTS] [HDFS] [DUMP] The path was present before == " + hdfspath)
            val partitionb4df = sqlContext.read.parquet(hdfspath).toDF((dataSource.fullTableSchema + ",TSpartitionKey").split(','): _*) //(dataSource.fullTableSchema)

            //val partitionb4df = sc.textFile(hdfspath+"part-r-").toDF((dataSource.fullTableSchema+",TSpartitionKey").split(',') : _*) //("hdfs://quickstart.cloudera:8020/user/cloudera/README.md")

            val df_deduped_ = df_.groupBy(dataSource.primarykey).max(dataSource.primarykey,dataSource.bookmark)
            val affectedPKs = df_deduped_.select(dataSource.primarykey).rdd.map(r => r(0).asInstanceOf[String]).collect()
            val sc = SparkContext.getOrCreate(conf)
            val affectedPKsBrdcst = sc.broadcast(affectedPKs)

            val func1a: (String => Boolean) = (arg: String) => !affectedPKsBrdcst.value.contains(arg)
            val sqlfunc1a = udf(func1a)
            val statusRecordsDB_1a = partitionb4df.filter(sqlfunc1a(col(dataSource.primarykey)))
            val statusRecordsDB_2a = df_deduped_.unionAll(statusRecordsDB_1a)
            statusRecordsDB_2a.write.format("json").mode("overwrite").save(hdfspath)
            //statusRecordsDB_2a.write.mode(SaveMode.Overwrite).parquet(hdfspath)

            //val partitionPopulationRecordsDB = partitionb4df.unionAll(df_)
            //val partitionPopulationRecordsDB_to_write = partitionPopulationRecordsDB.groupBy(dataSource.primarykey).max(dataSource.primarykey, dataSource.bookmark)
            //partitionPopulationRecordsDB_to_write.write.format("json").mode("overwrite").save(hdfspath)
            //hdfs.delete(new org.apache.hadoop.fs.Path(hdfspath), true)
            //partitionPopulationRecordsDB.write.mode(SaveMode.ErrorIfExists).parquet(hdfspath)
          }
          else {
            debug("[MY DEBUG STATEMENTS] [HDFS] [DUMP] The path was not present before == " + hdfspath)
            val df_to_write = df_.groupBy(dataSource.primarykey).max(dataSource.primarykey, dataSource.bookmark)
            //partitionPopulationRecordsDB_to_write.write.format("json").mode("overwrite").save(hdfspath)
            //df_.write.format("parquet").mode("overwrite").save(hdfspath) //.save(hdfspath)
            df_.write.format("com.databricks.spark.csv").mode("overwrite").save(hdfspath)
          }
        }
        debug("[MY DEBUG STATEMENTS] updating the current bookmark for this db + table into BOOKMARKS table . . .")
        dataSource.insertInRunLogsPassed(hash)
        dataSource.insertInRequestsPassed(hash)
        dataSource.updateBookMark(currBookMark)
      }
      else {
        debug("[MY DEBUG STATEMENTS] [PICKING UP ### SUPPLIED PARTITION ONLY] == " + dataSource.hdfsPartitionCol)
        val partitionColFunc = udf((colName: String, partKey: String) => {
          try {
            partKey+"="+colName
          } catch {
            case _: Throwable => {
              partKey+"=defaultNullPartition"
            }
          }
          // 10 digit partition size
        })
        val PKsAffectedDF_partition = PKsAffectedDF.withColumn("TSpartitionKey", partitionColFunc(col(dataSource.hdfsPartitionCol),lit(dataSource.hdfsPartitionCol)))
        debug("[MY DEBUG STATEMENTS] atleast @@@PKsAffectedDF_partition@@@ formed . . .")
        import sqlContext.implicits._
        val keys = PKsAffectedDF_partition.select("TSpartitionKey").distinct.collect.flatMap(_.toSeq)
        debug("[MY DEBUG STATEMENTS] keys distinct for the partitions obtained . . .")
        debug("[MY DEBUG STATEMENTS] [BEFORE KAFKA] atleast keys == ? ? ?")
        val byPartitionArray = keys.map(key => PKsAffectedDF_partition.where($"TSpartitionKey" <=> key))
        debug("[MY DEBUG STATEMENTS] byPartitionArray formed . . .")
        val dbname = dataSource.db
        val dbtable = dataSource.table

        val PKsAffectedDF_json = PKsAffectedDF_partition.toJSON
        debug("[MY DEBUG STATEMENTS] PKsAffecteddf_JSON ==== json conversions . . .")
        /*
        COMMENTING KAFKA AS OF NOW
        PKsAffectedDF_json.foreachPartition { partitionOfRecords => {
          val props = new Properties()
          props.put("bootstrap.servers", "kafka01.production.askmebazaar.com:9092,kafka02.production.askmebazaar.com:9092,kafka03.production.askmebazaar.com:9092")
          //props.put("metadata.broker.list", "localhost:9092")//"kafka01.production.askmebazaar.com:2181,kafka02.production.askmebazaar.com:2181,kafka03.production.askmebazaar.com:2181")
          props.put("group.id", "ramanujan")
          props.put("producer.type", "async")
          props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
          props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
          props.put("request.required.acks", "1")
          props.put("auto.create.topics.enable", topicCreateEnable)
          //              props.put("bootstrap.servers", servers) // "kafka01.production.askmebazaar.com:9092,kafka02.production.askmebazaar.com:9092,kafka03.production.askmebazaar.com:9092")
          //              //props.put("metadata.broker.list", "localhost:9092")//"kafka01.production.askmebazaar.com:2181,kafka02.production.askmebazaar.com:2181,kafka03.production.askmebazaar.com:2181")
          //              props.put("group.id", "ramanujan") //"ramanujan")
          //              props.put("producer.type", "async") //"async")
          //              props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer") //"org.apache.kafka.common.serialization.StringSerializer")
          //              props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer") //"org.apache.kafka.common.serialization.StringSerializer")
          //              props.put("request.required.acks", "1")
          //              props.put("auto.create.topics.enable", "true")
          //              //props.put("block.on.buffer.full","false")
          //              //props.put("advertised.host.name","localhost")

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
        */
        for (i <- 0 to (keys.length - 1)) {
          val partKey_ = keys(i).toString().split("=")(0)
          debug("[MY DEBUG STATEMENTS] WOOOOOOOOOOOOOOOOOOOOOLOOOO"+partKey_)
          val key_ = keys(i).toString().split("=")(1)
          debug("[MY DEBUG STATEMENTS] EEEEEEEEEEEEEEEEEEEEEEEWOOOOOOOOOOOOOOOOOOOOOLOOOO"+key_)
          val df_ = byPartitionArray(i)
          // hdfs://<host>:<port>/user/home/cloudera/cm_api.py <host> is Hadoop NameNode host and the <port> port number of Hadoop NameNode, 50070
          // s3a://<<ACCESS_KEY>>:<<SECRET_KEY>>@<<BUCKET>>/<<FOLDER>>/<<FILE>>
          // http://www.infoobjects.com/different-ways-of-setting-aws-credentials-in-spark/ https://www.supergloo.com/fieldnotes/apache-spark-amazon-s3-examples-of-text-files/

          //val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

          val parentHDFSPath = "hdfs://localhost:9000/"+dbname+"_"+dbtable

          val hdfspath = "hdfs://localhost:9000/"+dbname + "_" + dbtable + "/partitioned_on_" + keys(i).toString()
          val hdfspathTemp = "hdfs://localhost:9000/"+dbname + "_" + dbtable + "_tmp/partitioned_on_" + keys(i).toString()

          val hconf = sc.hadoopConfiguration
          //hconf.addResource(new Path("/usr/local/hadoop/libexec/etc/hadoop/core-site.xml"))
          //hconf.addResource(new Path("/usr/local/hadoop/libexec/etc/hadoop/hdfs-site.xml"))
          //hconf.addResource(new Path("/usr/local/hadoop/libexec/etc/hadoop/yarn-site.xml"))
          //hconf.addResource(new Path("/usr/local/hadoop/libexec/etc/hadoop/mapred-site.xml"))
          //hconf.set("fs.default.name", "hdfs://localhost:9000");
          debug("AAAAAAAAAAAAAAAAAAAAAAAAA conf.getRaw(fs.default.name)) === "+hconf.getRaw("fs.default.name"))
          debug("AAAAAAAAAAAAAAAAAAAAAAAAA conf.getRaw(fs.defaultFS)) === "+hconf.getRaw("fs.defaultFS"))
          hconf.set("fs.default.name", "hdfs://" + string("sinks.hdfs.url") + ":" + string("sinks.hdfs.port"))
          hconf.set("fs.defaultFS", "hdfs://" + string("sinks.hdfs.url") + ":" + string("sinks.hdfs.port"))
          debug("AAAAAAAAAAAAAAAAAAAAAAAAA conf.getRaw(fs.default.name)) === "+hconf.getRaw("fs.default.name"))
          debug("AAAAAAAAAAAAAAAAAAAAAAAAA conf.getRaw(fs.defaultFS)) === "+hconf.getRaw("fs.defaultFS"))
          val hdfs = org.apache.hadoop.fs.FileSystem.get(hconf)
          //val exists = hdfs.exists(new org.apache.hadoop.fs.Path(hdfspath))

          //hconf.set("fs.default.name", "hdfs://" + string("sinks.hdfs.url") + ":" + string("sinks.hdfs.port"))
          //conf.set("fs.defaultFS", "hdfs://" + string("sinks.hdfs.url") + ":" + string("sinks.hdfs.port"))
          //conf.set("mapreduce.jobtracker.address", string("sinks.hdfs.url")+":"+string("sinks.hdfs.port"))

          val parentPathExistsBefore = hdfs.exists(new Path(parentHDFSPath))
          debug("WOLLLLLLLLLLLLLLLLLLONGANGAAAAAA =parent= "+parentPathExistsBefore)

          if(!parentPathExistsBefore){
            debug("[MY DEBUG STATEMENTS] WOLLLLLLONGANGAAAAA =creating parent hdfs path ")
            hdfs.mkdirs(new Path(parentHDFSPath))
            val parentTableCreateQuery = "CREATE TABLE IF NOT EXISTS HIVE_TABLE_"+(dataSource.host).replaceAll("[^a-zA-Z]", "")+"_"+(dataSource.db).replaceAll("[^a-zA-Z]", "")+"_"+(dataSource.table).replaceAll("[^a-zA-Z]", "")+" ("+dataSource.getColAndType()+") COMMENT '"+dataSource.host+"_"+dataSource.db+"_"+dataSource.table+"' PARTITIONED BY (partitioned_on_"+dataSource.hdfsPartitionCol+" STRING)"// STORED AS PARQUET LOCATION '"+parentHDFSPath+"'"
            val hiveCreateTableStmt = hiveCon.createStatement()
            val createTableres = hiveCreateTableStmt.execute(parentTableCreateQuery)
            debug("[MY DEBUG STATEMENTS] create table statement executed . . . ")

            debug("[HIVE QUERY] == "+parentTableCreateQuery)
            //hiveContext.sql(parentTableCreateQuery)
          }
          else{
            debug("[MY DEBUG STATEMENTS] WOLLLLLLLLONGAAAANGAAAAA =parent hdfs path was already there . . .")
          }

          val partitionExistsBefore = hdfs.exists(new org.apache.hadoop.fs.Path(hdfspath))
          debug("WOLLLLLLLLLLLLLLLLLLLONGANGAAAAAA == "+partitionExistsBefore)

          if (partitionExistsBefore) {
              debug("WOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOD == "+key_)
            debug("[MY DEBUG STATEMENTS] [HDFS] [DUMP] The path was present before == " + hdfspath)
            val partitionb4df = sqlContext.read.parquet(hdfspath).toDF((dataSource.fullTableSchema + ",TSpartitionKey").split(','): _*) //(dataSource.fullTableSchema)
            debug("WOLEOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO partitionb4df COUNT == "+partitionb4df.count())
            //val partitionb4df = sc.textFile(hdfspath+"part-r-").toDF((dataSource.fullTableSchema+",TSpartitionKey").split(',') : _*) //("hdfs://quickstart.cloudera:8020/user/cloudera/README.md")

            //val w = Window.partitionBy($"\""+dataSource.primarykey+"\"").orderBy(($"\""+dataSource.bookmark+"\"").desc)
            //val w = Window.partitionBy($(dataSource.primarykey)).orderBy($(dataSource.bookmark).desc)
            val w = Window.partitionBy(col(dataSource.primarykey)).orderBy(col((dataSource.bookmark)).desc)
            val df_dedupe_ = df_.sort(col(dataSource.primarykey),col(dataSource.bookmark).desc).dropDuplicates(Seq(dataSource.primarykey))
            //debug("WOLEOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO df_dedupe_ COUNT == "+df_dedupe_.count())
            //val df_dedupe_ : DataFrame = df_.withColumn("rn", row_number.over(w)).where($"rn" === 1).drop("rn")

            //val df_deduped_ = df_.groupBy(dataSource.primarykey).max(dataSource.primarykey,dataSource.bookmark)
            val affectedPKs = df_dedupe_.select(dataSource.primarykey).rdd.map(r => r(0).asInstanceOf[String]).collect()
            val sc = SparkContext.getOrCreate(conf)
            val affectedPKsBrdcst = sc.broadcast(affectedPKs)

            val func1a: (String => Boolean) = (arg: String) => !affectedPKsBrdcst.value.contains(arg)
            val sqlfunc1a = udf(func1a)
            val statusRecordsDB_1a = partitionb4df.filter(sqlfunc1a(col(dataSource.primarykey)))
            //debug("WOLEOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO statusRecordsDB_1a COUNT == "+statusRecordsDB_1a.count())
            //affectedPKsBrdcst.destroy()
            val statusRecordsDB_2a = statusRecordsDB_1a.unionAll(df_dedupe_)
            //debug("WOLEOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO statusRecordsDB_2a COUNT == "+statusRecordsDB_2a.count())
            debug("BELOOOOOOOOOOOOOOOOOOOOOOOOONA ##### OVER $$$$$")
            //statusRecordsDB_2a.write.format("parquet").mode("overwrite").save(hdfspathTemp)
            statusRecordsDB_2a.write.format("parquet").mode("overwrite").save(hdfspathTemp)
            debug("BELOOOOOOOOOOOOOOOOOOOOOOOOONA ##### W R O T E $$$$$")
            hdfs.delete(new org.apache.hadoop.fs.Path(hdfspath),true)
            hdfs.rename(new org.apache.hadoop.fs.Path(hdfspathTemp),new org.apache.hadoop.fs.Path(hdfspath))
            hdfs.delete(new org.apache.hadoop.fs.Path(hdfspathTemp),true)
            debug("BELOOOOOOOOOOOOOOOOOOOOOOOOONA ##### D O N E $$$$$")

            //statusRecordsDB_2a.write.mode(SaveMode.Overwrite).parquet(hdfspath)

            //val partitionPopulationRecordsDB = partitionb4df.unionAll(df_)
            //val partitionPopulationRecordsDB_to_write = partitionPopulationRecordsDB.groupBy(dataSource.primarykey).max(dataSource.primarykey, dataSource.bookmark)
            //partitionPopulationRecordsDB_to_write.write.format("json").mode("overwrite").save(hdfspath)
            //hdfs.delete(new org.apache.hadoop.fs.Path(hdfspath), true)
            //partitionPopulationRecordsDB.write.mode(SaveMode.ErrorIfExists).parquet(hdfspath)
          }
          else {
            debug("STONNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNE == "+key_)
            debug("[MY DEBUG STATEMENTS] [HDFS] [DUMP] The path was not present before == " + hdfspath)
            val w = Window.partitionBy(col(dataSource.primarykey)).orderBy(col((dataSource.bookmark)).desc)
            //val w = Window.partitionBy($"\""+dataSource.primarykey+"\"").orderBy(($"\""+dataSource.bookmark+"\"").desc)
            //val w = Window.partitionBy(dataSource.primarykey).orderBy(($("\""+dataSource.bookmark+"\"").desc))

            val df_dedupe_ = df_.sort(col(dataSource.primarykey),col(dataSource.bookmark).desc).dropDuplicates(Seq(dataSource.primarykey))
            debug("WOLENNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN df_dedupe_ COUNT == "+df_dedupe_.count())
            //df_dedupe_.write.format("parquet").mode("overwrite").save(hdfspath)
            df_dedupe_.write.format("parquet").mode("overwrite").save(hdfspath)

            val partitionAddQuery = "ALTER TABLE HIVE_TABLE_"+(dataSource.host).replaceAll("[^a-zA-Z]", "")+"_"+(dataSource.db).replaceAll("[^a-zA-Z]", "")+"_"+(dataSource.table).replaceAll("[^a-zA-Z]", "")+" ADD PARTITION (partitioned_on_"+(partKey_)+"='"+(key_)+"') location '"+hdfspath+"'"
            val hiveAddPartitionStmt = hiveCon.createStatement()
            val addPartitionHiveRes = hiveAddPartitionStmt.execute(partitionAddQuery)
            debug("[HIVE QUERY] == "+partitionAddQuery)
            //hiveContext.sql(partitionAddQuery)
            //val df_dedupe_ : DataFrame = df_.withColumn("rn", row_number.over(w)).where($"rn" === 1).drop("rn")

            //val df_to_write = df_.groupBy(dataSource.primarykey).max(dataSource.primarykey, dataSource.bookmark)
            //partitionPopulationRecordsDB_to_write.write.format("json").mode("overwrite").save(hdfspath)
          }

        }
        debug("[MY DEBUG STATEMENTS] updating the current bookmark for this db + table into BOOKMARKS table . . .")
        dataSource.insertInRunLogsPassed(hash)
        dataSource.insertInRequestsPassed(hash)
        dataSource.updateBookMark(currBookMark)
      }
      //}catch {
      //case e : Exception => {
      //debug("[MY DEBUG STATEMENTS] [HDFS] [UPSERT] [PROBLEM] ")
      //debug(e.printStackTrace())
      // update run logs
      //dataSource.insertInRunLogsFailed(hash,e)
      // update request accordingly
      //dataSource.insertInRequestsFailed(hash,e)
      //}

      //}
      //        val prevBookMark = dataSource.getPrevBookMark()
      //        debug("[MY DEBUG STATEMENTS] {{"+hash+"}} the previous bookmark == "+prevBookMark)
      //        val currBookMark = dataSource.getCurrBookMark()
      //        debug("[MY DEBUG STATEMENTS] {{"+hash+"}} the current bookmark == "+currBookMark)
      //        val PKsAffectedDF: DataFrame = dataSource.getAffectedPKs(prevBookMark,currBookMark)
      //        debug("[MY DEBUG STATEMENTS] {{"+hash+"}} the count of PKs returned == "+PKsAffectedDF.count())
      //
      //        val servers = string("sinks.kafka.bootstrap.servers")
      //        val brokers = string("sinks.kafka.metadata.brokers.list")
      //        val group = string("sinks.kafka.group.id")
      //        val producertype = string("sinks.kafka.producer.type")
      //        val keyserial = string("sinks.kafka.key.serializer")
      //        val valueserial = string("sinks.kafka.value.serializer")
      //        val topicCreateEnable = string("sinks.kafka.auto.create.topics.enable")
      //
      //        if(PKsAffectedDF.rdd.isEmpty()){
      //          info("[SQL] {{"+hash+"}} no records to upsert in the internal Status Table == for bookmarks : "+prevBookMark+" ==and== "+currBookMark+" for table == "+dataSource.db+"_"+dataSource.table+" @host@ == "+dataSource.host)
      //        }
      //        def shortenTS: (String => String) = (ts: String) => {
      //          "dt="+ts.replaceAll(" ", "").substring(0, 10) // 10 digit partition size
      //        }
      //        def lowerCase: (String => String) = (colName: String) => {
      //          colName.toLowerCase() // 10 digit partition size
      //        }
      //        if(dataSource.hdfsPartitionCol.isEmpty()){
      //          val partitionColFunc = udf(shortenTS)
      //          val PKsAffectedDF_partition = PKsAffectedDF.withColumn("TSpartitionKey", partitionColFunc(col(dataSource.bookmark)))
      //          import sqlContext.implicits._
      //          val keys = PKsAffectedDF_partition.select("TSpartitionKey").distinct.collect.flatMap(_.toSeq)
      //          val byPartitionArray = keys.map(key => PKsAffectedDF_partition.where($"TSpartitionKey" <=> key))
      //          val dbname = dataSource.db
      //          val dbtable = dataSource.table
      //
      //          val PKsAffectedDF_json = PKsAffectedDF_partition.toJSON
      //
      //          PKsAffectedDF_json.foreachPartition { partitionOfRecords => {
      //            val props = new Properties()
      //
      //            props.put("bootstrap.servers", servers) // "kafka01.production.askmebazaar.com:9092,kafka02.production.askmebazaar.com:9092,kafka03.production.askmebazaar.com:9092")
      //            //props.put("metadata.broker.list", "localhost:9092")//"kafka01.production.askmebazaar.com:2181,kafka02.production.askmebazaar.com:2181,kafka03.production.askmebazaar.com:2181")
      //            props.put("group.id", group) //"ramanujan")
      //            props.put("producer.type", producertype) //"async")
      //            props.put("key.serializer", keyserial) //"org.apache.kafka.common.serialization.StringSerializer")
      //            props.put("value.serializer", valueserial) //"org.apache.kafka.common.serialization.StringSerializer")
      //            props.put("request.required.acks", "1")
      //            props.put("auto.create.topics.enable", topicCreateEnable)
      //            //props.put("block.on.buffer.full","false")
      //            //props.put("advertised.host.name","localhost")
      //
      //            val producer = new KafkaProducer[String, String](props)
      //            partitionOfRecords.foreach {
      //              case x: String => {
      //                val message = new ProducerRecord[String, String]("TOPIC_" + dbname + "_" + dbtable, dbname, x)
      //                producer.send(message).get()
      //              }
      //            }
      //            producer.close()
      //          }
      //          }
      //          for(i <- 0 to (keys.length - 1)) {
      //            val key_ = keys(i).toString()
      //            val df_ = byPartitionArray(i)
      //            // hdfs://<host>:<port>/user/home/cloudera/cm_api.py <host> is Hadoop NameNode host and the <port> port number of Hadoop NameNode, 50070
      //            // s3a://<<ACCESS_KEY>>:<<SECRET_KEY>>@<<BUCKET>>/<<FOLDER>>/<<FILE>>
      //            // http://www.infoobjects.com/different-ways-of-setting-aws-credentials-in-spark/ https://www.supergloo.com/fieldnotes/apache-spark-amazon-s3-examples-of-text-files/
      //            val hdfspath = "hdfs://" + string("sinks.hdfs.url") + ":" + string("sinks.hdfs.port") + "/" + dbname + "_" + dbtable + "/" + key_
      //            val hconf = sc.hadoopConfiguration
      //            val hdfs = org.apache.hadoop.fs.FileSystem.get(hconf)
      //            val partitionExistsBefore = hdfs.exists(new org.apache.hadoop.fs.Path(hdfspath))
      //            hconf.set("fs.default.name", "hdfs://" + string("sinks.hdfs.url") + ":" + string("sinks.hdfs.port"))
      //
      //            if (partitionExistsBefore) {
      //              try {
      //                debug("[MY DEBUG STATEMENTS] [HDFS] [DUMP] The path was present before == " + hdfspath)
      //                val partitionb4df = sqlContext.read.parquet(hdfspath).toDF((dataSource.fullTableSchema + ",TSpartitionKey").split(','): _*) //(dataSource.fullTableSchema)
      //                //val partitionb4df = sc.textFile(hdfspath+"part-r-").toDF((dataSource.fullTableSchema+",TSpartitionKey").split(',') : _*) //("hdfs://quickstart.cloudera:8020/user/cloudera/README.md")
      //
      //                val partitionPopulationRecordsDB = partitionb4df.unionAll(df_)
      //                partitionPopulationRecordsDB.write.format("json").mode("overwrite").save(hdfspath)
      //                //hdfs.delete(new org.apache.hadoop.fs.Path(hdfspath), true)
      //                //partitionPopulationRecordsDB.write.mode(SaveMode.ErrorIfExists).parquet(hdfspath)
      //              } catch {
      //                case _: Throwable => debug("[MY DEBUG STATEMENTS] [HDFS] [UPSERT] [PROBLEM] " + key_)
      //              }
      //            }
      //            else {
      //              debug("[MY DEBUG STATEMENTS] [HDFS] [DUMP] The path was present before == " + hdfspath)
      //              df_.write.format("json").mode("overwrite").save(hdfspath) //.save(hdfspath)
      //            }
      //            debug("[MY DEBUG STATEMENTS] updating the current bookmark for this db + table into BOOKMARKS table . . .")
      //            dataSource.updateBookMark(currBookMark)
      //
      //          }
      //        }
      //        else{
      //          val partitionColFunc = udf(lowerCase)
      //          val PKsAffectedDF_partition = PKsAffectedDF.withColumn("TSpartitionKey", partitionColFunc(col(dataSource.hdfsPartitionCol)))
      //          import sqlContext.implicits._
      //          val keys = PKsAffectedDF_partition.select("TSpartitionKey").distinct.collect.flatMap(_.toSeq)
      //          val byPartitionArray = keys.map(key => PKsAffectedDF_partition.where($"TSpartitionKey" <=> key))
      //          val dbname = dataSource.db
      //          val dbtable = dataSource.table
      //
      //          val PKsAffectedDF_json = PKsAffectedDF_partition.toJSON
      //
      //          PKsAffectedDF_json.foreachPartition { partitionOfRecords => {
      //            val props = new Properties()
      //
      //            props.put("bootstrap.servers", servers) // "kafka01.production.askmebazaar.com:9092,kafka02.production.askmebazaar.com:9092,kafka03.production.askmebazaar.com:9092")
      //            //props.put("metadata.broker.list", "localhost:9092")//"kafka01.production.askmebazaar.com:2181,kafka02.production.askmebazaar.com:2181,kafka03.production.askmebazaar.com:2181")
      //            props.put("group.id", "ramanujan") //"ramanujan")
      //            props.put("producer.type", "async") //"async")
      //            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer") //"org.apache.kafka.common.serialization.StringSerializer")
      //            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer") //"org.apache.kafka.common.serialization.StringSerializer")
      //            props.put("request.required.acks", "1")
      //            props.put("auto.create.topics.enable", "true")
      //            //props.put("block.on.buffer.full","false")
      //            //props.put("advertised.host.name","localhost")
      //
      //            val producer = new KafkaProducer[String, String](props)
      //            partitionOfRecords.foreach {
      //              case x: String => {
      //                val message = new ProducerRecord[String, String]("TOPIC_" + dbname + "_" + dbtable, dbname, x)
      //                producer.send(message).get()
      //              }
      //            }
      //            producer.close()
      //          }
      //          }
      //          for(i <- 0 to (keys.length - 1)) {
      //            val key_ = keys(i).toString()
      //            val df_ = byPartitionArray(i)
      //            // hdfs://<host>:<port>/user/home/cloudera/cm_api.py <host> is Hadoop NameNode host and the <port> port number of Hadoop NameNode, 50070
      //            // s3a://<<ACCESS_KEY>>:<<SECRET_KEY>>@<<BUCKET>>/<<FOLDER>>/<<FILE>>
      //            // http://www.infoobjects.com/different-ways-of-setting-aws-credentials-in-spark/ https://www.supergloo.com/fieldnotes/apache-spark-amazon-s3-examples-of-text-files/
      //            val hdfspath = "hdfs://" + string("sinks.hdfs.url") + ":" + string("sinks.hdfs.port") + "/" + dbname + "_" + dbtable + "/" + key_
      //            val hconf = sc.hadoopConfiguration
      //            val hdfs = org.apache.hadoop.fs.FileSystem.get(hconf)
      //            val partitionExistsBefore = hdfs.exists(new org.apache.hadoop.fs.Path(hdfspath))
      //            hconf.set("fs.default.name", "hdfs://" + string("sinks.hdfs.url") + ":" + string("sinks.hdfs.port"))
      //
      //            if (partitionExistsBefore) {
      //              try {
      //                debug("[MY DEBUG STATEMENTS] [HDFS] [DUMP] The path was present before == " + hdfspath)
      //                val partitionb4df = sqlContext.read.parquet(hdfspath).toDF((dataSource.fullTableSchema + ",TSpartitionKey").split(','): _*) //(dataSource.fullTableSchema)
      //                //val partitionb4df = sc.textFile(hdfspath+"part-r-").toDF((dataSource.fullTableSchema+",TSpartitionKey").split(',') : _*) //("hdfs://quickstart.cloudera:8020/user/cloudera/README.md")
      //
      //                val partitionPopulationRecordsDB = partitionb4df.unionAll(df_)
      //                partitionPopulationRecordsDB.write.format("json").mode("overwrite").save(hdfspath)
      //                //hdfs.delete(new org.apache.hadoop.fs.Path(hdfspath), true)
      //                //partitionPopulationRecordsDB.write.mode(SaveMode.ErrorIfExists).parquet(hdfspath)
      //              } catch {
      //                case _: Throwable => debug("[MY DEBUG STATEMENTS] [HDFS] [UPSERT] [PROBLEM] " + key_)
      //              }
      //            }
      //            else {
      //              debug("[MY DEBUG STATEMENTS] [HDFS] [DUMP] The path was present before == " + hdfspath)
      //              df_.write.format("json").mode("overwrite").save(hdfspath) //.save(hdfspath)
      //
      //            }
      //            debug("[MY DEBUG STATEMENTS] updating the current bookmark for this db + table into BOOKMARKS table . . .")
      //            dataSource.updateBookMark(currBookMark)
      //          }
      //        }

      //        PKsAffectedDF_json.foreachPartition { partitionOfRecords => {
      //          var props = new Properties()
      //
      //          props.put("bootstrap.servers", servers) // "kafka01.production.askmebazaar.com:9092,kafka02.production.askmebazaar.com:9092,kafka03.production.askmebazaar.com:9092")
      //          //props.put("metadata.broker.list", "localhost:9092")//"kafka01.production.askmebazaar.com:2181,kafka02.production.askmebazaar.com:2181,kafka03.production.askmebazaar.com:2181")
      //          props.put("group.id", "ramanujan") //"ramanujan")
      //          props.put("producer.type", "async") //"async")
      //          props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer") //"org.apache.kafka.common.serialization.StringSerializer")
      //          props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer") //"org.apache.kafka.common.serialization.StringSerializer")
      //          props.put("request.required.acks", "1")
      //          props.put("auto.create.topics.enable", "true")
      //          //props.put("block.on.buffer.full","false")
      //          //props.put("advertised.host.name","localhost")
      //
      //          val producer = new KafkaProducer[String, String](props)
      //          partitionOfRecords.foreach {
      //            case x: String => {
      //              val message = new ProducerRecord[String, String]("TOPIC_" + dbname + "_" + dbtable, dbname, x)
      //              producer.send(message).get()
      //            }
      //          }
      //          producer.close()
      //        }
      //        }

      //        for(i <- 0 to (keys.length - 1)) {
      //          val key_ = keys(i).toString()
      //          val df_ = byPartitionArray(i)
      //          // hdfs://<host>:<port>/user/home/cloudera/cm_api.py <host> is Hadoop NameNode host and the <port> port number of Hadoop NameNode, 50070
      //          // s3a://<<ACCESS_KEY>>:<<SECRET_KEY>>@<<BUCKET>>/<<FOLDER>>/<<FILE>>
      //          // http://www.infoobjects.com/different-ways-of-setting-aws-credentials-in-spark/ https://www.supergloo.com/fieldnotes/apache-spark-amazon-s3-examples-of-text-files/
      //          val hdfspath = "hdfs://" + string("sinks.hdfs.url") + ":" + string("sinks.hdfs.port") + "/" + dbname + "_" + dbtable + "/" + key_
      //          val hconf = sc.hadoopConfiguration
      //          val hdfs = org.apache.hadoop.fs.FileSystem.get(hconf)
      //          val partitionExistsBefore = hdfs.exists(new org.apache.hadoop.fs.Path(hdfspath))
      //          hconf.set("fs.default.name", "hdfs://" + string("sinks.hdfs.url") + ":" + string("sinks.hdfs.port"))
      //
      //          if (partitionExistsBefore) {
      //            try {
      //              debug("[MY DEBUG STATEMENTS] [HDFS] [DUMP] The path was present before == " + hdfspath)
      //              val partitionb4df = sqlContext.read.parquet(hdfspath).toDF((dataSource.fullTableSchema + ",TSpartitionKey").split(','): _*) //(dataSource.fullTableSchema)
      //              //val partitionb4df = sc.textFile(hdfspath+"part-r-").toDF((dataSource.fullTableSchema+",TSpartitionKey").split(',') : _*) //("hdfs://quickstart.cloudera:8020/user/cloudera/README.md")
      //
      //              val partitionPopulationRecordsDB = partitionb4df.unionAll(df_)
      //              partitionPopulationRecordsDB.write.format("json").mode("overwrite").save(hdfspath)
      //              //hdfs.delete(new org.apache.hadoop.fs.Path(hdfspath), true)
      //              //partitionPopulationRecordsDB.write.mode(SaveMode.ErrorIfExists).parquet(hdfspath)
      //            } catch {
      //              case _: Throwable => debug("[MY DEBUG STATEMENTS] [HDFS] [UPSERT] [PROBLEM] " + key_)
      //            }
      //          }
      //          else {
      //            debug("[MY DEBUG STATEMENTS] [HDFS] [DUMP] The path was present before == " + hdfspath)
      //            df_.write.format("json").mode("overwrite").save(hdfspath) //.save(hdfspath)
      //          }
      //          debug("[MY DEBUG STATEMENTS] updating the current bookmark for this db + table into BOOKMARKS table . . .")
      //          dataSource.updateBookMark(currBookMark)
      //
      //
      //        }

      //}(ExecutionContext.Implicits.global) onSuccess {
      //case _ => {
      //listener ! new WorkerDone(dataSource)
      //}

      //}
      //listener ! new WorkerExecuting(dataSource)
    }
  }
}
