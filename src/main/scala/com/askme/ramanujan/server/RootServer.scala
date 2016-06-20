package com.askme.ramanujan.server

import java.util

import akka.actor.{ActorSystem, Props}
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import com.askme.ramanujan.Configurable
import com.askme.ramanujan.handler.RamanujanHandler
import com.askme.ramanujan.util.LazyMap
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import kafka.consumer.{ConsumerConfig, KafkaStream, TopicFilter, Whitelist}
import kafka.javaapi.consumer.SimpleConsumer
import kafka.javaapi.producer.Producer
import kafka.producer.ProducerConfig
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.kafka.clients.producer.KafkaProducer
import kafka.serializer.StringDecoder
import kafka.admin.AdminUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import spray.can.Http

import scala.collection.JavaConversions._
import scala.collection.convert.decorateAsScala._
import scala.concurrent.duration.DurationInt
import scala.concurrent.ExecutionContext.Implicits.global

object RootServer extends Logging {
  class AppContext private[RootServer](val config: Config) extends Configurable {

    private val conf = sparkConf("spark")

    val rddCache = new LazyMap[RDD[_]](1000)

    val sparkContext = new SparkContext(conf)
    val ssc = new StreamingContext(sparkContext, Seconds(2))

    var offsetRanges = Array[OffsetRange]()

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder] (
      ssc,
      Map[String, String](
        "metadata.broker.list" -> string("handler.stream.order-pipeline.source.brokers"),
        "auto.offset.reset" -> "smallest"
      ),
      Set[String](string("handler.stream.order-pipeline.source.topic"))
    ).transform{ rdd=>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.map(msg=>msg._1+": "+msg._2)

    messages.foreachRDD(rdd=>
      rdd.collect().foreach{ m=>
        info("HELLO! "+m)
      }
    )
    messages.foreachRDD(rdd=>
      for(o<-offsetRanges)
        info("YELLO! "+s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
    )


    ssc.start()
    ssc.awaitTermination()
 
    private[RootServer] def close() {
      ssc.stop(false, true)
      sparkContext.stop()
    }
  }

}

class RootServer(val config: Config) extends Server with Logging {
  private implicit val system = ActorSystem(string("actorSystem.name"), conf("actorSystem"))
  private val context = new RootServer.AppContext(config)
  private val topActor = system.actorOf(Props(classOf[RamanujanHandler], conf("handler"), context), name = string("handler.name"))

  private implicit val timeout = Timeout(int("timeout").seconds)
  private val transport = IO(Http)


  override def bind {
    transport ! Http.Bind(topActor, interface = string("host"), port = int("port"))
    info("server bound: " + string("host") + ":" + int("port"))
  }

  override def close() {
    transport ? Http.Unbind
    context.close()
    system.stop(topActor)
    system.shutdown()
    info("server shutdown complete: " + string("host") + ":" + int("port"))
  }

}
