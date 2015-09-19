package com.askme.ramanujan.server

import java.util

import akka.actor.{ActorSystem, Props}
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import com.askme.ramanujan.Configurable
import com.askme.ramanujan.handler.RamanujanHandler
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import kafka.consumer.{KafkaStream, Whitelist, TopicFilter, ConsumerConfig}
import kafka.javaapi.consumer.SimpleConsumer
import kafka.javaapi.producer.Producer
import kafka.producer.ProducerConfig
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.{SparkContext, SparkConf}
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.client.Client
import org.elasticsearch.common.logging.ESLoggerFactory
import org.elasticsearch.common.logging.slf4j.Slf4jESLoggerFactory
import org.elasticsearch.node.NodeBuilder
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.search.aggregations.AggregationBuilders._
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.apache.kafka.clients.producer.KafkaProducer
import kafka.serializer.StringDecoder
import kafka.admin.AdminUtils

import spray.can.Http
import scala.collection.JavaConversions._
import scala.collection.convert.decorateAsScala._

import scala.concurrent.duration.DurationInt

object RootServer extends Logging {
  class AppContext private[RootServer](val config: Config) extends Configurable {

    private val conf = sparkConf("spark")

    val sparkContext = new SparkContext(conf)
 
    private[RootServer] def close() {
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
