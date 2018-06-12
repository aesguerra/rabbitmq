package com.minyodev.rabbitmq

import com.rabbitmq.client._
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.math.random
import scala.util.Random

object Get2 {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val factory = new ConnectionFactory()
    factory.setHost("localhost")
    factory.setUsername("guest")
    factory.setPassword("guest")

    val uiQueueName = "foo"
    val connection = factory.newConnection()
    val channel = connection.createChannel()

    channel.basicQos(1)

    channel.queueDeclare(uiQueueName, false, false, false, null)
    LOG.info("[*] Waiting for message. To exit press Ctrl + C")

    var ctr = 0
    channel.basicQos(1); // accept only one unack-ed message at a time (see below)

    while(true) {
      val a = channel.basicGet(uiQueueName, false)
      if(a == null) {
        println("No message received")
      }
      else {
        val message = new String(a.getBody, "UTF-8")
        LOG.info("---------------------------------------------")
        LOG.info("[x] Received '" + message + "'")
        LOG.info("[x] Reply to '" + a.getProps.getReplyTo + "'")
        channel.basicPublish("", a.getProps.getReplyTo(), a.getProps, a.getBody)
        channel.basicAck(a.getEnvelope.getDeliveryTag, true)
        LOG.info("[x] Sent!")
        LOG.info("---------------------------------------------")
        LOG.info("finished consuming...")
      }
      Thread.sleep(1000L)
    }
  }
}
