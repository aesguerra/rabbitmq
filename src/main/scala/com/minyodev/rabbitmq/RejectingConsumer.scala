package com.minyodev.rabbitmq

import com.minyodev.rabbitmq.Consumer1.LOG
import com.rabbitmq.client._
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.math.random
import scala.util.Random

object RejectingConsumer {

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

    channel.queueDeclare(uiQueueName, false, false, false, null)
    LOG.info("[*] Waiting for message. To exit press Ctrl + C")

    val consumer = new DefaultConsumer(channel) {
      override def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: Array[Byte]): Unit = {
        val message = new String(body, "UTF-8")
        LOG.info("---------------------------------------------")
        LOG.info("[x] Received '" + message + "'")
        basicConsume(message, envelope.getDeliveryTag, channel)
        LOG.info("[x] Rejected '" + message + "'")
        LOG.info("---------------------------------------------")
      }
    }

    channel.basicConsume(uiQueueName, false, consumer)
  }

  def basicConsume(message: String, deliveryTag: Long, channel: Channel): Unit = {
    LOG.info("MR FOO GOT SOME EXCEPTIONS FOR " + message)
    channel.basicReject(deliveryTag, true)
  }
}
