package com.minyodev.rabbitmq

import com.rabbitmq.client._
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.math.random
import scala.util.Random

object Consumer2 {

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

    val consumer = new DefaultConsumer(channel) {
      override def handleDelivery(consumerTag: String, envelope: Envelope, props: AMQP.BasicProperties, body: Array[Byte]): Unit = {
        //
        val message = new String(body, "UTF-8")
        LOG.info("---------------------------------------------")
        LOG.info("[x] Received '" + message + "'")
        LOG.info("[x] Reply to '" + props.getReplyTo + "'")
        channel.basicPublish("", props.getReplyTo(), props, body)
        channel.basicAck(envelope.getDeliveryTag, true)
        LOG.info("[x] Sent!")
        LOG.info("---------------------------------------------")
        Thread.sleep(5000L)
        //
      }
    }

    channel.basicConsume(uiQueueName, false, consumer)
  }
}
