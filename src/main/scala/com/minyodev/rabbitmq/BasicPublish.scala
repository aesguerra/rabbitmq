package com.minyodev.rabbitmq

import java.util.UUID
import java.util.concurrent.ArrayBlockingQueue

import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{Channel, ConnectionFactory}
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import scala.util.Random

object BasicPublish {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load()

    // create server connection
    val factory = new ConnectionFactory()
    factory.setHost("localhost")
    factory.setUsername("guest")
    factory.setPassword("guest")

    val connection = factory.newConnection()
    val channel = connection.createChannel()

    // create channel
    val replyQueueName = channel.queueDeclare().getQueue

    while(true) {
      val corrId = UUID.randomUUID().toString
      val props = new BasicProperties().builder().correlationId(UUID.randomUUID().toString).replyTo(replyQueueName).build()

      basicPublish("foo", channel)

      val response = new ArrayBlockingQueue[String](1)

      import com.rabbitmq.client.AMQP
      import com.rabbitmq.client.DefaultConsumer
      import java.io.IOException

      channel.basicConsume(replyQueueName, true, new DefaultConsumer(channel) {
        @throws[IOException]
        def handleDelivery(consumerTag: String, envelope: Nothing, properties: AMQP.BasicProperties, body: Array[Byte]): Unit = {
          if (properties.getCorrelationId == corrId) response.offer(new String(body, "UTF-8"))
        }
      })

      Thread.sleep(100000L)
    }

    // close channel and server connection
    channel.close()
    connection.close()
  }

  def basicPublish(queueName: String, channel: Channel): Unit = {

    val props = new BasicProperties().builder().correlationId(UUID.randomUUID().toString).build()
    val message = "MESSAGE" + Random.nextInt(1000) + "" + Random.nextInt(1000)
    channel.basicPublish("", "foo", props, message.getBytes())

    println("[x] Sent '" + message + "'")
  }
}
