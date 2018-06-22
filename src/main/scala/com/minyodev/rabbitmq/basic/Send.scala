package com.minyodev.rabbitmq.basic

import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory

object Send {

  private val QUEUE_NAME = "banana"

  @throws[Exception]
  def main(argv: Array[String]): Unit = {
    val factory = new ConnectionFactory()
    factory.setHost("localhost")
    factory.setUsername("guest")
    factory.setPassword("guest")

    val connection = factory.newConnection
    val channel = connection.createChannel

    channel.queueDeclare(QUEUE_NAME, false, false, false, null)
    val message = "Hello World!"

    channel.basicPublish("", QUEUE_NAME, null, message.getBytes("UTF-8"))

    println(" [x] Sent '" + message + "'")

    Thread.sleep(20000)

    channel.close
    connection.close
  }
}
