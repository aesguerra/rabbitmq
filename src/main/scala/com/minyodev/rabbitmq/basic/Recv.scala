package com.minyodev.rabbitmq.basic

import com.rabbitmq.client.{ConnectionFactory, Envelope}

object Recv {

  import com.rabbitmq.client.AMQP
  import com.rabbitmq.client.Consumer
  import com.rabbitmq.client.DefaultConsumer
  import java.io.IOException

  private val QUEUE_NAME = "banana"

  def main(argv: Array[String]): Unit = {
    val factory = new ConnectionFactory()
    factory.setHost("localhost")
    factory.setUsername("guest")
    factory.setPassword("guest")

    val connection = factory.newConnection()
    val channel = connection.createChannel

    channel.queueDeclare(QUEUE_NAME, false, false, false, null)

    println(" [*] Waiting for messages. To exit press CTRL+C")

    val consumer = new DefaultConsumer(channel) {
      @throws[IOException]
      override def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: Array[Byte]): Unit = {
        val message = new String(body, "UTF-8")
        println(" [x] Received '" + message + "'")
      }
    }
    channel.basicConsume(QUEUE_NAME, true, consumer)
  }
}
