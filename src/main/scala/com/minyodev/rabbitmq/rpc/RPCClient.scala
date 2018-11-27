package com.minyodev.rabbitmq.rpc

import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.Connection
import com.rabbitmq.client.Channel
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Envelope
import java.io.IOException
import java.util.UUID
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.TimeoutException

import com.rabbitmq.client.AMQP.BasicProperties

import scala.util.Random

object RPCClient {

  val requestQueueName = "rpc_queue"

  def main(args: Array[String]): Unit = {
    val factory = new ConnectionFactory()
    factory.setHost("localhost")

    val connection = factory.newConnection()
    val channel = connection.createChannel()

    val replyQueueName = channel.queueDeclare().getQueue()

    val corrId = UUID.randomUUID().toString()

    val props = new AMQP.BasicProperties
      .Builder()
      .correlationId(corrId)
      .replyTo(replyQueueName)
      .build()

    val response = new ArrayBlockingQueue[String](1)

    channel.basicConsume(replyQueueName, true, new DefaultConsumer(channel) {
      @throws[IOException]
      override def handleDelivery(consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]): Unit = {
        if(properties.getCorrelationId.equals(corrId)) {
          println(">> " + new String(body, "UTF-8"))
          response.offer(new String(body, "UTF-8"))
        }
      }
    })

    while(true) {
      val message = UUID.randomUUID().toString
      println("sending message: " + message)
      channel.basicPublish("", requestQueueName, props, message.getBytes("UTF-8"))

      val message2 = UUID.randomUUID().toString
      println("sending message: " + message2)
      channel.basicPublish("", requestQueueName, props, message2.getBytes("UTF-8"))

      Thread.sleep(10000)
    }

    response.take()
    channel.close()
  }
}
