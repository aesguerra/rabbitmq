package com.minyodev.rabbitmq.rpc

import com.rabbitmq.client.{AMQP, ConnectionFactory, DefaultConsumer, Envelope}

object RPCServerRejectNoRequeue {
  def main(args: Array[String]): Unit = {
    val factory = new ConnectionFactory()
    factory.setHost("localhost")
    factory.setUsername("guest")
    factory.setPassword("guest")

    val uiQueueName = "rpc_queue"
    val connection = factory.newConnection()
    val channel = connection.createChannel()

    channel.basicQos(1)
    channel.queueDeclare(uiQueueName, false, false, false, null)
    println("[*] Waiting for message. To exit press Ctrl + C")

    val consumer = new DefaultConsumer(channel) {
      override def handleDelivery(consumerTag: String, envelope: Envelope, props: AMQP.BasicProperties, body: Array[Byte]): Unit = {
        try {
          val message = new String(body, "UTF-8")
          println("---------------------------------------------")
          println("[x] Received '" + message + "' to be rejected and not requeue")
          println("[x] Reply to '" + props.getReplyTo + "'")
          channel.basicPublish("", props.getReplyTo(), props, body)
          channel.basicReject(envelope.getDeliveryTag, false)
          // channel.basicAck(envelope.getDeliveryTag, true)
          println("[x] Sent!")
          println("---------------------------------------------")
          Thread.sleep(1000L)
        }catch {
          case e: Exception => println("???: " + e.getMessage)
        }
      }
    }

    channel.basicConsume(uiQueueName, false, consumer)
  }
}
