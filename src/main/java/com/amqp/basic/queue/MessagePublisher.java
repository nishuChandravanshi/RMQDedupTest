package com.amqp.basic.queue;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.UUID;

import static com.amqp.basic.queue.CommonConfigs.DELIMITER;

public class MessagePublisher {
	public static void main (String[] args) throws Exception {
		ConnectionFactory factory    = new ConnectionFactory();
		Connection        connection = factory.newConnection(CommonConfigs.AMQP_URL);
		Channel           channel    = connection.createChannel();
		channel.queueDeclare(CommonConfigs.DEFAULT_QUEUE, true, false, false, null);

		publishMessageInRange(channel);
//		publishMessageInLoop(channel);

		channel.close();
		connection.close();
	}


	private static void publishMessageInRange (Channel channel) throws IOException {
		for (int i = 0; i < 15; i++) {
			String uuid    = UUID.randomUUID().toString();
			String message = "Msg" + i + DELIMITER + uuid;
//			String message1 = "Msg" + i;

			//publish - (exchange, routingKey, properties, message)
			channel.basicPublish("", CommonConfigs.DEFAULT_QUEUE, null, message.getBytes());
		}
	}


	private static void publishMessageInLoop (Channel channel) throws IOException {
		int i = 0;
		while (true) {
			String uuid    = UUID.randomUUID().toString();
			String message = "Msg" + i++ + DELIMITER + uuid;

			//publish - (exchange, routingKey, properties, message)
			channel.basicPublish("", CommonConfigs.DEFAULT_QUEUE, null, message.getBytes());
		}
	}
}
