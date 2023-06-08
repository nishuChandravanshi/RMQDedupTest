package com.amqp.basic.queue;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

import static com.amqp.basic.queue.CommonConfigs.DELIMITER;
import static com.amqp.basic.queue.MessageSubscriber.*;


public class MyDeliveryCallback extends DefaultConsumer {

	Connection connection;


	/**
	 * Constructs a new instance and records its association to the passed-in channel.
	 *
	 * @param channel
	 * 		the channel to which this consumer is attached
	 */
	public MyDeliveryCallback (Channel channel) {
		super(channel);
	}


	@Override
	public void handleDelivery (String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
		executorService.submit(() -> {
			System.out.println(Thread.currentThread().getName());
			try {
				dedupMessage(envelope, body, getChannel());
			} catch (Exception e) {
				System.out.println(e);
				nackDelivery(envelope);
				throw new RuntimeException(e);
			}

		});
	}


	private static void dedupMessage (Envelope envelope, byte[] body, Channel channel) throws IOException, TimeoutException, InterruptedException {
		String message         = new String(body, StandardCharsets.UTF_8);
		int    channelHashCode = channel.isOpen() ? channel.hashCode() : -1;
		System.out.println("channel1 | message " + message + ", delTag " + envelope.getDeliveryTag());

		String[] msgs          = message.split(DELIMITER);
		String   msgIdentifier = msgs[1];
		if ("true".equals(redisService.getKey(msgIdentifier))) {
			System.out.println("Message Already Processed");
		} else {
			//ProcessMsg
			System.out.println("Processing Received message: " + message);
		}
		if (channel != null && channel.isOpen()) {
			System.out.println("channel open | ack msg");
			channel.basicAck(envelope.getDeliveryTag(), false);
		} else {
			System.out.println("channel closed | adding msg to reids");
			redisService.setKeyWithExpiry(msgIdentifier, "true", 300L);
		}
	}


	private static Channel closeAndGeNewChannel (Channel channel, Connection connection) throws IOException, TimeoutException {
		System.out.println("closing channel before ack | " + channel);
		channel.close();
		System.out.println("Creating New Channel");
		channel = connection.createChannel();
		return channel;
	}


	private void nackDelivery (Envelope envelope) {
		try {
			getChannel().basicNack(envelope.getDeliveryTag(), false, true);
		} catch (IOException ex) {
			System.out.println("Exception Occurred while Nack");
		}
	}
}
