package com.amqp.basic.queue;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

import static com.amqp.basic.queue.CommonConfigs.DELIMITER;
import static com.amqp.basic.queue.MessageSubscriber.*;

public class MyDeliveryCallbackWithTimeOut extends DefaultConsumer {

	Connection connection;


	/**
	 * Constructs a new instance and records its association to the passed-in channel.
	 *
	 * @param channel
	 * 		the channel to which this consumer is attached
	 */
	public MyDeliveryCallbackWithTimeOut (Channel channel, Connection connection) {
		super(channel);
		this.connection = connection;
	}


	@Override
	public void handleDelivery (String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
		executorService.submit(() -> {
			System.out.println(Thread.currentThread().getName());
			try {
				dedupMessage(envelope, body, getChannel(), connection);
			} catch (Exception e) {
				System.out.println(e);
				nackDelivery(envelope);
				throw new RuntimeException(e);
			}

		});
	}


	private static void dedupMessage (Envelope envelope, byte[] body, Channel channel, Connection connection) throws IOException, TimeoutException, InterruptedException {
		String message = new String(body, StandardCharsets.UTF_8);
		System.out.println("channel | message " + message + ", delTag " + envelope.getDeliveryTag());

		String[] msgs = message.split(DELIMITER);
		System.out.println("set " + set.size());

		if (msgs.length > 1 && set.contains(msgs[1])) {
			System.out.println("Message Already Processed");
			channel.basicAck(envelope.getDeliveryTag(), false);
		} else {
			//ProcessMsg
			System.out.println("Processing Received message: " + message);
			set.add(msgs[1]);
//				System.out.println("Closing channel before ack");
//				connection.close();
//				channel.close();
//				channel = closeAndGeNewChannel(channel, connection);
			Thread.sleep(THREAD_SLEEP);
			channel.basicAck(envelope.getDeliveryTag(), false);
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
