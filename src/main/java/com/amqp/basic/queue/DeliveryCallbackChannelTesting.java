package com.amqp.basic.queue;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

import static com.amqp.basic.queue.CommonConfigs.DELIMITER;
import static com.amqp.basic.queue.MessageSubscriber.*;

public class DeliveryCallbackChannelTesting extends DefaultConsumer {

	Connection connection;
	static boolean closeChannel = true;


	public DeliveryCallbackChannelTesting (Channel channel, Connection connection) {
		super(channel);
		this.connection = connection;
	}


	@Override
	public void handleDelivery (String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
		executorService.submit(() -> {
			System.out.println(Thread.currentThread().getName());
			try {
				processMessage(envelope, body, getChannel());
			} catch (Exception e) {
				System.out.println(e);
				nackDelivery(envelope);
				throw new RuntimeException(e);
			}

		});
	}


	@Override
	public void handleShutdownSignal (String consumerTag, ShutdownSignalException sig) {
		System.out.println("Inside handleShutdownSignal | consumerTag => " + consumerTag + " with error => " + sig);
		channelClosed = true;
		createNewChannel();
	}


	private void processMessage (Envelope envelope, byte[] body, Channel channel) {
		String message = new String(body, StandardCharsets.UTF_8);
		System.out.println("processMsgLog1 | message " + message + ", delTag " + envelope.getDeliveryTag() + ", channelHashCode " + channel.hashCode());

//		testing purpose simulating channel close in testing
		if (!channelClosed) {
			try {
				Thread.sleep(THREAD_SLEEP);
			} catch (InterruptedException e) {
				System.out.println("SleepExcep : " + e);
			}
		}

		if (channel != null && channel.isOpen()) {
			System.out.println("channel open | ack msg | channelHashCode : " + channel.hashCode());
			ackDelivery(envelope, channel);
		} else {
			System.out.println("channel closed | channelHashCode : " + (channel != null ? channel.hashCode() : null));
		}
	}


	private static void closeChannelForTesting (Channel channel) throws IOException {
		if (closeChannel) {
			closeChannel(channel);
			closeChannel = false;
		}
	}


	private static void closeChannel (Channel channel) throws IOException {
		try {
			channel.close();
		} catch (TimeoutException e) {
			System.out.println("Exception while closing channel " + e);
		}
	}


	private static void ackDelivery (Envelope envelope, Channel channel) {
		try {
			channel.basicAck(envelope.getDeliveryTag(), false);
		} catch (Exception e) {
			System.out.println("AckDelEx1 |" + e);
		}
	}


	private void nackDelivery (Envelope envelope) {
		try {
			getChannel().basicNack(envelope.getDeliveryTag(), false, true);
		} catch (IOException ex) {
			System.out.println("Exception Occurred while Nack");
		}
	}
}
