package com.amqp.basic.queue;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import static com.amqp.basic.queue.CommonConfigs.DELIMITER;
import static com.amqp.basic.queue.MessageSubscriber.*;

public class MyDeliveryCallbackWithTimeOut extends DefaultConsumer {

	Connection connection;
	static boolean closeChannel = true;


	public static void main (String[] args) throws InterruptedException {
		CountDownLatch countDownLatch = new CountDownLatch(1);

		System.out.println("hi");
		countDownLatch.countDown();
		countDownLatch.countDown();

		System.out.println("hi1");
	}
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
				dedupMessage(envelope, body, getChannel());
			} catch (Exception e) {
				System.out.println(e);
				nackDelivery(envelope);
				throw new RuntimeException(e);
			}

		});
	}


//	@Override
//	public void handleShutdownSignal (String consumerTag, ShutdownSignalException sig) {
//		System.out.println("Inside handleShutdownSignal | consumerTag => " + consumerTag + " with error => " + sig);
//		createNewChannel();
//	}


	private void createNewChannel () {
		try {
			if (this.connection != null && !connection.isOpen()) {
				System.out.println("close exisitign connection");
				connection.close();
				System.out.println("nish Establishing new RMQ Connection");
				createNewConnection();
			}
			if (channel != null && !channel.isOpen()) {
				System.out.println("Nish Establishing New RMQ Channel");
				channel = connection.createChannel();
				channel.basicConsume(CommonConfigs.DEFAULT_QUEUE, false, new MyDeliveryCallbackWithTimeOut(channel, connection));
			} else {
				throw new Exception("Could not create new channel as connection Null or Not Open");
			}
		} catch (IOException ex) {
			System.out.println("Establishing New Channel Exception " + ex);
			throw new RuntimeException(ex);
		} catch (Exception ex) {
			System.out.println("Exception Occurred while creating new channel " + ex);
			throw new RuntimeException(ex);
		}

	}


	private void createNewConnection () {
		try {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setAutomaticRecoveryEnabled(false);
//			factory.setConnectionTimeout(10000); //tcp connection timeout | timeout for establishing rmq connection
			connection = factory.newConnection(CommonConfigs.AMQP_URL);

		} catch (IOException | TimeoutException ex) {
			System.out.println("Establishing New Connection Exception " + ex);
			System.out.println(ex);
		}
	}


	private static void dedupMessage (Envelope envelope, byte[] body, Channel channel) throws
																					   IOException, TimeoutException, InterruptedException {
		String message = new String(body, StandardCharsets.UTF_8);
		System.out.println("channel1 | message " + message + ", delTag " + envelope.getDeliveryTag());

		String[] msgs          = message.split(DELIMITER);
		String   msgIdentifier = msgs[1];
		if ("true".equals(redisService.getKey(msgIdentifier))) {
			System.out.println("Message Already Processed");
		} else {
			//ProcessMsg
			System.out.println("Processing Received message: " + message);
		}

//		closeChannelForTesting(channel);

		if (channel != null && channel.isOpen()) {
			System.out.println("channel open | ack msg | channelHashCode : " + channel.hashCode());
			ackDelivery(envelope, channel);
		} else {
			System.out.println("channel closed | adding msg to redis | channelHashCode : " + (channel != null ? channel.hashCode() : null));
			redisService.setKeyWithExpiry(msgIdentifier, "true", 86400L);
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
			Thread.sleep(THREAD_SLEEP);
			channel.basicAck(envelope.getDeliveryTag(), false);
		} catch (Exception e) {
			System.out.println("exception while ack " + e);
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
