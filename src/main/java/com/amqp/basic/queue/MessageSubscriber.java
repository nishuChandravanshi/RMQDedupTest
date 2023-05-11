package com.amqp.basic.queue;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

public class MessageSubscriber {
	static int             numThreads      = 1;
	static int             THREAD_SLEEP    = 30010;
	static ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
	static Set <String>    set             = new HashSet <>();
	static Connection      connection;
	static Channel         channel;
	static Channel         channel1;


	public static void main (String[] args) throws IOException, TimeoutException {
		createNewConnection();
		channel  = connection.createChannel();
//		channel1 = connection.createChannel();

		connection.addShutdownListener((e) -> {
			System.out.println("Shutting down RMQ Connection");
			System.out.println(e.getMessage() + e);
			//Not Working for here ie creating new connection
			createNewConnection();
		});

		channel.addShutdownListener((e) -> {
			System.out.println("Shutting down RMQ Channel");
			System.out.println(e.getMessage() + e);
			//Not Working
			channel = createNewChannel();
		});

//		channel1.addShutdownListener((e) -> {
//			System.out.println("Shutting down RMQ Channel1");
//			System.out.println(e.getMessage() + e);
//			//Not Working
//			channel1 = createNewChannel();
//		});
		channel.basicQos(5);
		channel.basicConsume(CommonConfigs.DEFAULT_QUEUE, false, new MyDeliveryCallbackWithTimeOut(channel, connection));
//		channel1.basicConsume(CommonConfigs.DEFAULT_QUEUE, false, new MyDeliveryCallback(channel1));
	}


	private static void createNewConnection () {
		try {
			ConnectionFactory factory = new ConnectionFactory();
//			factory.setConnectionTimeout(10000); //tcp connection timeout | timeout for establishing rmq connection
			connection = factory.newConnection(CommonConfigs.AMQP_URL);

		} catch (IOException | TimeoutException ex) {
			System.out.println("Establishing New Connection Exception " + ex);
			System.out.println(ex);
		}
	}


	private static Channel createNewChannel () {
		try {
			if (connection != null && connection.isOpen()) {
				System.out.println("Establishing new Channel");
				return connection.createChannel();
			} else {
				throw new Exception("Could not create new channel as connection Null or Not Open");
			}
		} catch (IOException ex) {
			System.out.println("Establishing New Channel Exception " + ex);
			throw new RuntimeException(ex);
		} catch (Exception ex) {
			System.out.println(ex);
			throw new RuntimeException(ex);
		}
	}

}
