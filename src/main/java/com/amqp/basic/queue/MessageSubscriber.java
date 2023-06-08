package com.amqp.basic.queue;

import com.amqp.redis.RedisService;
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
	static Connection      connection;
	static Channel         channel;
	static Channel         channel1;

	static boolean      channelClosed = false;
	static RedisService redisService  = new RedisService();


	public static void main (String[] args) throws IOException, TimeoutException {
		createNewConnection();
		channel = connection.createChannel();

//		connection.addShutdownListener((e) -> {
//			System.out.println("Shutting down RMQ Connection");
//			System.out.println(e.getMessage() + e);
//			//Not Working for here ie creating new connection
////			createNewConnection();
//		});

//		channel.addShutdownListener((e) -> {
//			System.out.println("Shutting down RMQ Channel");
//			System.out.println(e.getMessage() + e);
//			//Not Working
//			createNewChannel();
//		});


		channel.basicQos(5);
		channel.basicConsume(CommonConfigs.DEFAULT_QUEUE, false, new DeliveryCallbackChannelTesting(channel, connection));
//		channel.basicConsume(CommonConfigs.DEFAULT_QUEUE, false, new MyDeliveryCallbackWithTimeOut(channel, connection));
//		channel1.basicConsume(CommonConfigs.DEFAULT_QUEUE, false, new MyDeliveryCallback(channel1));
	}


	private static void createNewConnection () {
		try {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setAutomaticRecoveryEnabled(true);
//			factory.setConnectionTimeout(10000); //tcp connection timeout | timeout for establishing rmq connection
			connection = factory.newConnection(CommonConfigs.AMQP_URL);
			System.out.println("CreateConnLog1 | Connection Name, id " + connection.getClientProvidedName() + ", " + connection.getId());
		} catch (IOException | TimeoutException ex) {
			System.out.println("CreateConnEx1 | Establishing New Connection Exception " + ex);
		}
	}


	static void createNewChannel () {
		try {
			if (connection != null && !connection.isOpen()) {
				System.out.println("CreateChLog1| Establishing new RMQ Connection");
				createNewConnection();
			}
			if (channel != null && !channel.isOpen()) {
				System.out.println("CreateChLog2 | Establishing New RMQ Channel");
				channel = connection.createChannel();
				System.out.println("CreateChLog3 | start consuming on new channel");
				channel.basicConsume(CommonConfigs.DEFAULT_QUEUE, false, new DeliveryCallbackChannelTesting(channel, connection));
				System.out.println("CreateChLog4 | Channel Created " + channel.isOpen() + " " + channel.hashCode());
			} else {
				System.out.println("CreateChLog4 | Could not create new channel as channel Null or Open");
			}
		} catch (IOException ex) {
			System.out.println("CreateChEx1 | Establishing New Channel Exception " + ex);
		} catch (Exception ex) {
			System.out.println("CreateChEx2 | Exception Occurred while creating new channel " + ex);
		}

	}

}
