package com.amqp.redis;

import com.amqp.basic.queue.MyDeliveryCallback;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

public class RedisService {
	Jedis jedis = new Jedis("localhost");


	public static void main (String[] args) {
		System.out.println(MyDeliveryCallback.class.getSimpleName());
	}
	public String setKeyWithExpiry (String key, String value, Long expiryTimeInSeconds) {
		String statusCode = null;

		try {
			statusCode = jedis.set(key, value, new SetParams().ex(expiryTimeInSeconds));
		} catch (Exception e) {
			System.out.println(e.getMessage() + e);
		}

		return statusCode;
	}


	public String getKey (String key) {
		String value = null;
		try {
			value = jedis.get(key);
		} catch (Exception e) {
			System.out.println(e.getMessage() + e);
		}

		return value;
	}
}

