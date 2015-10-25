package net.zsy.indexsubmitter.mq.accepter;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.StringUtils;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.rabbitmq.client.ShutdownSignalException;

import net.zsy.indexsubmitter.mq.Accepter;
import net.zsy.indexsubmitter.mq.MessageQueue;

public class RabbitMQAccepter implements Accepter, Runnable {

	private Map<String, String> configurations;

	private ConnectionFactory connectionFactory;

	private Boolean inited = false;
	private Boolean started = false;

	private BlockingQueue<String> messages = new LinkedBlockingQueue<String>();

	private String queuename;

	private ExecutorService exec;

	@Override
	public void setConfiguration(Map<String, String> configurations) {
		this.configurations = configurations;
	}

	@Override
	public void init() {
		if (!inited) {
			synchronized (inited) {
				if (!inited) {
					String hostport = configurations.get(MessageQueue.HOSTPORT);
					if (StringUtils.isBlank(hostport)) {
						throw new RuntimeException("hostport not configured.");
					}
					String[] hp = hostport.split(":");
					String username = configurations.get(MessageQueue.USERNAME);
					String password = configurations.get(MessageQueue.PASSWORD);
					queuename = configurations.get(MessageQueue.QUEUENAME);

					connectionFactory = new ConnectionFactory();
					connectionFactory.setHost(hp[0]);
					connectionFactory.setPort(Integer.valueOf(hp[1]));
					connectionFactory.setUsername(username);
					connectionFactory.setPassword(password);

					exec = Executors.newSingleThreadExecutor();

					inited = true;
				}
			}
		}

	}

	@Override
	public synchronized void start() {
		if (inited && !started) {
			exec.submit(this);
			started = true;
		}
	}

	@Override
	public synchronized void stop() {
		if (started && !exec.isShutdown()) {
			exec.shutdown();
		}
	}

	@Override
	public String accept() {
		if (started) {
			try {
				return messages.take();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	@Override
	public void run() {
		Connection connection = null;
		Channel channel = null;
		try {
			connection = connectionFactory.newConnection();
			channel = connection.createChannel();
			channel.queueDeclare(queuename, false, false, false, null);

			QueueingConsumer consumer = new QueueingConsumer(channel);
			channel.basicConsume(queuename, true, consumer);

			while (!Thread.currentThread().isInterrupted()) {
				Delivery delivery = consumer.nextDelivery();
				String message = new String(delivery.getBody());
				while (messages.size() >= 1000) {
					TimeUnit.SECONDS.sleep(3);
				}
				messages.put(message);
			}
			Thread.currentThread().interrupt();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (TimeoutException e) {
			e.printStackTrace();
		} catch (ShutdownSignalException e) {
			e.printStackTrace();
		} catch (ConsumerCancelledException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			if (channel != null && channel.isOpen()) {
				try {
					channel.close();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (TimeoutException e) {
					e.printStackTrace();
				}
			}
			if (connection != null && connection.isOpen()) {
				try {
					connection.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

}
