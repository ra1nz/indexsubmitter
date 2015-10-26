package net.zsy.indexsubmitter.dispatcher;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.zsy.indexsubmitter.handler.Handler;
import net.zsy.indexsubmitter.submitter.Submitter;

public class AsyncDispatcher implements Dispatcher {

	private BlockingQueue<String> messages;

	private Map<String, Handler> handlers;

	private AtomicBoolean inited = new AtomicBoolean(false);

	private AtomicBoolean running = new AtomicBoolean(false);

	private Lock dispatchingLock = new ReentrantLock();

	private Thread dispatchThread;

	@Override
	public synchronized void init() {
		if (!inited.get()) {
			messages = new LinkedBlockingQueue<String>();
			handlers = new HashMap<String, Handler>();
			inited.set(true);
		}

	}

	@Override
	public void start() {
		if (!inited.get()) {
			throw new RuntimeException("Dispatcher must inited at the first.");
		}
		dispatchThread = new Thread(createRunnable());
		dispatchThread.start();
		running.set(true);
	}

	@Override
	public void stop() {
		if (running.get()) {
			dispatchThread.interrupt();
			running.set(false);
		}
	}

	@Override
	public void register(String type, Handler handler) {
		if (!inited.get()) {
			throw new RuntimeException("Dispatcher must inited at the first.");
		}
		handlers.put(type, handler);
	}

	@Override
	public void add(String message) {
		try {
			messages.put(message);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private Runnable createRunnable() {
		return new Runnable() {

			private ObjectMapper objectMapper = new ObjectMapper();

			@Override
			public void run() {
				while (running.get() && !Thread.currentThread().isInterrupted()) {
					dispatchingLock.lock();
					try {
						String message = messages.take();
						Map<?, ?> map = objectMapper.readValue(message.getBytes(), Map.class);
						String type = map.get(Submitter.TYPE).toString();
						handlers.get(type).handle(message);
					} catch (InterruptedException e) {
						e.printStackTrace();
						running.set(false);
					} catch (JsonParseException e) {
						e.printStackTrace();
					} catch (JsonMappingException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					} finally {
						dispatchingLock.unlock();
					}
				}
			}
		};
	}

}
