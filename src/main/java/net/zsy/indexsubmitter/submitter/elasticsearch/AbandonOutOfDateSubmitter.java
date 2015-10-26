package net.zsy.indexsubmitter.submitter.elasticsearch;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;

import net.zsy.indexsubmitter.submitter.ConcurrentSubmitter;

public class AbandonOutOfDateSubmitter extends AbstractIndexSubmitter implements ConcurrentSubmitter {

	private final int threadNum;

	private BlockingQueue<Data> queue;

	private ExecutorService exec;

	private Lock lock;

	private Map<String, Map<String, Long>> timestamps;

	// TODO kill thread
	private Map<String, SubmitThread> threads;

	private Boolean inited = false;
	private Boolean started = false;

	public AbandonOutOfDateSubmitter(Map<String, String> configurations, int threadNum) {
		super(configurations);
		this.threadNum = threadNum;
	}

	@Override
	public synchronized void init() {
		if (!inited) {
			super.init();
			exec = Executors.newFixedThreadPool(threadNum);
			queue = new LinkedBlockingQueue<Data>();
			lock = new ReentrantLock();
			timestamps = new HashMap<String, Map<String, Long>>();
			inited = true;
		}
	}

	@Override
	public void submit(String id, String type, Long timestamp, String json) {
		try {
			queue.put(new Data(id, type, timestamp, json));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public synchronized void start() {
		if (!inited) {
			throw new RuntimeException("Submitter must inited at the first.");
		}
		if (!started) {
			for (int i = 0; i < threadNum; i++) {
				exec.submit(new SubmitThread());
			}
			started = true;
		}
	}

	@Override
	public synchronized void stop() {
		if (started && exec.isShutdown()) {
			exec.shutdown();
			started = false;
		}
	}

	private class SubmitThread implements Runnable {

		private AtomicBoolean shutdown = new AtomicBoolean(false);

		@Override
		public void run() {
			while (!shutdown.get() && !Thread.currentThread().isInterrupted()) {
				try {
					Data data = queue.take();
					String id = data.getId();
					String type = data.getType();
					Long timestamp = data.getTimestamp();
					String json = data.getJson();
					System.out.println(json);
					try {
						lock.lock();
						Map<String, Long> times = timestamps.get(type);
						if (times == null) {
							times = new HashMap<String, Long>();
						} else {
							Long lastsubmittime = times.get(id);
							if (lastsubmittime != null && lastsubmittime > timestamp) {
								continue;
							}
						}
						times.put(id, timestamp);
					} finally {
						lock.unlock();
					}
					UpdateRequest updateRequest = new UpdateRequest();
					updateRequest.index(indexname);
					updateRequest.type(type);
					updateRequest.id(id);
					updateRequest.doc(json.getBytes());
					updateRequest.upsert(json.getBytes());
					UpdateResponse response = client.update(updateRequest).get();
					System.out.println(response.isCreated() ? "create "
							: "update " + response.getIndex() + "/" + response.getType() + "/" + response.getId());
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			Thread.currentThread().interrupt();
		}

		private synchronized void shutdown() {
			shutdown.set(true);
		}
	}

	private class Data {
		private final String id;
		private final String type;
		private final Long timestamp;
		private final String json;

		private Data(String id, String type, Long timestamp, String json) {
			this.id = id;
			this.type = type;
			this.timestamp = timestamp;
			this.json = json;
		}

		public String getId() {
			return id;
		}

		public String getType() {
			return type;
		}

		public Long getTimestamp() {
			return timestamp;
		}

		public String getJson() {
			return json;
		}
	}

	@Override
	public void kill(String threadName) {
		if (threads.containsKey(threadName)) {
			threads.get(threadName).shutdown();
		}

	}

}
