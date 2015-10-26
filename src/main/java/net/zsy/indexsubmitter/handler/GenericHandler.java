package net.zsy.indexsubmitter.handler;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.zsy.indexsubmitter.submitter.Submitter;

public class GenericHandler extends AbstractHandler {

	private BlockingQueue<String> messages = new LinkedBlockingQueue<String>();

	public GenericHandler(Submitter submitter) {
		super(submitter);
		Thread t = new Thread(new HandleThread());
		t.start();
	}

	@Override
	public void handle(String message) {
		try {
			messages.put(message);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private class HandleThread implements Runnable {

		private ObjectMapper objectMapper = new ObjectMapper();

		@Override
		public void run() {
			while (!Thread.currentThread().isInterrupted()) {
				try {
					String message = messages.take();

					Map<?, ?> data = objectMapper.readValue(message.getBytes(), Map.class);
					String id = data.get(Submitter.ID).toString();
					String type = data.get(Submitter.TYPE).toString();
					Long timestamp = Long.valueOf(data.get(Submitter.TIMESTAMP).toString());
					String json = data.get(Submitter.DATA).toString();

					submitter.submit(id, type, timestamp, json);
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (JsonParseException e) {
					e.printStackTrace();
				} catch (JsonMappingException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (NullPointerException e) {
					e.printStackTrace();
				}
			}
		}

	}

}
