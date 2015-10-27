package net.zsy.indexsubmitter;

import net.zsy.indexsubmitter.dispatcher.AsyncDispatcher;
import net.zsy.indexsubmitter.dispatcher.Dispatcher;
import net.zsy.indexsubmitter.handler.GenericHandler;
import net.zsy.indexsubmitter.mq.Accepter;
import net.zsy.indexsubmitter.mq.MessageQueues;
import net.zsy.indexsubmitter.submitter.ConcurrentSubmitter;
import net.zsy.indexsubmitter.submitter.Submitters;

public class IndexSubmitter {
	public static void main(String[] args) {
		final Accepter accepter = MessageQueues.getAccepter();
		final Dispatcher dispatcher = new AsyncDispatcher();
		ConcurrentSubmitter cs = Submitters.getAOODSubmitter(5);

		Thread mainThread = new Thread() {

			@Override
			public void run() {
				while (!Thread.currentThread().isInterrupted()) {
					dispatcher.add(accepter.accept());
				}
			}
		};
		accepter.init();
		dispatcher.init();
		cs.init();

		dispatcher.register("product", new GenericHandler(cs));

		accepter.start();
		dispatcher.start();
		cs.start();
		mainThread.start();
	}
}
