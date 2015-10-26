package net.zsy.indexsubmitter.submitter;

public interface ConcurrentSubmitter extends Submitter {

	void init();

	void start();

	void stop();

	void kill(String threadName);
}
