package net.zsy.indexsubmitter.submitter;

/**
 * 并发提交器接口定义
 *
 */
public interface ConcurrentSubmitter extends Submitter {

	void init();

	void start();

	void stop();

	void kill(String threadName);
}
